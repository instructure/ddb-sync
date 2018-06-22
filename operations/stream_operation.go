package operations

import (
	"context"
	"fmt"
	"time"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/shard_tree"
	"gerrit.instructure.com/ddb-sync/shard_watcher"
	"gerrit.instructure.com/ddb-sync/status"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type StreamOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	inputDescribeTableClient *dynamodb.DynamoDB
	inputClient              *dynamodbstreams.DynamoDBStreams
	outputClient             *dynamodb.DynamoDB

	writeLatency LatencyLock

	c         chan dynamodbstreams.Record
	streamARN string

	streamRead Phase
	writing    Phase

	watcher *shard_watcher.Watcher

	readItemRateTracker    *RateTracker
	wcuRateTracker         *RateTracker
	writtenItemRateTracker *RateTracker
}

func NewStreamOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*StreamOperation, error) {
	inputSession, outputSession, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodbstreams.New(inputSession)
	inputDescribeTableClient := dynamodb.New(inputSession)
	outputClient := dynamodb.New(outputSession)

	return &StreamOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		c: make(chan dynamodbstreams.Record, 3500),

		inputClient:              inputClient,
		inputDescribeTableClient: inputDescribeTableClient,
		outputClient:             outputClient,

		readItemRateTracker:    NewRateTracker("Items", 9*time.Second),
		wcuRateTracker:         NewRateTracker("WCUs", 9*time.Second),
		writtenItemRateTracker: NewRateTracker("Items", 9*time.Second),
	}, nil
}

func (o *StreamOperation) Preflights(in *dynamodb.DescribeTableOutput, _ *dynamodb.DescribeTableOutput) error {
	streamSpecification := in.Table.StreamSpecification
	if streamSpecification == nil {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not enabled", *in.Table.TableName)
	}
	if !*streamSpecification.StreamEnabled {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not enabled", *in.Table.TableName)
	}

	if !(*streamSpecification.StreamViewType == dynamodb.StreamViewTypeNewImage || *streamSpecification.StreamViewType == dynamodb.StreamViewTypeNewAndOldImages) {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not a correct type 'NEW_IMAGE' or 'NEW_AND_OLD_IMAGES'", *in.Table.TableName)
	}

	o.streamARN = *in.Table.LatestStreamArn

	return nil
}

func (o *StreamOperation) Run() error {
	o.readItemRateTracker.Start()
	o.wcuRateTracker.Start()
	o.writtenItemRateTracker.Start()

	defer o.readItemRateTracker.Stop()
	defer o.wcuRateTracker.Stop()
	defer o.writtenItemRateTracker.Stop()

	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.readStream)
	collator.Register(o.writeRecords)

	return collator.Run()
}

func (o *StreamOperation) Status() string {
	if !o.watcher.Started() {
		return pendingMsg
	}

	return fmt.Sprintf("%d written (%s latent)", o.writtenItemRateTracker.Count(), o.writeLatency.Status())
}

// Checkpoint is a periodic status output meant for historical tracking.  This will be called when an update is desired.
func (o *StreamOperation) Checkpoint() string {
	if o.writing.Running() {
		return fmt.Sprintf("%s Streaming: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), o.writtenItemRateTracker.Duration().String())
	}
	return ""
}

func (o *StreamOperation) Rate() string {
	if o.writing.Running() {
		buffer := float64(o.bufferFill()) / float64(o.bufferCapacity())
		return fmt.Sprintf("%s %s %s", o.readItemRateTracker.RatePerSecond(), status.BufferStatus(buffer), o.wcuRateTracker.RatePerSecond())
	}
	return ""
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)
	log.Printf("%s Streaming started…", o.OperationPlan.Description())
	o.streamRead.Start()

	err := o.lookupLatestStreamARN(o.OperationPlan.Input.TableName)
	if err != nil {
		return err
	}

	watcherInput := &shard_watcher.RunInput{
		Context:           o.context,
		ContextCancelFunc: o.contextCancelFunc,

		InputTableName:  o.OperationPlan.Input.TableName,
		OutputTableName: o.OperationPlan.Output.TableName,
		StreamARN:       o.streamARN,
		Client:          o.inputClient,

		ShardProcessor: o.processShard,
	}

	o.watcher = shard_watcher.New(watcherInput)

	err = o.watcher.RunWorkers()
	if err == nil {
		log.Printf("%s Stream closed: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), o.writtenItemRateTracker.Duration().String())
		o.streamRead.Finish()
	} else {
		o.streamRead.Error()
	}

	return err
}

func (o *StreamOperation) processShard(shard *shard_tree.Shard) error {
	shardIteratorOutput, err := o.inputClient.GetShardIteratorWithContext(
		o.context,
		&dynamodbstreams.GetShardIteratorInput{
			StreamArn:         &o.streamARN,
			ShardId:           &shard.Id,
			ShardIteratorType: aws.String("TRIM_HORIZON"),
		},
	)
	if err != nil {
		return err
	}

	iterator := shardIteratorOutput.ShardIterator

	recordInput := &dynamodbstreams.GetRecordsInput{Limit: aws.Int64(1000), ShardIterator: iterator}
	recordOutput, err := o.inputClient.GetRecordsWithContext(o.context, recordInput)

	if err != nil {
		return err
	}

	done := o.context.Done()
	for recordOutput.NextShardIterator != nil && *recordOutput.NextShardIterator != "" {
		for _, record := range recordOutput.Records {
			o.readItemRateTracker.Increment(1)
			select {
			case o.c <- *record:
			case <-done:
				return o.context.Err()
			}
		}

		iterator := recordOutput.NextShardIterator
		recordInput := &dynamodbstreams.GetRecordsInput{Limit: aws.Int64(1000), ShardIterator: iterator}
		var err error
		recordOutput, err = o.inputClient.GetRecordsWithContext(o.context, recordInput)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *StreamOperation) lookupLatestStreamARN(tableName string) error {
	tableOutput, err := o.inputDescribeTableClient.DescribeTableWithContext(
		o.context,
		&dynamodb.DescribeTableInput{
			TableName: &o.OperationPlan.Input.TableName,
		},
	)

	if err != nil {
		return err
	}

	o.streamARN = *tableOutput.Table.LatestStreamArn
	return nil
}

func (o *StreamOperation) writeRecords() error {
	o.writing.Start()
	for record := range o.c {
		o.writeLatency.Update(*record.Dynamodb.ApproximateCreationDateTime)
		if *record.EventName == "REMOVE" {
			input := &dynamodb.DeleteItemInput{
				Key:       record.Dynamodb.Keys,
				TableName: aws.String(o.OperationPlan.Output.TableName),
			}
			resp, err := o.outputClient.DeleteItemWithContext(o.context, input)
			if err != nil {
				return err
			}

			o.markItemWritten(resp.ConsumedCapacity)
		} else {
			input := &dynamodb.PutItemInput{
				Item: record.Dynamodb.NewImage,
				ReturnConsumedCapacity: aws.String("TOTAL"),
				TableName:              aws.String(o.OperationPlan.Output.TableName),
			}
			resp, err := o.outputClient.PutItemWithContext(o.context, input)
			if err != nil {
				return err
			}

			o.markItemWritten(resp.ConsumedCapacity)
		}
	}

	o.writing.Finish()

	return nil
}

func (o *StreamOperation) bufferFill() int {
	return len(o.c)
}

func (o *StreamOperation) bufferCapacity() int {
	return cap(o.c)
}

func (o *StreamOperation) markItemWritten(cap *dynamodb.ConsumedCapacity) {
	o.writtenItemRateTracker.Increment(1)
	o.wcuRateTracker.Increment(int64(*cap.CapacityUnits))
}
