package dispatcher

import (
	"context"
	"fmt"
	"sync/atomic"

	"gerrit.instructure.com/ddb-sync/shard_tree"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type shardResult struct {
	err   error
	shard *shard_tree.Shard
}

type DispatchInput struct {
	Context           context.Context
	ContextCancelFunc context.CancelFunc

	InputTableName string
	StreamARN      string
	Client         *dynamodbstreams.DynamoDBStreams
	ShardProcessor func(*shard_tree.Shard) error
}

type Dispatcher struct {
	*DispatchInput

	tree *shard_tree.ShardTree

	results chan *shardResult

	dispatchedCount int32
	workerCount     int32
}

// New creates a new dispatcher
func New(input *DispatchInput) *Dispatcher {
	return &Dispatcher{
		DispatchInput: input,

		tree: shard_tree.New(),

		results: make(chan *shardResult),

		dispatchedCount: 0,
		workerCount:     0,
	}
}

func (d *Dispatcher) dispatchWork() error {
	err := d.updateShardTree()
	if err != nil {
		return err
	}
	for _, availableShard := range d.tree.AvailableShards() {
		atomic.AddInt32(&d.dispatchedCount, 1)
		atomic.AddInt32(&d.workerCount, 1)
		go d.shardHandler(availableShard)
	}
	return nil
}

func (d *Dispatcher) RunWorkers() error {
	var finalErr error
	err := d.dispatchWork()
	if err != nil {
		return err
	}
loop:
	for {
		select {
		case result := <-d.results:
			atomic.AddInt32(&d.workerCount, -1)
			if result.err != nil {
				finalErr = result.err
				d.ContextCancelFunc()
			}
			d.tree.ShardComplete(result.shard)
			err = d.dispatchWork()
			if err != nil {
				finalErr = err
				d.ContextCancelFunc()
			}

			if finalErr != nil && atomic.LoadInt32(&d.workerCount) == 0 {
				break loop
			}
		case <-d.Context.Done():
			finalErr = d.Context.Err()
		}
	}
	close(d.results)
	return finalErr
}

func (d *Dispatcher) Status() string {
	return fmt.Sprintf("Streaming from %d shard(s), %d/%d remaining", d.ActiveWorkerCount(), int32(d.tree.Count())-d.DispatchedCount(), d.tree.Count())
}

func (d *Dispatcher) DispatchedCount() int32 {
	return atomic.LoadInt32(&d.dispatchedCount)
}

func (d *Dispatcher) ActiveWorkerCount() int32 {
	return atomic.LoadInt32(&d.workerCount)
}

func (d *Dispatcher) shardHandler(shard *shard_tree.Shard) {
	err := d.ShardProcessor(shard)
	d.results <- &shardResult{
		err:   err,
		shard: shard,
	}
	return
}

func (d *Dispatcher) updateShardTree() error {
	streamDescription, err := d.describeStreamWithChecks()
	if err != nil {
		return err
	}

	shards := shard_tree.ShardsForDynamoDBShards(streamDescription.StreamDescription.Shards)

	err = d.tree.Add(shards)

	return err
}

func (d *Dispatcher) describeStreamWithChecks() (*dynamodbstreams.DescribeStreamOutput, error) {
	streamRequest := dynamodbstreams.DescribeStreamInput{StreamArn: &d.StreamARN}
	streamDescription, err := d.Client.DescribeStreamWithContext(d.Context, &streamRequest)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodbstreams.ErrCodeResourceNotFoundException {
				return nil, fmt.Errorf("[%s] Error: Stream not found", d.InputTableName)
			}
		}
		return nil, err
	}
	if streamDescription.StreamDescription == nil {
		return nil, fmt.Errorf("[%s] Error: Stream not found", d.InputTableName)
	}
	if *streamDescription.StreamDescription.StreamStatus != "ENABLED" {
		return nil, fmt.Errorf("[%s] Error: Stream not found", d.InputTableName)
	}
	return streamDescription, nil
}
