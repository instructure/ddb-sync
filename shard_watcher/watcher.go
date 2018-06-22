package shard_watcher

import (
	"context"
	"fmt"
	"sync/atomic"

	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/shard_tree"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type shardResult struct {
	err   error
	shard *shard_tree.Shard
}

type RunInput struct {
	Context           context.Context
	ContextCancelFunc context.CancelFunc

	InputTableName  string
	OutputTableName string

	StreamARN      string
	Client         *dynamodbstreams.DynamoDBStreams
	ShardProcessor func(*shard_tree.Shard) error
}

type Watcher struct {
	*RunInput

	tree *shard_tree.ShardTree

	results chan *shardResult

	dispatchedCount int32
	workerCount     int32
}

// New creates a new watcher
func New(input *RunInput) *Watcher {
	return &Watcher{
		RunInput: input,

		tree: shard_tree.New(),

		results: make(chan *shardResult),

		dispatchedCount: 0,
		workerCount:     0,
	}
}

func (w *Watcher) dispatchWork() error {
	err := w.updateShardTree()
	if err != nil {
		return err
	}
	for _, availableShard := range w.tree.AvailableShards() {
		atomic.AddInt32(&w.dispatchedCount, 1)
		atomic.AddInt32(&w.workerCount, 1)
		go w.shardHandler(availableShard)
	}
	return nil
}

func (w *Watcher) logShardCompletion() {
	log.Printf("[%s] ⇨ [%s] Shard complete. %d completed.\n", w.InputTableName, w.OutputTableName, w.DispatchedCount())
}

func (w *Watcher) RunWorkers() error {
	var finalErr error
	err := w.dispatchWork()
	if err != nil {
		return err
	}
loop:
	for {
		select {
		case result := <-w.results:
			atomic.AddInt32(&w.workerCount, -1)
			if result.err != nil {
				finalErr = result.err
				w.ContextCancelFunc()
			}
			w.tree.ShardComplete(result.shard)
			w.logShardCompletion()
			err = w.dispatchWork()
			if err != nil {
				finalErr = err
				w.ContextCancelFunc()
			}

			if finalErr != nil && atomic.LoadInt32(&w.workerCount) == 0 {
				break loop
			}
		case <-w.Context.Done():
			finalErr = w.Context.Err()
		}
	}
	close(w.results)
	return finalErr
}

func (w *Watcher) Started() bool {
	return w != nil
}

func (w *Watcher) DispatchedCount() int32 {
	return atomic.LoadInt32(&w.dispatchedCount)
}

func (w *Watcher) ActiveWorkerCount() int32 {
	return atomic.LoadInt32(&w.workerCount)
}

func (w *Watcher) shardHandler(shard *shard_tree.Shard) {
	err := w.ShardProcessor(shard)
	w.results <- &shardResult{
		err:   err,
		shard: shard,
	}
	return
}

func (w *Watcher) updateShardTree() error {
	streamDescription, err := w.describeStreamWithChecks()
	if err != nil {
		return err
	}

	shards := shard_tree.ShardsForDynamoDBShards(streamDescription.StreamDescription.Shards)

	err = w.tree.Add(shards)

	return err
}

func (w *Watcher) describeStreamWithChecks() (*dynamodbstreams.DescribeStreamOutput, error) {
	streamRequest := dynamodbstreams.DescribeStreamInput{StreamArn: &w.StreamARN}
	streamDescription, err := w.Client.DescribeStreamWithContext(w.Context, &streamRequest)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodbstreams.ErrCodeResourceNotFoundException {
				return nil, fmt.Errorf("[%s] Error: Stream not found", w.InputTableName)
			}
		}
		return nil, err
	}
	if streamDescription.StreamDescription == nil {
		return nil, fmt.Errorf("[%s] Error: Stream not found", w.InputTableName)
	}
	if *streamDescription.StreamDescription.StreamStatus != "ENABLED" {
		return nil, fmt.Errorf("[%s] Error: Stream not found", w.InputTableName)
	}
	return streamDescription, nil
}
