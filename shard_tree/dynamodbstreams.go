package shard_tree

import (
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func ShardsForDynamoDBShards(dynamodbShards []*dynamodbstreams.Shard) []*Shard {
	shards := []*Shard{}
	for _, dynamodbShard := range dynamodbShards {
		shard := &Shard{Id: *dynamodbShard.ShardId}
		if dynamodbShard.ParentShardId != nil {
			shard.ParentId = *dynamodbShard.ParentShardId
		}

		shards = append(shards, shard)
	}

	return shards
}
