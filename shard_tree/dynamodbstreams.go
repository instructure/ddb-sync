/*
 * ddb-sync
 * Copyright (C) 2018 Instructure Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
