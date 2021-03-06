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
	"errors"
)

var (
	ErrShardNotFound = errors.New("Shard not found in tree")
	ErrShardConflict = errors.New("Conflicting shard already exists in tree")

	ErrAncestorInProgress = errors.New("Ancestor in-progress")
	ErrAncestorIncomplete = errors.New("Ancestor incomplete")
)

type Shard struct {
	Id       string
	ParentId string

	Parent *Shard
}

type ShardStatus struct {
	Shard *Shard

	InProgress bool
	Complete   bool
}

type ShardTree struct {
	// The most descendent shards in the tree (shards who are not parents)
	descendentShards map[string]*Shard

	// The status of all shards
	shardStatuses map[string]*ShardStatus
}

func New() *ShardTree {
	return &ShardTree{
		descendentShards: make(map[string]*Shard),
		shardStatuses:    make(map[string]*ShardStatus),
	}
}

func (t *ShardTree) Add(shards []*Shard) error {
	// Add new shards
	for _, shard := range shards {
		// Check if the shard already exists
		if existingStatus := t.shardStatuses[shard.Id]; existingStatus != nil {
			if existingStatus.Shard.ParentId != shard.ParentId {
				return ErrShardConflict
			}
			continue
		}

		t.descendentShards[shard.Id] = shard
		t.shardStatuses[shard.Id] = &ShardStatus{
			Shard: shard,
		}
	}

	// Link new shards into the tree
	for _, status := range t.shardStatuses {
		// it's okay if the parent doesn't exist
		if parentStatus := t.shardStatuses[status.Shard.ParentId]; parentStatus != nil {
			status.Shard.Parent = parentStatus.Shard
		}

		// this shard's parent can no longer be considered one of the most-descendent shards
		delete(t.descendentShards, status.Shard.ParentId)
	}

	return nil
}

func (t *ShardTree) ShardComplete(shard *Shard) error {
	shardStatus := t.shardStatuses[shard.Id]
	if shardStatus == nil {
		return ErrShardNotFound
	}

	if parentStatus, present := t.shardStatuses[shard.ParentId]; present {
		if !parentStatus.Complete {
			return ErrAncestorIncomplete
		}
	}

	shardStatus.InProgress = false
	shardStatus.Complete = true
	return nil
}

func (t *ShardTree) AvailableShards() []*Shard {
	availableShards := []*Shard{}
	for _, descendant := range t.descendentShards {
		availableAncestor, err := t.availableAncestor(descendant)
		if err != nil {
			// the descendant or its ancestors are not currently available
			continue
		}

		if availableAncestor != nil {
			t.shardStatuses[availableAncestor.Id].InProgress = true
			availableShards = append(availableShards, availableAncestor)
		}
	}

	return availableShards
}

func (t *ShardTree) Count() int {
	return len(t.shardStatuses)
}

func (t *ShardTree) availableAncestor(shard *Shard) (*Shard, error) {
	status := t.shardStatuses[shard.Id]

	// If the shard doesn't have a status, it was never added to the tree. This
	// means we aren't able to track the shard properly and must panic.
	if status == nil {
		panic("Invalid shard")
	}

	// If the shard is complete, the ancestors are also complete
	if status.Complete {
		return nil, nil
	}

	// Shards are not available if they or any of their ancestors are in-progress
	if status.InProgress {
		return nil, ErrAncestorInProgress
	}

	// Check if an older ancestor is available
	if shard.Parent != nil {
		if ancestorShard, err := t.availableAncestor(shard.Parent); ancestorShard != nil || err != nil {
			return ancestorShard, err
		}
	}

	// No ancestors are available, but this shard is!
	return shard, nil
}
