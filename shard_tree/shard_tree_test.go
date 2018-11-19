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

package shard_tree_test

import (
	"testing"

	"github.com/instructure/ddb-sync/shard_tree"
)

func TestShardTreeAddConflictingShardsReturnsError(t *testing.T) {
	tree := shard_tree.New()

	shardSet1 := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1", ParentId: "test-old"},
	}
	if err := tree.Add(shardSet1); err != nil {
		t.Fatalf("Unknown error adding shard set 1: %v", err)
	}

	shardSet2 := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1", ParentId: "test-conflict"},
	}
	if err := tree.Add(shardSet2); err != shard_tree.ErrShardConflict {
		t.Fatalf("Expected a shard conflict error, received: %v", err)
	}
}

func TestShardTreeMarkCompleteReturnsErrorIfMarkingShardNotPresent(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
	}

	unknownShard := shard_tree.Shard{Id: "test-4", ParentId: "test-1"}

	tree.Add(shardSet)
	err := tree.ShardComplete(&unknownShard)

	if err == nil {
		t.Errorf("Expected marking shard complete in invalid condition to error, but didn't")
	}

	if err != shard_tree.ErrShardNotFound {
		t.Errorf("Expected marking shard complete in invalid condition to ErrShardNotFound, but didn't")
	}
}

func TestShardTreeAbleToAddNodes(t *testing.T) {
	tree := shard_tree.New()

	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
	}
	if err := tree.Add(shardSet); err != nil {
		t.Fatalf("Unknown error adding shard set: %v", err)
	}
}

func TestAvailableShardsAreOnlyNodesWhenAvailable(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
	}

	tree.Add(shardSet)

	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
	}
}

func TestShardTreeAbleToAddChildShards(t *testing.T) {
	tree := shard_tree.New()

	// Multiple Add calls
	shardSet1 := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
	}
	if err := tree.Add(shardSet1); err != nil {
		t.Fatalf("Unknown error adding shard set 1: %v", err)
	}

	shardSet2 := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	if err := tree.Add(shardSet2); err != nil {
		t.Fatalf("Unknown error adding shard set 2: %v", err)
	}

	// Verify linkage
	for _, shard := range shardSet2 {
		if shard.Parent != shardSet1[0] {
			t.Fatalf("Expected shard %s to have parent %#v, found %#v instead", shard.Id, shardSet1[0], shard.Parent)
		}
	}
}

func TestAvailableShardsIncludeOnlyIncompleteAncestors(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
	}
}

func TestAbleToMarkAncestorShardComplete(t *testing.T) {
	tree := shard_tree.New()
	shard1 := &shard_tree.Shard{Id: "test-1"}
	shardSet := []*shard_tree.Shard{
		shard1,
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	tree.Add(shardSet)
	err := tree.ShardComplete(shard1)

	if err != nil {
		t.Fatalf("Unknown error completing shard: %v", err)
	}
}

func TestAvailableShardsDoesNotIncludeCompletedAncestors(t *testing.T) {
	tree := shard_tree.New()
	shard1 := &shard_tree.Shard{Id: "test-1"}
	shardSet := []*shard_tree.Shard{
		shard1,
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	tree.Add(shardSet)
	tree.ShardComplete(shard1)

	nowAvailableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		nowAvailableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-3", "test-4", "test-2"} {
		if _, exists := nowAvailableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
		delete(nowAvailableShardIds, expectedId)
	}

	for extraneousId := range nowAvailableShardIds {
		t.Errorf("Extraneous shard returned as available: '%s'", extraneousId)
	}
}

func TestShardTreeGetAvailableShardsDoesNotReturnShardWithIncompleteAncestors(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	// Verify available shards
	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
		delete(availableShardIds, expectedId)
	}

	for extraneousId := range availableShardIds {
		t.Errorf("Shard with incomplete ancestor returned: '%s'", extraneousId)
	}
}

func TestShardTreeDoesNotReturnDifferentAvailableShards(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	// Verify available shards
	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
	}

	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}
}

func TestMarkingLeafShardCompleteReturnsNoNewAvailableShards(t *testing.T) {
	tree := shard_tree.New()
	shard4 := &shard_tree.Shard{Id: "test-4", ParentId: "test-1"}

	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-3", ParentId: "test-1"},
		shard4,
	}

	tree.Add(shardSet)
	tree.ShardComplete(shard4)

	// Verify available shards
	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
	}

	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}
}

func TestShardTreeMarkCompleteReturnsErrorIfParentNotComplete(t *testing.T) {
	tree := shard_tree.New()
	shard3 := &shard_tree.Shard{Id: "test-3", ParentId: "test-1"}
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		shard3,
	}

	tree.Add(shardSet)
	err := tree.ShardComplete(shard3)

	if err == nil {
		t.Errorf("Expected marking shard complete in invalid condition to error, but didn't")
	}

	if err != shard_tree.ErrAncestorIncomplete {
		t.Errorf("Expected marking shard complete in invalid condition to be ErrAncestorIncomplete, but wasn't")
	}
}

func TestShardTreeAttemptingToMarkShardCompleteWhenInvalidDoesNotEffectAvailableShards(t *testing.T) {
	tree := shard_tree.New()
	shard3 := &shard_tree.Shard{Id: "test-3", ParentId: "test-1"}
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		shard3,
		&shard_tree.Shard{Id: "test-4", ParentId: "test-3"},
		&shard_tree.Shard{Id: "test-5", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	// test-1 is still incomplete but test-3 is marked as complete incorrectly
	err := tree.ShardComplete(shard3)

	if err == nil || err != shard_tree.ErrAncestorIncomplete {
		t.Errorf("Expected marking shard complete in invalid condition to be ErrAncestorIncomplete, but wasn't")
	}

	// Verify available shards
	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
		delete(availableShardIds, expectedId)
	}

	for extraneousId := range availableShardIds {
		t.Errorf("Received invalid shard(s): '%s'", extraneousId)
	}
}

func TestShardTreeGetAvailableShardsWhenCalledTwiceDoesNotReturnDuplicates(t *testing.T) {
	tree := shard_tree.New()
	shard3 := &shard_tree.Shard{Id: "test-3", ParentId: "test-1"}
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		shard3,
		&shard_tree.Shard{Id: "test-4", ParentId: "test-3"},
		&shard_tree.Shard{Id: "test-5", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	// test-1 is still incomplete but test-3 is marked as complete incorrectly
	tree.ShardComplete(shard3)

	tree.AvailableShards()

	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}
}

func TestShardTreeParentShardNotProvidedReturnsAsAvailableShard(t *testing.T) {
	tree := shard_tree.New()
	shardSet := []*shard_tree.Shard{
		&shard_tree.Shard{Id: "test-1"},
		&shard_tree.Shard{Id: "test-2"},
		&shard_tree.Shard{Id: "test-4", ParentId: "test-3"},
		&shard_tree.Shard{Id: "test-5", ParentId: "test-1"},
	}

	tree.Add(shardSet)

	// Verify available shards
	availableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		availableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-1", "test-2", "test-4"} {
		if _, exists := availableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
		delete(availableShardIds, expectedId)
	}

	for extraneousId := range availableShardIds {
		t.Errorf("Received invalid shard(s): '%s'", extraneousId)
	}
}
