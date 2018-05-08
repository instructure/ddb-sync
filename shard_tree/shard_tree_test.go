package shard_tree_test

import (
	"testing"

	"gerrit.instructure.com/ddb-sync/shard_tree"
)

func TestShardTreeHappyPath(t *testing.T) {
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
		t.Errorf("Extraneous shard returned as available: '%s'", extraneousId)
	}

	// No additional shards available until some complete
	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Status quo. Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}

	// Terminal shard complete ("test-2")
	if err := tree.ShardComplete(shardSet1[1]); err != nil {
		t.Fatalf("Unknown error completing shard: %v", err)
	}

	// No additional shards available ("test-2" has no children)
	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Terminal complete. Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}

	// Parent shard complete ("test-1")
	if err := tree.ShardComplete(shardSet1[0]); err != nil {
		t.Fatalf("Unknown error completing shard: %v", err)
	}

	// Verify now available shards
	nowAvailableShardIds := make(map[string]bool)
	for _, shard := range tree.AvailableShards() {
		nowAvailableShardIds[shard.Id] = true
	}

	for _, expectedId := range []string{"test-3", "test-4"} {
		if _, exists := nowAvailableShardIds[expectedId]; !exists {
			t.Errorf("Expected '%s' to be available, but wasn't", expectedId)
		}
		delete(nowAvailableShardIds, expectedId)
	}

	for extraneousId := range nowAvailableShardIds {
		t.Errorf("Extraneous shard returned as available: '%s'", extraneousId)
	}

	// Complete remaining shards
	if err := tree.ShardComplete(shardSet2[0]); err != nil {
		t.Fatalf("Unknown error completing shard: %v", err)
	}
	if err := tree.ShardComplete(shardSet2[1]); err != nil {
		t.Fatalf("Unknown error completing shard: %v", err)
	}

	// No additional shards available (all shards complete)
	for _, invalidShard := range tree.AvailableShards() {
		t.Errorf("Shards complete. Shard available, but shouldn't be: '%s'", invalidShard.Id)
	}
}

func TestShardTreeAddConflictingShards(t *testing.T) {
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
