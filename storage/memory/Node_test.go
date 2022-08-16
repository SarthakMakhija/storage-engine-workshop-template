package memory

import (
	"reflect"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/utils"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(model.NilSlice(), model.NilSlice(), maxLevel)

	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	getResult := sentinelNode.Get(key, keyComparator)
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(model.NilSlice(), model.NilSlice(), maxLevel)

	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	getResult := sentinelNode.Get(key, keyComparator)
	if getResult.Exists != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeysInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(model.NilSlice(), model.NilSlice(), maxLevel)

	sentinelNode.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")), keyComparator, utils.NewLevelGenerator(maxLevel))
	sentinelNode.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")), keyComparator, utils.NewLevelGenerator(maxLevel))

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
	}
	multiGetResult, _ := sentinelNode.MultiGet(keys, keyComparator)
	allGetResults := multiGetResult.Values

	expected := []model.GetResult{
		{Value: model.NewSlice([]byte("Hard disk")), Exists: true},
		{Value: model.NewSlice([]byte("Solid state")), Exists: true},
	}

	for index, e := range expected {
		if e.Value.AsString() != allGetResults[index].Value.AsString() {
			t.Fatalf("Expected %v, received %v", e.Value.AsString(), allGetResults[index].Value.AsString())
		}
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeysWithMissingKeysInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(model.NilSlice(), model.NilSlice(), maxLevel)

	sentinelNode.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")), keyComparator, utils.NewLevelGenerator(maxLevel))
	sentinelNode.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")), keyComparator, utils.NewLevelGenerator(maxLevel))

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
		model.NewSlice([]byte("PMEM")),
	}
	multiGetResult, missingKeys := sentinelNode.MultiGet(keys, keyComparator)
	allGetResults := multiGetResult.Values

	expected := []model.GetResult{
		{Value: model.NewSlice([]byte("Hard disk")), Exists: true},
		{Value: model.NewSlice([]byte("Solid state")), Exists: true},
	}
	expectedMissing := []model.Slice{
		model.NewSlice([]byte("PMEM")),
	}

	for index, e := range expected {
		if e.Value.AsString() != allGetResults[index].Value.AsString() {
			t.Fatalf("Expected %v, received %v", e.Value.AsString(), allGetResults[index].Value.AsString())
		}
	}
	if !reflect.DeepEqual(missingKeys, expectedMissing) {
		t.Fatalf("Expected missing keys to be %v, received %v", missingKeys, expectedMissing)
	}
}

func TestGetsAllKeyValues(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(model.NilSlice(), model.NilSlice(), maxLevel)
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	keyValuePairs := sentinelNode.AllKeyValues()

	if keyValuePairs[0].Key.AsString() != key.AsString() {
		t.Fatalf("Expected persistent key to be %v received %v", key.AsString(), keyValuePairs[0].Key.AsString())
	}

	if keyValuePairs[0].Value.AsString() != value.AsString() {
		t.Fatalf("Expected persistent value to be %v received %v", value.AsString(), keyValuePairs[0].Value.AsString())
	}
}
