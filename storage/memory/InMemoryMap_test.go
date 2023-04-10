package memory

import (
	"reflect"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInMemoryMap(t *testing.T) {
	sentinelNode := NewInMemoryMap()

	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value)

	getResult := sentinelNode.Get(key)
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInMemoryMap(t *testing.T) {
	sentinelNode := NewInMemoryMap()

	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value)

	getResult := sentinelNode.Get(key)
	if getResult.Exists != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeysInMemoryMap(t *testing.T) {
	sentinelNode := NewInMemoryMap()

	sentinelNode.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	sentinelNode.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
	}
	multiGetResult, _ := sentinelNode.MultiGet(keys)
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

func TestPutsKeyValuesAndDoesMultiGetByKeysWithMissingKeysInMemoryMap(t *testing.T) {
	sentinelNode := NewInMemoryMap()

	sentinelNode.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	sentinelNode.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
		model.NewSlice([]byte("PMEM")),
	}
	multiGetResult, missingKeys := sentinelNode.MultiGet(keys)
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

func TestGetsAllKeyValuesInMemoryMap(t *testing.T) {
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewInMemoryMap()
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value)
	sentinelNode.Put(model.NewSlice([]byte("Disk")), model.NewSlice([]byte("SSD")))

	keyValuePairs := sentinelNode.AllKeyValues(keyComparator)

	if keyValuePairs[0].Key.AsString() != model.NewSlice([]byte("Disk")).AsString() {
		t.Fatalf("Expected persistent key to be %v received %v", model.NewSlice([]byte("Disk")).AsString(), keyValuePairs[0].Key.AsString())
	}
	if keyValuePairs[1].Key.AsString() != key.AsString() {
		t.Fatalf("Expected persistent key to be %v received %v", key.AsString(), keyValuePairs[0].Key.AsString())
	}

	if keyValuePairs[0].Value.AsString() != model.NewSlice([]byte("SSD")).AsString() {
		t.Fatalf("Expected persistent value to be %v received %v", model.NewSlice([]byte("SSD")).AsString(), keyValuePairs[0].Value.AsString())
	}
	if keyValuePairs[1].Value.AsString() != value.AsString() {
		t.Fatalf("Expected persistent value to be %v received %v", value.AsString(), keyValuePairs[0].Value.AsString())
	}
}
