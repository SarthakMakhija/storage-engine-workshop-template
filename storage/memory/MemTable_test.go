package memory

import (
	"reflect"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"testing"
)

func TestPutAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	getResult := memTable.Get(key)
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	getResult := memTable.Get(key)
	if getResult.Exists != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeyInNodeInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTable.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
		model.NewSlice([]byte("PMEM")),
	}
	multiGetResult, missingKeys := memTable.MultiGet(keys)
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

func TestPutAKeyValueAndGetsAllKeyValues(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	keyValuePairs := memTable.AllKeyValues()

	if keyValuePairs[0].Key.AsString() != key.AsString() {
		t.Fatalf("Expected key to be %v from all keys but received %v", key.AsString(), keyValuePairs[0].Key.AsString())
	}
	if keyValuePairs[0].Value.AsString() != value.AsString() {
		t.Fatalf("Expected value to be %v from all keys but received %v", value.AsString(), keyValuePairs[0].Value.AsString())
	}
}

func TestPutAKeyValueAndGetsTheTotalKeysInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTable.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	totalKeys := memTable.TotalKeys()

	if totalKeys != 2 {
		t.Fatalf("Expected %v keys but received %v", 2, totalKeys)
	}
}

func TestReturnsTheTotalMemTableSize(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	size := memTable.TotalSize()
	expected := key.Size() + value.Size()

	if size != uint64(expected) {
		t.Fatalf("Expected total memtable size to be %v, received %v", expected, size)
	}
}
