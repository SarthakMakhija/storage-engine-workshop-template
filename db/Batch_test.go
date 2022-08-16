package db

import (
	"storage-engine-workshop/db/model"
	wal "storage-engine-workshop/log"
	"testing"
)

func TestEmptyBatch(t *testing.T) {
	batch := NewBatch()

	if !batch.isEmpty() {
		t.Fatalf("Expected batch to be empty but was not")
	}
}

func TestNonEmptyBatch(t *testing.T) {
	batch := NewBatch()
	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))

	if batch.isEmpty() {
		t.Fatalf("Expected batch to be non-empty but was empty")
	}
}

func TestBatchKeyValuePairCount(t *testing.T) {
	batch := NewBatch()
	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))

	if totalPairs := batch.totalPairs(); totalPairs != 1 {
		t.Fatalf("Expected batch to contain %v key value pairs but it had %v", 1, totalPairs)
	}
}

func TestBatchIsTotalSizeGreaterThanAsFalse(t *testing.T) {
	batch := NewBatch()
	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))

	if isGreater := batch.isTotalSizeGreaterThan(1000); isGreater {
		t.Fatalf("Expected batch isTotalSizeGreaterThan 1000 to return false but it returned true")
	}
}

func TestBatchIsTotalSizeGreaterThanAsTrue(t *testing.T) {
	batch := NewBatch()
	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))

	if isGreater := batch.isTotalSizeGreaterThan(1); !isGreater {
		t.Fatalf("Expected batch isTotalSizeGreaterThan 1 to return true but it returned false")
	}
}

func TestTotalBatchSize(t *testing.T) {
	batch := NewBatch()

	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))
	batch.add(model.NewSlice([]byte("key-2")), model.NewSlice([]byte("value-2")))
	batch.add(model.NewSlice([]byte("key-3")), model.NewSlice([]byte("value-3")))

	slice := wal.PersistentLogSlice{}
	slice.Add(wal.NewPersistentLogSlice(model.KeyValuePair{Key: model.NewSlice([]byte("key-1")), Value: model.NewSlice([]byte("value-1"))}))
	slice.Add(wal.NewPersistentLogSlice(model.KeyValuePair{Key: model.NewSlice([]byte("key-2")), Value: model.NewSlice([]byte("value-2"))}))
	slice.Add(wal.NewPersistentLogSlice(model.KeyValuePair{Key: model.NewSlice([]byte("key-3")), Value: model.NewSlice([]byte("value-3"))}))
	expectedSize := slice.Size()

	if batch.totalSize() != uint16(expectedSize) {
		t.Fatalf("Expected batch size to be %v, received %v", expectedSize, batch.totalSize())
	}
}
