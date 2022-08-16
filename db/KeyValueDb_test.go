package db

import (
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"strconv"
	"testing"
)

func TestPutsKeysValuesAndGetByKeys(t *testing.T) {
	const segmentMaxSizeBytes uint64 = 32
	const bufferMaxSizeBytes uint64 = 64

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	db, _ := NewKeyValueDb(configuration)

	txn := db.newTransaction()
	_ = txn.Put(model.NewSlice([]byte("Key")), model.NewSlice([]byte("Value")))

	if err := txn.Commit(); err != nil {
		log.Fatal(err)
	}

	readonlyTxn := db.newReadonlyTransaction()
	getResult := readonlyTxn.Get(model.NewSlice([]byte("Key")))
	if getResult.Value.AsString() != "Value" {
		t.Fatalf("Expected %v, received %v", "Value", getResult.Value.AsString())
	}
}

func TestPuts20KeysValuesAndGetByKeys(t *testing.T) {
	const segmentMaxSizeBytes uint64 = 32
	const bufferMaxSizeBytes uint64 = 64

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	db, _ := NewKeyValueDb(configuration)

	txn := db.newTransaction()
	for count := 1; count <= 20; count++ {
		err := txn.Put(keyUsing(count), valueUsing(count))
		if err != nil {
			t.Error(err)
		}
	}
	if err := txn.Commit(); err != nil {
		log.Fatal(err)
	}

	allowFlushingSSTable()

	readonlyTxn := db.newReadonlyTransaction()
	for count := 1; count <= 20; count++ {
		getResult := readonlyTxn.Get(keyUsing(count))
		expectedValue := valueUsing(count)

		if getResult.Value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
		}
	}
}
