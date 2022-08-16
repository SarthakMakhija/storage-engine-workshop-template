package db

import (
	"os"
	"storage-engine-workshop/db/model"
	"strconv"
	"sync"
	"testing"
)

func TestAttemptsToCommitATransactionWithEmptyBatch(t *testing.T) {
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	transaction := newTransaction(executor)

	err := transaction.Commit()

	if err == nil {
		t.Fatalf("Expected an error on commiting without invoking put but received not error")
	}
}

func TestPutsAKeyValuePairAndGetsByKey(t *testing.T) {
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	transaction := newTransaction(executor)

	_ = transaction.Put(model.NewSlice([]byte("Key")), model.NewSlice([]byte("Value")))
	_ = transaction.Commit()

	readonlyTxn := newReadonlyTransaction(executor)
	if getResult := readonlyTxn.Get(model.NewSlice([]byte("Key"))); getResult.Value.AsString() != "Value" {
		t.Fatalf("Expected %v, received %v", "Value", getResult.Value.AsString())
	}
}

func TestPutsMultipleKeyValuePairsInDifferentGoroutines(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(10)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	for count := 1; count <= 10; count++ {
		go func(keyIndex int) {
			defer wg.Done()
			transaction := newTransaction(executor)
			_ = transaction.Put(keyUsing(keyIndex), valueUsing(keyIndex))
			_ = transaction.Commit()
		}(count)
	}
	wg.Wait()

	readonlyTxn := newReadonlyTransaction(executor)
	for count := 1; count <= 10; count++ {
		getResult := readonlyTxn.Get(keyUsing(count))
		expectedValue := valueUsing(count)

		if getResult.Value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
		}
	}
}
