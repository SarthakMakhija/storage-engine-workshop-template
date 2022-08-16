package log

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"strconv"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "wal")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestAppendsASuccessfulTransactionalEntryAndReadsIt(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)

	key, value := model.NewSlice([]byte("Key")), model.NewSlice([]byte("Value"))
	persistentLogSlice := NewPersistentLogSlice(model.KeyValuePair{Key: key, Value: value})
	allEntriesSize := persistentLogSlice.Size()

	if err := wal.BeginTransactionHeader(uint16(allEntriesSize)); err != nil {
		log.Fatal(err)
	}
	if err := wal.Append(persistentLogSlice); err != nil {
		log.Fatal(err)
	}
	if err := wal.MarkTransactionWith(TransactionStatusSuccess()); err != nil {
		log.Fatal(err)
	}

	transactionalEntries, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	onlyEntry := transactionalEntries[0]
	if onlyEntry.status.isFailed() {
		t.Fatalf("Expected status to be success, received failed")
	}
	if onlyEntry.keyValuePairs[0].Key.GetSlice().AsString() != "Key" {
		t.Fatalf("Expected key to be %v received %v", "Key", onlyEntry.keyValuePairs[0].Key.GetSlice().AsString())
	}
	if onlyEntry.keyValuePairs[0].Value.GetSlice().AsString() != "Value" {
		t.Fatalf("Expected value to be %v received %v", "Value", onlyEntry.keyValuePairs[0].Value.GetSlice().AsString())
	}
}

func TestAppendsAFailedTransactionalEntryAndReadsIt(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)

	key, value := model.NewSlice([]byte("Key")), model.NewSlice([]byte("Value"))
	persistentLogSlice := NewPersistentLogSlice(model.KeyValuePair{Key: key, Value: value})
	allEntriesSize := persistentLogSlice.Size()

	if err := wal.BeginTransactionHeader(uint16(allEntriesSize)); err != nil {
		log.Fatal(err)
	}
	if err := wal.Append(persistentLogSlice); err != nil {
		log.Fatal(err)
	}
	if err := wal.MarkTransactionWith(TransactionStatusFailed()); err != nil {
		log.Fatal(err)
	}

	transactionalEntries, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	onlyEntry := transactionalEntries[0]
	if onlyEntry.status.isSuccess() {
		t.Fatalf("Expected status to be failed, received success")
	}
	if onlyEntry.keyValuePairs[0].Key.GetSlice().AsString() != "Key" {
		t.Fatalf("Expected key to be %v received %v", "Key", onlyEntry.keyValuePairs[0].Key.GetSlice().AsString())
	}
	if onlyEntry.keyValuePairs[0].Value.GetSlice().AsString() != "Value" {
		t.Fatalf("Expected value to be %v received %v", "Value", onlyEntry.keyValuePairs[0].Value.GetSlice().AsString())
	}
}

func TestAppendsMultipleSuccessEntriesWithinOneTransactionAndReadsAllOfThem(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	persistentLogSlice := PersistentLogSlice{}
	for index := 0; index <= 20; index++ {
		key, value := keyUsing(index), valueUsing(index)
		persistentLogSlice.Add(NewPersistentLogSlice(model.KeyValuePair{Key: key, Value: value}))
	}

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)

	if err := wal.BeginTransactionHeader(uint16(persistentLogSlice.Size())); err != nil {
		log.Fatal(err)
	}
	if err := wal.Append(persistentLogSlice); err != nil {
		log.Fatal(err)
	}
	if err := wal.MarkTransactionWith(TransactionStatusSuccess()); err != nil {
		log.Fatal(err)
	}

	transactionalEntries, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	onlyEntry := transactionalEntries[0]
	if onlyEntry.status.isFailed() {
		t.Fatalf("Expected status to be success, received failed")
	}

	for index := 0; index <= 20; index++ {
		if onlyEntry.keyValuePairs[index].Key.GetSlice().AsString() != keyUsing(index).AsString() {
			t.Fatalf("Expected key to be %v received %v", keyUsing(index).AsString(), onlyEntry.keyValuePairs[index].Key.GetSlice().AsString())
		}
		if onlyEntry.keyValuePairs[index].Value.GetSlice().AsString() != valueUsing(index).AsString() {
			t.Fatalf("Expected value to be %v received %v", valueUsing(index).AsString(), onlyEntry.keyValuePairs[index].Value.GetSlice().AsString())
		}
	}
}

func TestAppendsMultipleFailedEntriesWithinOneTransactionAndReadsAllOfThem(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	persistentLogSlice := PersistentLogSlice{}
	for index := 0; index <= 20; index++ {
		key, value := keyUsing(index), valueUsing(index)
		persistentLogSlice.Add(NewPersistentLogSlice(model.KeyValuePair{Key: key, Value: value}))
	}

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)

	if err := wal.BeginTransactionHeader(uint16(persistentLogSlice.Size())); err != nil {
		log.Fatal(err)
	}
	if err := wal.Append(persistentLogSlice); err != nil {
		log.Fatal(err)
	}
	if err := wal.MarkTransactionWith(TransactionStatusFailed()); err != nil {
		log.Fatal(err)
	}

	transactionalEntries, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	onlyEntry := transactionalEntries[0]
	if onlyEntry.status.isSuccess() {
		t.Fatalf("Expected status to be failed, received success")
	}

	for index := 0; index <= 20; index++ {
		if onlyEntry.keyValuePairs[index].Key.GetSlice().AsString() != keyUsing(index).AsString() {
			t.Fatalf("Expected key to be %v received %v", keyUsing(index).AsString(), onlyEntry.keyValuePairs[index].Key.GetSlice().AsString())
		}
		if onlyEntry.keyValuePairs[index].Value.GetSlice().AsString() != valueUsing(index).AsString() {
			t.Fatalf("Expected value to be %v received %v", valueUsing(index).AsString(), onlyEntry.keyValuePairs[index].Value.GetSlice().AsString())
		}
	}
}

func TestAppendsMultipleSuccessEntriesWithinMultipleTransactionsAndReadsAllOfThem(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)

	makeTransactionalEntries := func(beginIndex, endIndex int) {
		persistentLogSlice := PersistentLogSlice{}
		for index := beginIndex; index < endIndex; index++ {
			key, value := keyUsing(index), valueUsing(index)
			persistentLogSlice.Add(NewPersistentLogSlice(model.KeyValuePair{Key: key, Value: value}))
		}

		if err := wal.BeginTransactionHeader(uint16(persistentLogSlice.Size())); err != nil {
			log.Fatal(err)
		}
		if err := wal.Append(persistentLogSlice); err != nil {
			log.Fatal(err)
		}
		if err := wal.MarkTransactionWith(TransactionStatusFailed()); err != nil {
			log.Fatal(err)
		}
	}
	makeTransactionalEntries(0, 20)
	makeTransactionalEntries(20, 40)

	transactionalEntries, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	assertEntries := func(entryIndex int, keyValuePairIndexOffset, keyValuePairBeginIndex, keyValuePairEndIndex int) {
		entry := transactionalEntries[entryIndex]
		if entry.status.isSuccess() {
			t.Fatalf("Expected status to be failed, received success")
		}

		for index := keyValuePairBeginIndex; index < keyValuePairEndIndex; index++ {
			if entry.keyValuePairs[index].Key.GetSlice().AsString() != keyUsing(index+keyValuePairIndexOffset).AsString() {
				t.Fatalf("Expected key to be %v received %v", keyUsing(index+keyValuePairIndexOffset).AsString(), entry.keyValuePairs[index].Key.GetSlice().AsString())
			}
			if entry.keyValuePairs[index].Value.GetSlice().AsString() != valueUsing(index+keyValuePairIndexOffset).AsString() {
				t.Fatalf("Expected value to be %v received %v", valueUsing(index+keyValuePairIndexOffset).AsString(), entry.keyValuePairs[index].Value.GetSlice().AsString())
			}
		}
	}
	assertEntries(0, 0, 0, 20)
	assertEntries(1, 20, 0, 20)
}
