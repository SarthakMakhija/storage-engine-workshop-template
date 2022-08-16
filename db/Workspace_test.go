package db

import (
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"strconv"
	"testing"
)

func TestPut200KeysValuesAndGetByKeysInWorkspace(t *testing.T) {
	const segmentMaxSizeBytes uint64 = 10 * 1024
	const bufferMaxSizeBytes uint64 = 512

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	workspace, _ := newWorkSpace(configuration)

	batch := NewBatch()
	for count := 1; count <= 200; count++ {
		batch.add(keyUsing(count), valueUsing(count))
	}
	_ = workspace.put(batch)

	allowFlushingSSTable()

	for count := 1; count <= 200; count++ {
		getResult := workspace.get(keyUsing(count))
		expectedValue := valueUsing(count)

		if getResult.Value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
		}
	}
}

func TestPut1000KeysValuesAndMultiGetKeysInWorkspace(t *testing.T) {
	const segmentMaxSizeBytes uint64 = 10 * 1024
	const bufferMaxSizeBytes uint64 = 512

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	workspace, _ := newWorkSpace(configuration)

	batch := NewBatch()
	for count := 1; count <= 200; count++ {
		batch.add(keyUsing(count), valueUsing(count))
	}
	_ = workspace.put(batch)

	allowFlushingSSTable()

	keys := []model.Slice{
		model.NewSlice([]byte("Key-1")),
		model.NewSlice([]byte("Key-100")),
		model.NewSlice([]byte("Key-400")),
		model.NewSlice([]byte("Key-900")),
		model.NewSlice([]byte("Key-Unknown")),
	}

	expectedValueByKey := map[string]string{
		"Key-1":       "Value-1",
		"Key-100":     "Value-100",
		"Key-400":     "Value-400",
		"Key-900":     "Value-900",
		"Key-Unknown": "",
	}
	multiGetResult := workspace.multiGet(keys)
	for _, result := range multiGetResult {
		if result.Value.AsString() != expectedValueByKey[result.Key.AsString()] {
			t.Fatalf("Expected value to be %v, received %v", expectedValueByKey[result.Key.AsString()], result.Value.AsString())
		}
	}
}
