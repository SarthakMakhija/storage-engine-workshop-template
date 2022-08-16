package db

import (
	"fmt"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"strconv"
	"sync"
	"testing"
	"time"
)

func initRequestExecutor() (*RequestExecutor, string) {
	const segmentMaxSizeBytes uint64 = 32
	const bufferMaxSizeBytes uint64 = 1024

	directory := tempDirectory()

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	workSpace, _ := newWorkSpace(configuration)

	return newRequestExecutor(workSpace), directory
}

func TestPutFollowedByGet(t *testing.T) {
	var wg sync.WaitGroup
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	wg.Add(2)
	go func() {
		defer wg.Done()
		batch := NewBatch()
		batch.add(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		<-executor.put(batch)
	}()

	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		getResult := <-executor.get(model.NewSlice([]byte("Company")))
		if getResult.Value.AsString() != "TW" {
			t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", "TW", getResult.Value.AsString()))
		}
	}()

	wg.Wait()
}

func TestPutAndGetConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	wg.Add(2)
	go func() {
		defer wg.Done()
		batch := NewBatch()
		batch.add(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		<-executor.put(batch)
	}()

	go func() {
		defer wg.Done()
		getResult := <-executor.get(model.NewSlice([]byte("Company")))
		if getResult.Exists && getResult.Value.AsString() != "TW" {
			t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", "TW", getResult.Value.AsString()))
		}
	}()

	wg.Wait()
}

func TestMultiGetFollowedByPut(t *testing.T) {
	var wg sync.WaitGroup
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	wg.Add(2)
	go func() {
		defer wg.Done()
		batch := NewBatch()
		batch.add(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		batch.add(model.NewSlice([]byte("Field")), model.NewSlice([]byte("Storage engine")))
		<-executor.put(batch)
	}()

	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		expectedValueByKey := map[string]string{
			"Company": "TW",
			"Field":   "Storage engine",
		}
		multiGetResult := <-executor.multiGet([]model.Slice{model.NewSlice([]byte("Company")), model.NewSlice([]byte("Field"))})
		for _, result := range multiGetResult {
			if result.Value.AsString() != expectedValueByKey[result.Key.AsString()] {
				t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", expectedValueByKey[result.Key.AsString()], result.Value.AsString()))
			}
		}
	}()

	wg.Wait()
}

func TestPutMultipleConcurrentBatches(t *testing.T) {
	var wg sync.WaitGroup
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	wg.Add(10)
	keyUsing := func(id, index int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(id) + "-" + strconv.Itoa(index)))
	}
	valueUsing := func(id, index int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(id) + "-" + strconv.Itoa(index)))
	}
	for goroutineId := 1; goroutineId <= 10; goroutineId++ {
		go func(id int) {
			defer wg.Done()
			batch := NewBatch()
			for index := 1; index <= 200; index++ {
				batch.add(keyUsing(id, index), valueUsing(id, index))
			}
			<-executor.put(batch)
		}(goroutineId)
	}

	wg.Wait()
	allowFlushingSSTable()

	for goroutineId := 1; goroutineId <= 10; goroutineId++ {
		for index := 1; index <= 200; index++ {
			getResult := <-executor.get(keyUsing(goroutineId, index))
			expectedValue := valueUsing(goroutineId, index)
			if getResult.Value.AsString() != expectedValue.AsString() {
				t.Fatalf("Expected value to be %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
			}
		}
	}

	wg.Wait()
}

func TestPutAndMultiGetConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	executor, directory := initRequestExecutor()
	defer os.RemoveAll(directory)

	wg.Add(2)
	go func() {
		defer wg.Done()
		batch := NewBatch()
		batch.add(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		batch.add(model.NewSlice([]byte("Field")), model.NewSlice([]byte("Storage engine")))
		<-executor.put(batch)
	}()

	go func() {
		defer wg.Done()
		expectedValueByKey := map[string]string{
			"Company": "TW",
			"Field":   "Storage engine",
		}
		multiGetResult := <-executor.multiGet([]model.Slice{model.NewSlice([]byte("Company")), model.NewSlice([]byte("Field"))})
		for _, result := range multiGetResult {
			if result.Exists && result.Value.AsString() != expectedValueByKey[result.Key.AsString()] {
				t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", expectedValueByKey[result.Key.AsString()], result.Value.AsString()))
			}
		}
	}()

	wg.Wait()
}
