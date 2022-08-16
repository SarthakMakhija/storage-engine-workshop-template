package sst

import (
	"errors"
	"fmt"
	"path"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/filter"
	"storage-engine-workshop/storage/memory"
	"strconv"
)

type SSTable struct {
	store         *Store
	keyValuePairs []model.KeyValuePair
	bloomFilter   *filter.BloomFilter
}

func NewSSTableFrom(memTable *memory.MemTable, bloomFilters *filter.BloomFilters, directory string, fileId int) (*SSTable, error) {
	store, err := NewStore(path.Join(directory, fmt.Sprintf("%v.sst", fileId)))
	if err != nil {
		return nil, err
	}
	bloomFilter, err := createBloomFilter(fileId, memTable.TotalKeys(), bloomFilters)
	if err != nil {
		return nil, err
	}
	return &SSTable{
		store:         store,
		keyValuePairs: []model.KeyValuePair{}, //Assignment:SSTable:1:get all key value pairs
		bloomFilter:   bloomFilter,
	}, nil
}

func (ssTable *SSTable) Write() error {
	if len(ssTable.keyValuePairs) == 0 {
		return errors.New("ssTable does not contain any key value pairs to write to " + ssTable.store.file.Name())
	}
	beginOffsetByKey, offset, err := ssTable.writeKeyValues()
	if err != nil {
		return err
	}
	indexBlock := NewIndexBlock(ssTable.store)
	if err := indexBlock.Write(beginOffsetByKey, offset, ssTable.keyValuePairs); err != nil {
		return err
	}
	if err := ssTable.store.Sync(); err != nil {
		return errors.New("error while syncing the ssTable file " + ssTable.store.file.Name())
	}
	return nil
}

func (ssTable *SSTable) Get(key model.Slice, keyComparator comparator.KeyComparator) model.GetResult {
	NewIndexBlock(ssTable.store)
	//Assignment:SSTable:4:get the offset of the key by using the newly created index block
	var keyOffset int64 = 0
	var err error
	if err != nil {
		return model.GetResult{Key: key, Exists: false}
	}
	if keyOffset == -1 {
		return model.GetResult{Key: key, Exists: false}
	}

	//Assignment:SSTable:5:read at the offset obtained after completing //Assignment:SSTable:4
	var resultValue PersistentSSTableSlice
	if err != nil {
		return model.GetResult{Key: key, Exists: false}
	}
	return model.GetResult{Key: key, Value: resultValue.GetSlice(), Exists: true}
}

func (ssTable *SSTable) readAt(offset int64) (PersistentSSTableSlice, PersistentSSTableSlice, error) {
	bytes := make([]byte, int(reservedTotalSize))
	_, err := ssTable.store.ReadAt(bytes, offset)
	if err != nil {
		return EmptyPersistentSSTableSlice(), EmptyPersistentSSTableSlice(), err
	}
	sizeToRead := ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = ssTable.store.ReadAt(contents, offset)
	if err != nil {
		return EmptyPersistentSSTableSlice(), EmptyPersistentSSTableSlice(), err
	}
	key, value := NewPersistentSSTableSliceKeyValuePair(contents)
	return key, value, nil
}

func (ssTable *SSTable) writeKeyValues() ([]int64, int64, error) {
	var offset int64 = 0
	beginOffsetByKey := make([]int64, len(ssTable.keyValuePairs))

	for index, keyValuePair := range ssTable.keyValuePairs {
		if bytesWritten, err := ssTable.store.WriteAt(NewPersistentSSTableSlice(keyValuePair).GetPersistentContents(), offset); err != nil {
			return nil, 0, err
		} else {
			//Assignment:SSTable:2:capture the begin-offset of the key to be used in index block
			fmt.Println(index)
			offset = offset + int64(bytesWritten)
		}
		if err := ssTable.bloomFilter.Put(keyValuePair.Key); err != nil {
			return nil, 0, err
		}
	}
	return beginOffsetByKey, offset, nil
}

func createBloomFilter(fileNamePrefix int, totalKeys int, bloomFilters *filter.BloomFilters) (*filter.BloomFilter, error) {
	bloomFilter, err := bloomFilters.NewBloomFilter(filter.BloomFilterOptions{
		Capacity:       totalKeys,
		FileNamePrefix: strconv.Itoa(fileNamePrefix),
	})
	if err != nil {
		return nil, err
	}
	return bloomFilter, nil
}
