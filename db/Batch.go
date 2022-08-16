package db

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/log"
)

type Batch struct {
	keyValuePairs      []model.KeyValuePair
	persistentLogSlice *log.PersistentLogSlice
}

func NewBatch() *Batch {
	return &Batch{
		keyValuePairs:      []model.KeyValuePair{},
		persistentLogSlice: &log.PersistentLogSlice{},
	}
}

func (batch *Batch) add(key, value model.Slice) {
	keyValuePair := model.KeyValuePair{Key: key, Value: value}
	batch.keyValuePairs = append(batch.keyValuePairs, keyValuePair)
	batch.persistentLogSlice.Add(log.NewPersistentLogSlice(keyValuePair))
}

func (batch *Batch) allEntriesAsPersistentLogSlice() log.PersistentLogSlice {
	return *(batch.persistentLogSlice)
}

func (batch *Batch) isEmpty() bool {
	return batch.totalPairs() == 0
}

func (batch *Batch) isTotalSizeGreaterThan(allowedSize uint16) bool {
	return batch.totalSize() > allowedSize
}

func (batch *Batch) totalSize() uint16 {
	return uint16(batch.persistentLogSlice.Size())
}

func (batch *Batch) totalPairs() int {
	return len(batch.keyValuePairs)
}
