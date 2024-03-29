package memory

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/utils"
)

type MemTable struct {
	//head           *Node
	inMemoryMap    *InMemoryMap
	size           uint64
	totalKeys      int
	keyComparator  comparator.KeyComparator
	levelGenerator utils.LevelGenerator
}

func NewMemTable(maxLevel int, keyComparator comparator.KeyComparator) *MemTable {
	return &MemTable{
		//head:           NewNode(model.NilSlice(), model.NilSlice(), maxLevel),
		inMemoryMap:    NewInMemoryMap(),
		size:           0,
		keyComparator:  keyComparator,
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) Put(key, value model.Slice) bool {
	if ok := memTable.inMemoryMap.Put(key, value); ok {
		memTable.size = memTable.size + uint64(key.Size()) + uint64(value.Size())
		memTable.totalKeys = memTable.totalKeys + 1
		return ok
	}
	return false
}

func (memTable *MemTable) Get(key model.Slice) model.GetResult {
	return memTable.inMemoryMap.Get(key)
}

func (memTable *MemTable) MultiGet(keys []model.Slice) (model.MultiGetResult, []model.Slice) {
	return memTable.inMemoryMap.MultiGet(keys)
}

func (memTable *MemTable) AllKeyValues() []model.KeyValuePair {
	return memTable.inMemoryMap.AllKeyValues(memTable.keyComparator)
}

func (memTable *MemTable) TotalSize() uint64 {
	return memTable.size
}

func (memTable *MemTable) TotalKeys() int {
	return memTable.totalKeys
}
