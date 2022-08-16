package sst

import (
	"fmt"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"unsafe"
)

var (
	ReservedOffsetSize = unsafe.Sizeof(uint64(0))
)

type IndexBlock struct {
	store *Store
}

func NewIndexBlock(store *Store) *IndexBlock {
	return &IndexBlock{
		store: store,
	}
}

func (indexBlock *IndexBlock) Write(beginOffsetByKey []int64, blockBeginOffset int64, keyValuePairs []model.KeyValuePair) error {
	offset, indexBlockBeginOffset := blockBeginOffset, blockBeginOffset

	for index, keyValuePair := range keyValuePairs {
		bytes := indexBlock.marshal(keyValuePair.Key, beginOffsetByKey[index])
		//Assignment:SSTable:3:write the marshalled byte array that represents the key, in the file
		fmt.Println(bytes)
		bytesWritten := 0
		var err error = nil
		if err != nil {
			return err
		} else {
			offset = offset + int64(bytesWritten)
		}
	}
	bytes := make([]byte, ReservedOffsetSize)
	bigEndian.PutUint64(bytes, uint64(indexBlockBeginOffset))

	_, err := indexBlock.store.WriteAt(bytes, offset)
	return err
}

func (indexBlock *IndexBlock) GetKeyOffset(key model.Slice, keyComparator comparator.KeyComparator) (int64, error) {
	blockBytes, err := indexBlock.readIndexBlock()
	if err != nil {
		return -1, err
	}
	index := 0
	for index < len(blockBytes) {
		actualKeySize := bigEndian.Uint32(blockBytes[index:])
		keyBeginIndex := index + int(reservedKeySize) + int(ReservedOffsetSize)
		serializedKey := blockBytes[keyBeginIndex : keyBeginIndex+int(actualKeySize)]
		if keyComparator.Compare(model.NewSlice(serializedKey), key) == 0 {
			keyOffset := bigEndian.Uint64(blockBytes[(index + int(reservedKeySize)):])
			return int64(keyOffset), nil
		}
		index = index + int(reservedKeySize) + int(ReservedOffsetSize) + int(actualKeySize)
	}
	return -1, nil
}

func (indexBlock *IndexBlock) readIndexBlock() ([]byte, error) {
	size, _ := indexBlock.store.Size()
	offsetContainingIndexBegin := size - int64(ReservedOffsetSize)
	_, err := indexBlock.store.SeekFromBeginning(offsetContainingIndexBegin)
	if err != nil {
		return nil, err
	}
	indexBlockBeginOffsetBytes := make([]byte, int(ReservedOffsetSize))
	_, err = indexBlock.store.ReadAt(indexBlockBeginOffsetBytes, offsetContainingIndexBegin)
	if err != nil {
		return nil, err
	}
	indexBlockBeginOffset := bigEndian.Uint64(indexBlockBeginOffsetBytes[0:])
	blockBytes := make([]byte, offsetContainingIndexBegin-int64(indexBlockBeginOffset))
	_, err = indexBlock.store.ReadAt(blockBytes, int64(indexBlockBeginOffset))
	if err != nil {
		return nil, err
	}
	return blockBytes, nil
}

func (indexBlock *IndexBlock) marshal(key model.Slice, keyBeginOffset int64) []byte {
	actualTotalSize := uint64(reservedKeySize) + uint64(ReservedOffsetSize) + uint64(key.Size())

	//The way index block is encoded is: 4 bytes for keySize | 8 bytes for offsetSize | Key content
	bytes := make([]byte, actualTotalSize)
	index := 0

	bigEndian.PutUint32(bytes[index:], uint32(key.Size()))
	index = index + int(reservedKeySize)

	bigEndian.PutUint64(bytes[index:], uint64(keyBeginOffset))
	index = index + int(ReservedOffsetSize)

	copy(bytes[index:], key.GetRawContent())
	return bytes
}
