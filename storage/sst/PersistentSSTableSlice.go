package sst

import (
	"encoding/binary"
	"storage-engine-workshop/db/model"
	"unsafe"
)

var (
	bigEndian         = binary.BigEndian
	reservedTotalSize = unsafe.Sizeof(uint32(0))
	reservedKeySize   = unsafe.Sizeof(uint32(0))
)

type PersistentSSTableSlice struct {
	contents []byte
}

var emptyPersistentSSTableSlice = PersistentSSTableSlice{contents: []byte{}}

func EmptyPersistentSSTableSlice() PersistentSSTableSlice {
	return emptyPersistentSSTableSlice
}
func NewPersistentSSTableSlice(keyValuePair model.KeyValuePair) PersistentSSTableSlice {
	return marshal(keyValuePair)
}

func NewPersistentSSTableSliceKeyValuePair(contents []byte) (PersistentSSTableSlice, PersistentSSTableSlice) {
	return unmarshal(contents)
}

func (persistentLogSlice PersistentSSTableSlice) GetPersistentContents() []byte {
	return persistentLogSlice.contents
}

func (persistentLogSlice PersistentSSTableSlice) GetSlice() model.Slice {
	return model.NewSlice(persistentLogSlice.GetPersistentContents())
}

func (persistentLogSlice PersistentSSTableSlice) Size() int {
	return len(persistentLogSlice.contents)
}

func ActualTotalSize(bytes []byte) uint32 {
	return bigEndian.Uint32(bytes)
}

func marshal(keyValuePair model.KeyValuePair) PersistentSSTableSlice {
	reservedTotalSize, reservedKeySize := reservedTotalSize, reservedKeySize
	actualTotalSize :=
		len(keyValuePair.Key.GetRawContent()) +
			len(keyValuePair.Value.GetRawContent()) +
			int(reservedKeySize) +
			int(reservedTotalSize)

	//The way keyValuePair is encoded is: 4 bytes for totalSize | 4 bytes for keySize | Key content | Value content
	bytes := make([]byte, actualTotalSize)
	offset := 0

	bigEndian.PutUint32(bytes, uint32(actualTotalSize))
	offset = offset + int(reservedTotalSize)

	bigEndian.PutUint32(bytes[offset:], uint32(len(keyValuePair.Key.GetRawContent())))
	offset = offset + int(reservedKeySize)

	copy(bytes[offset:], keyValuePair.Key.GetRawContent())
	offset = offset + len(keyValuePair.Key.GetRawContent())

	copy(bytes[offset:], keyValuePair.Value.GetRawContent())
	return PersistentSSTableSlice{contents: bytes}
}

func unmarshal(bytes []byte) (PersistentSSTableSlice, PersistentSSTableSlice) {
	bytes = bytes[reservedTotalSize:]
	keySize := bigEndian.Uint32(bytes)
	keyEndOffset := uint32(reservedKeySize) + keySize

	return PersistentSSTableSlice{contents: bytes[reservedKeySize:keyEndOffset]}, PersistentSSTableSlice{contents: bytes[keyEndOffset:]}
}
