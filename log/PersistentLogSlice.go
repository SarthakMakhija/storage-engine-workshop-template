package log

import (
	"encoding/binary"
	"storage-engine-workshop/db/model"
	"unsafe"
)

var (
	bigEndian                           = binary.BigEndian
	reservedEntrySize                   = unsafe.Sizeof(uint32(0))
	reservedKeySize                     = unsafe.Sizeof(uint32(0))
	reservedTransactionHeaderSize uint8 = 2
	reservedTransactionStatusSize uint8 = TransactionStatusSize()
)

type TransactionalEntry struct {
	keyValuePairs []PersistentKeyValuePair
	status        TransactionStatus
}

type PersistentLogSlice struct {
	contents []byte
}

func NewPersistentLogSlice(keyValuePair model.KeyValuePair) PersistentLogSlice {
	return marshal(keyValuePair)
}

func NewPersistentLogSliceKeyValuePairs(contents []byte) []PersistentKeyValuePair {
	return unmarshal(contents)
}

func NewPersistentLogSliceTransactionHeader(totalSize uint16) PersistentLogSlice {
	bytes := make([]byte, reservedTransactionHeaderSize)
	bigEndian.PutUint16(bytes, totalSize)
	return PersistentLogSlice{contents: bytes}
}

func (persistentLogSlice PersistentLogSlice) GetPersistentContents() []byte {
	return persistentLogSlice.contents
}

func (persistentLogSlice PersistentLogSlice) GetSlice() model.Slice {
	return model.NewSlice(persistentLogSlice.GetPersistentContents())
}

func (persistentLogSlice PersistentLogSlice) Size() int {
	return len(persistentLogSlice.contents)
}

func (persistentLogSlice *PersistentLogSlice) Add(other PersistentLogSlice) {
	persistentLogSlice.contents = append(persistentLogSlice.contents, other.contents...)
}

func TransactionalEntrySize(bytes []byte) uint16 {
	return bigEndian.Uint16(bytes)
}

func marshal(keyValuePair model.KeyValuePair) PersistentLogSlice {
	entrySize :=
		len(keyValuePair.Key.GetRawContent()) +
			len(keyValuePair.Value.GetRawContent()) +
			int(reservedKeySize) +
			int(reservedEntrySize)

	//The way PutCommand is encoded is: 4 bytes for entrySize | 4 bytes for keySize | Key content | Value content
	bytes := make([]byte, entrySize)
	offset := 0

	bigEndian.PutUint32(bytes, uint32(entrySize))
	offset = offset + int(reservedEntrySize)

	bigEndian.PutUint32(bytes[offset:], uint32(len(keyValuePair.Key.GetRawContent())))
	offset = offset + int(reservedKeySize)

	copy(bytes[offset:], keyValuePair.Key.GetRawContent())
	offset = offset + len(keyValuePair.Key.GetRawContent())

	copy(bytes[offset:], keyValuePair.Value.GetRawContent())
	return PersistentLogSlice{contents: bytes}
}

func unmarshal(bytes []byte) []PersistentKeyValuePair {
	var keyValuePairs []PersistentKeyValuePair

	length := uint32(len(bytes))
	var index uint32 = 0
	for index < length {
		entrySize := bigEndian.Uint32(bytes[index:])
		endIndex := index + entrySize

		index = index + uint32(reservedEntrySize)
		keySize := bigEndian.Uint32(bytes[index:])
		index = index + uint32(reservedKeySize)

		keyEndOffset := index + keySize
		key := bytes[index:keyEndOffset]
		index = index + uint32(len(key))

		valueSize := endIndex - keyEndOffset
		value := bytes[index : index+valueSize]
		index = index + uint32(len(value))

		keyValuePairs = append(keyValuePairs,
			PersistentKeyValuePair{
				Key:   PersistentLogSlice{contents: key},
				Value: PersistentLogSlice{contents: value},
			},
		)
	}
	return keyValuePairs
}
