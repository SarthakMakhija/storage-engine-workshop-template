package log

import (
	"errors"
	"fmt"
	"log"
	"os"
)

type Store struct {
	file *os.File
	size int64
}

func NewStore(filePath string) (*Store, error) {
	storeFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := storeFile.Stat()
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile, size: stat.Size()}, nil
}

func (store *Store) Append(persistentLogSlice PersistentLogSlice) error {
	////Assignment:WAL:3:append to the file
	bytesWritten, err := store.file.Write(nil)
	if err != nil {
		return err
	}
	if bytesWritten <= 0 {
		return errors.New("could not append persistentLogSlice to WAL")
	}
	if bytesWritten < persistentLogSlice.Size() {
		return errors.New(fmt.Sprintf("%v bytes written to WAL, where as total bytes that should have been written are %v", bytesWritten, persistentLogSlice.Size()))
	}
	store.size = store.size + int64(bytesWritten)
	return nil
}

func (store *Store) ReadAll() ([]TransactionalEntry, error) {
	var entries []TransactionalEntry
	var currentOffset int64 = 0

	for currentOffset < store.size {
		transactionalEntries, nextOffset, err := store.readAt(currentOffset)
		if err != nil {
			return nil, err
		}
		entries = append(entries, transactionalEntries)
		currentOffset = nextOffset
	}
	return entries, nil
}

func (store *Store) Size() int64 {
	return store.size
}

func (store *Store) Close() {
	err := store.file.Close()
	if err != nil {
		log.Default().Println("Error while closing the file " + store.file.Name())
	}
}

func (store *Store) readAt(offset int64) (TransactionalEntry, int64, error) {
	transactionEntrySizeBytes := make([]byte, reservedTransactionHeaderSize)
	_, err := store.file.ReadAt(transactionEntrySizeBytes, offset)
	if err != nil {
		return TransactionalEntry{}, -1, err
	}
	transactionEntrySize := TransactionalEntrySize(transactionEntrySizeBytes)
	transactionEntryBytes := make([]byte, transactionEntrySize)
	offset = offset + int64(reservedTransactionHeaderSize)

	bytesRead, err := store.file.ReadAt(transactionEntryBytes, offset)
	if err != nil {
		return TransactionalEntry{}, -1, err
	}
	offset = offset + int64(bytesRead)
	pairs := NewPersistentLogSliceKeyValuePairs(transactionEntryBytes)

	transactionStatusBytes := make([]byte, reservedTransactionStatusSize)
	_, err = store.file.ReadAt(transactionStatusBytes, offset)
	if err != nil {
		return TransactionalEntry{}, -1, err
	}
	offset = offset + int64(reservedTransactionStatusSize)
	return TransactionalEntry{keyValuePairs: pairs, status: TransactionStatusFrom(transactionStatusBytes)}, offset, nil
}
