package sst

import (
	"errors"
	"fmt"
	"os"
)

type Store struct {
	file *os.File
}

func NewStore(filePath string) (*Store, error) {
	storeFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile}, nil
}

func (store *Store) WriteAt(bytes []byte, offset int64) (int, error) {
	bytesWritten, err := store.file.WriteAt(bytes, offset)
	if err != nil {
		return 0, err
	}
	if bytesWritten <= 0 {
		return 0, errors.New(fmt.Sprintf("%v bytes written to SSTable, could not dump content to SSTable", bytesWritten))
	}
	if bytesWritten < len(bytes) {
		return 0, errors.New(fmt.Sprintf("%v bytes written to SSTable, where as total bytes that should have been written are %v", bytesWritten, len(bytes)))
	}
	return bytesWritten, nil
}

func (store *Store) ReadAt(bytes []byte, offset int64) (int, error) {
	return store.file.ReadAt(bytes, offset)
}

func (store *Store) SeekFromBeginning(offset int64) (int64, error) {
	return store.file.Seek(offset, 0)
}

func (store *Store) Size() (int64, error) {
	stat, err := store.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (store *Store) Sync() error {
	return store.file.Sync()
}
