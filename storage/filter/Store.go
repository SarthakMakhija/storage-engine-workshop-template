package filter

import (
	"github.com/edsrzf/mmap-go"
	"log"
	"os"
)

type Store struct {
	file               *os.File
	memoryMappedRegion mmap.MMap
}

func NewStore(filePath string, size int) (*Store, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	store := &Store{file: file}
	if memoryMappedRegion, err := store.memoryMap(size); err != nil {
		return nil, err
	} else {
		store.memoryMappedRegion = memoryMappedRegion
		return store, nil
	}
}

func (store *Store) memoryMap(size int) (mmap.MMap, error) {
	if err := store.file.Truncate(int64(size)); err != nil {
		return nil, err
	}
	memoryMappedRegion, err := mmap.MapRegion(store.file, size, mmap.RDWR, 0, 0)
	if err != nil {
		return nil, err
	}
	return memoryMappedRegion, nil
}

func (store *Store) SetBit(index uint64, mask byte) {
	store.memoryMappedRegion[index] = store.memoryMappedRegion[index] | mask
}

func (store *Store) GetByte(index uint64) byte {
	return store.memoryMappedRegion[index]
}

func (store *Store) Size() int {
	return len(store.memoryMappedRegion)
}

func (store *Store) Close() {
	err := store.file.Close()
	if err != nil {
		log.Default().Println("Error while closing the file " + store.file.Name())
	}
}
