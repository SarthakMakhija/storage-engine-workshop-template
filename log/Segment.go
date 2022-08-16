package log

import (
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
)

type Segment struct {
	directory    string
	store        *Store
	baseOffSet   int64
	maxSizeBytes uint64
}

func NewSegment(directory string, baseOffset int64, maxSizeBytes uint64) (*Segment, error) {
	store, err := NewStore(path.Join(directory, fmt.Sprintf("%d%s", baseOffset, ".store")))
	if err != nil {
		return nil, err
	}
	return &Segment{
		directory:    directory,
		store:        store,
		baseOffSet:   baseOffset,
		maxSizeBytes: maxSizeBytes,
	}, nil
}

func (segment *Segment) Append(persistentLogSlice PersistentLogSlice) error {
	//Assignment:WAL:2:append to the segment
	var err error = nil
	if err != nil {
		return err
	}
	return nil
}

func (segment *Segment) ReadAll() ([]TransactionalEntry, error) {
	return segment.store.ReadAll()
}

func (segment *Segment) IsMaxed() bool {
	if segment.store.Size() >= int64(segment.maxSizeBytes) {
		return true
	}
	return false
}

func (segment *Segment) LastOffset() int64 {
	return segment.store.Size() + segment.baseOffSet
}

func (segment *Segment) Close() {
	segment.store.Close()
}

func parseSegmentFileName(file fs.FileInfo) int64 {
	offsetPrefix := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
	offset, _ := strconv.ParseUint(offsetPrefix, 10, 0)
	return int64(offset)
}
