package log

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sort"
)

type WAL struct {
	directory       string
	activeSegment   *Segment
	passiveSegments []*Segment
}

const subDirectoryPermission = 0744

func NewLog(directory string, segmentMaxSizeBytes uint64) (*WAL, error) {
	if len(directory) == 0 {
		return nil, errors.New("directory can not be empty while creating new log")
	}
	subDirectory := path.Join(directory, "wal")
	if _, err := os.Stat(subDirectory); os.IsNotExist(err) {
		if err := os.Mkdir(subDirectory, subDirectoryPermission); err != nil {
			return nil, err
		}
	}
	log := &WAL{directory: subDirectory}
	if err := log.init(segmentMaxSizeBytes); err != nil {
		return nil, err
	} else {
		return log, nil
	}
}

func (log *WAL) BeginTransactionHeader(totalSize uint16) error {
	rollOverActiveSegment := func() error {
		log.passiveSegments = append(log.passiveSegments, log.activeSegment)
		return log.openActiveSegmentAt(log.activeSegment.LastOffset(), log.activeSegment.maxSizeBytes)
	}
	appendToActiveSegment := func() error {
		if err := log.activeSegment.Append(NewPersistentLogSliceTransactionHeader(totalSize)); err != nil {
			return err
		}
		return nil
	}
	if log.activeSegment.IsMaxed() {
		if err := rollOverActiveSegment(); err != nil {
			return err
		}
	}
	return appendToActiveSegment()
}

func (log *WAL) Append(persistentLogSlice PersistentLogSlice) error {
	appendToActiveSegment := func() error {
		//Assignment:WAL:1:append to the log
		var err error = nil
		if err != nil {
			return err
		}
		return nil
	}
	return appendToActiveSegment()
}

func (log *WAL) MarkTransactionWith(transactionStatus TransactionStatus) error {
	return log.activeSegment.Append(PersistentLogSlice{contents: transactionStatus.Marshal()})
}

func (log *WAL) ReadAll() ([]TransactionalEntry, error) {
	allSegments := func() []*Segment {
		copiedPassiveSegments := make([]*Segment, len(log.passiveSegments))
		copy(copiedPassiveSegments, log.passiveSegments)

		return append(copiedPassiveSegments, log.activeSegment)
	}
	readAllSegments := func() ([]TransactionalEntry, error) {
		var allEntries []TransactionalEntry
		for _, segment := range allSegments() {
			if transactionalEntries, err := segment.ReadAll(); err != nil {
				return nil, err
			} else {
				allEntries = append(allEntries, transactionalEntries...)
			}
		}
		return allEntries, nil
	}
	return readAllSegments()
}

func (log *WAL) Close() {
	log.activeSegment.Close()
	for _, segment := range log.passiveSegments {
		segment.Close()
	}
}

func (log *WAL) init(segmentMaxSizeBytes uint64) error {
	sortedSegmentOffsets := func() ([]int64, error) {
		segmentFiles, err := ioutil.ReadDir(log.directory)
		if err != nil {
			return nil, err
		}
		var baseOffsets []int64
		for _, file := range segmentFiles {
			baseOffsets = append(baseOffsets, parseSegmentFileName(file))
		}
		sort.Slice(baseOffsets, func(i, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		})
		return baseOffsets, nil
	}
	reOpenSegments := func() error {
		offsets, err := sortedSegmentOffsets()
		if err != nil {
			return err
		}
		if len(offsets) == 0 {
			return log.openActiveSegmentAt(0, segmentMaxSizeBytes)
		}
		if err := log.openActiveSegmentAt(offsets[len(offsets)-1], segmentMaxSizeBytes); err != nil {
			return err
		}
		for index := 0; index < len(offsets)-1; index++ {
			segmentOffset := offsets[index]
			if err := log.openPassiveSegmentAt(segmentOffset, segmentMaxSizeBytes); err != nil {
				return err
			}
		}
		return nil
	}
	return reOpenSegments()
}

func (log *WAL) openActiveSegmentAt(offset int64, segmentMaxSizeBytes uint64) error {
	segment, err := log.openSegmentAt(offset, segmentMaxSizeBytes)
	if err != nil {
		return err
	}
	log.activeSegment = segment
	return nil
}

func (log *WAL) openPassiveSegmentAt(offset int64, segmentMaxSizeBytes uint64) error {
	segment, err := log.openSegmentAt(offset, segmentMaxSizeBytes)
	if err != nil {
		return err
	}
	log.passiveSegments = append(log.passiveSegments, segment)
	return nil
}

func (log *WAL) openSegmentAt(offset int64, segmentMaxSizeBytes uint64) (*Segment, error) {
	segment, err := NewSegment(log.directory, offset, segmentMaxSizeBytes)
	if err != nil {
		return nil, err
	}
	return segment, nil
}
