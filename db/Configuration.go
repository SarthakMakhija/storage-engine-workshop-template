package db

import "storage-engine-workshop/storage/comparator"

type Configuration struct {
	directory           string
	segmentMaxSizeBytes uint64
	bufferSizeBytes     uint64
	keyComparator       comparator.KeyComparator
}

func NewConfiguration(directory string, segmentMaxSizeBytes, bufferSizeBytes uint64, keyComparator comparator.KeyComparator) Configuration {
	return Configuration{
		directory:           directory,
		segmentMaxSizeBytes: segmentMaxSizeBytes,
		bufferSizeBytes:     bufferSizeBytes,
		keyComparator:       keyComparator,
	}
}
