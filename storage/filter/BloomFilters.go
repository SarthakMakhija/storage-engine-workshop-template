package filter

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"storage-engine-workshop/db/model"
	"strconv"
	"strings"
)

type BloomFilters struct {
	directory         string
	falsePositiveRate float64
	filters           []*BloomFilter
}

type BloomFilterOptions struct {
	Capacity       int
	DataSize       int
	FileNamePrefix string
}

const subDirectoryPermission = 0744

func NewBloomFilters(directory string, falsePositiveRate float64) (*BloomFilters, error) {
	if len(directory) == 0 {
		return nil, errors.New("bloom filter is persistent and needs a directory fileName")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, errors.New("bloom filter false positive rate must be between 0 and 1")
	}
	subDirectory := path.Join(directory, "bloom")
	if _, err := os.Stat(subDirectory); os.IsNotExist(err) {
		if err := os.Mkdir(subDirectory, subDirectoryPermission); err != nil {
			return nil, err
		}
	}
	filters := &BloomFilters{directory: subDirectory, falsePositiveRate: falsePositiveRate}
	if err := filters.init(); err != nil {
		return nil, err
	} else {
		return filters, nil
	}
}

func (bloomFilters *BloomFilters) NewBloomFilter(options BloomFilterOptions) (*BloomFilter, error) {
	if len(options.FileNamePrefix) == 0 {
		return nil, errors.New("bloom filter needs a prefix which will be a part of its name")
	}

	fileName := path.Join(bloomFilters.directory, bloomFilters.bloomFilterFileName(options))
	if filter, err := newBloomFilter(minCapacityToEnsureZeroFalseNegatives(options), options.DataSize, bloomFilters.falsePositiveRate, fileName); err != nil {
		return nil, err
	} else {
		bloomFilters.filters = append(bloomFilters.filters, filter)
		return filter, nil
	}
}

func (bloomFilters *BloomFilters) Close() {
	for _, bloomFilter := range bloomFilters.filters {
		bloomFilter.Close()
	}
}

func (bloomFilters *BloomFilters) Has(key model.Slice) bool {
	for _, bloomFilter := range bloomFilters.filters {
		if bloomFilter.Has(key) {
			return true
		}
	}
	return false
}

func (bloomFilters *BloomFilters) init() error {
	allBloomFilterFiles := func() ([]fs.FileInfo, error) {
		bloomFiles, err := ioutil.ReadDir(bloomFilters.directory)
		if err != nil {
			return nil, err
		}
		return bloomFiles, nil
	}
	parseFileName := func(file fs.FileInfo) BloomFilterOptions {
		fileName := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		nameParts := strings.Split(fileName, "_")
		prefix := nameParts[0]
		capacity, _ := strconv.Atoi(nameParts[1])
		dataSize, _ := strconv.Atoi(nameParts[2])

		return BloomFilterOptions{
			FileNamePrefix: prefix,
			Capacity:       capacity,
			DataSize:       dataSize,
		}
	}
	reloadAllBloomFilters := func() error {
		bloomFilterFiles, err := allBloomFilterFiles()
		if err != nil {
			return err
		}
		for _, file := range bloomFilterFiles {
			if _, err := bloomFilters.NewBloomFilter(parseFileName(file)); err != nil {
				return err
			}
		}
		return nil
	}
	return reloadAllBloomFilters()
}

func (bloomFilters *BloomFilters) bloomFilterFileName(options BloomFilterOptions) string {
	return fmt.Sprintf("%s_%v_%v.bloom", options.FileNamePrefix, options.Capacity, options.DataSize)
}

func minCapacityToEnsureZeroFalseNegatives(options BloomFilterOptions) int {
	if options.Capacity <= 10 {
		return 2 * options.Capacity
	}
	return options.Capacity
}
