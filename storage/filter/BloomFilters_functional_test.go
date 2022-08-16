package filter

import (
	"os"
	"storage-engine-workshop/db/model"
	"strconv"
	"testing"
)

func TestAdds500KeysAndChecksForTheirPositiveExistence(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       500,
		FileNamePrefix: "1",
	})

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	for count := 1; count <= 500; count++ {
		_ = bloomFilter.Put(keyUsing(count))
	}

	for count := 1; count <= 500; count++ {
		contains := bloomFilter.Has(keyUsing(count))
		if contains == false {
			t.Fatalf("Expected key %v to be present but was not", keyUsing(count).AsString())
		}
	}
}

func TestAdds500KeysAndChecksForTheExistenceOfMissingKeys(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       500,
		FileNamePrefix: "2",
	})

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	for count := 1; count <= 500; count++ {
		_ = bloomFilter.Put(keyUsing(count))
	}

	for count := 1; count <= 500; count++ {
		contains := bloomFilter.Has(keyUsing(count * 600))
		if contains == true {
			t.Fatalf("Expected key %v to be missing but was present", keyUsing(count*600).AsString())
		}
	}
}
