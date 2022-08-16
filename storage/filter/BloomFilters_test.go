package filter

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"strconv"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "bloom")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "1",
	})

	key := model.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(key) == false {
		t.Fatalf("Expected %v key to be present but was not", key.AsString())
	}
}

func TestAddsAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "2",
	})

	key := model.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(model.NewSlice([]byte("Missing"))) == true {
		t.Fatalf("Expected %v key to be missing but was present", model.NewSlice([]byte("Missing")).AsString())
	}
}

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistenceSimulatingARestart(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	aBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "1",
	})

	_ = aBloomFilter.Put(model.NewSlice([]byte("Company")))
	_ = aBloomFilter.Put(model.NewSlice([]byte("State")))

	bloomFilters.Close()
	bloomFiltersAfterRestart, _ := NewBloomFilters(directory, 0.001)

	if bloomFiltersAfterRestart.Has(model.NewSlice([]byte("Company"))) == false {
		t.Fatalf("Expected key %v to be present but was not", model.NewSlice([]byte("Company")).AsString())
	}
	if bloomFiltersAfterRestart.Has(model.NewSlice([]byte("State"))) == false {
		t.Fatalf("Expected key %v to be present but was not", model.NewSlice([]byte("State")).AsString())
	}
}

func TestAddsAKeyWithMultipleBloomFiltersAndChecksForItsPositiveExistenceSimulatingARestart(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	aBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       2,
		FileNamePrefix: "1",
	})
	_ = aBloomFilter.Put(model.NewSlice([]byte("Key-1")))
	_ = aBloomFilter.Put(model.NewSlice([]byte("Key-2")))

	bBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       2,
		FileNamePrefix: "1",
	})
	_ = bBloomFilter.Put(model.NewSlice([]byte("Key-3")))
	_ = bBloomFilter.Put(model.NewSlice([]byte("Key-4")))

	bloomFilters.Close()
	bloomFiltersAfterRestart, _ := NewBloomFilters(directory, 0.001)

	for count := 1; count <= 4; count++ {
		key := model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
		contains := bloomFiltersAfterRestart.Has(key)
		if contains == false {
			t.Fatalf("Expected key %v to be present but was not", key.AsString())
		}
	}
}
