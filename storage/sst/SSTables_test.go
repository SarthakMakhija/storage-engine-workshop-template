package sst

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/memory"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "sst")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestWritesSSTableToDisk(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	if err := ssTable.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
}

func TestWrites2SSTablesToDisk(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTableA, _ := ssTables.NewSSTable(memTable)
	ssTableB, _ := ssTables.NewSSTable(memTable)

	if err := ssTableA.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
	if err := ssTableB.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
}

func TestCreatesSSTableAndPutsKeysInBloomFilter(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTable.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	contains := ssTable.bloomFilter.Has(model.NewSlice([]byte("SDD")))

	if contains == false {
		t.Fatalf("Expected key %v to be present in bloom filter corresponding to the SSTable but was not",
			model.NewSlice([]byte("SDD")).AsString(),
		)
	}
}

func TestGetsFromSSTable(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(model.NewSlice([]byte("HDD")), comparator.StringKeyComparator{})
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected value to be %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestGetsFromSSTableContainingMultipleKeyValues(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTable.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))
	memTable.Put(model.NewSlice([]byte("Pmem")), model.NewSlice([]byte("Persistent memory")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(model.NewSlice([]byte("SDD")), comparator.StringKeyComparator{})
	if getResult.Value.AsString() != "Solid state" {
		t.Fatalf("Expected value to be %v, received %v", "Solid state", getResult.Value.AsString())
	}
}

func TestGetNonExistentKeyFromSSTableContainingMultipleKeyValues(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTable.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))
	memTable.Put(model.NewSlice([]byte("Pmem")), model.NewSlice([]byte("Persistent memory")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(model.NewSlice([]byte("Unknown")), comparator.StringKeyComparator{})
	if getResult.Exists != false {
		t.Fatalf("Expected value to be missing for key %v, but was present", "Unknown")
	}
}

func TestMultiGetsFromSSTablesBasedOnBloomFilter(t *testing.T) {
	directory := tempDirectory()
	ssTables, _ := NewSSTables(directory)
	defer os.RemoveAll(directory)

	memTableA := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTableA.Put(model.NewSlice([]byte("HDD")), model.NewSlice([]byte("Hard disk")))
	memTableA.Put(model.NewSlice([]byte("SDD")), model.NewSlice([]byte("Solid state")))

	ssTableA, _ := ssTables.NewSSTable(memTableA)
	_ = ssTableA.Write()
	ssTables.AllowSearchIn(ssTableA)

	memTableB := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTableB.Put(model.NewSlice([]byte("PMEM")), model.NewSlice([]byte("Persistent memory")))
	memTableB.Put(model.NewSlice([]byte("NVMe")), model.NewSlice([]byte("Non volatile media")))

	ssTableB, _ := ssTables.NewSSTable(memTableB)
	_ = ssTableB.Write()
	ssTables.AllowSearchIn(ssTableB)

	keys := []model.Slice{
		model.NewSlice([]byte("HDD")),
		model.NewSlice([]byte("SDD")),
		model.NewSlice([]byte("PMEM")),
		model.NewSlice([]byte("NVMe")),
	}
	expected := []model.GetResult{
		{Value: model.NewSlice([]byte("Hard disk")), Exists: true},
		{Value: model.NewSlice([]byte("Solid state")), Exists: true},
		{Value: model.NewSlice([]byte("Persistent memory")), Exists: true},
		{Value: model.NewSlice([]byte("Non volatile media")), Exists: true},
	}

	multiGetResult := ssTables.MultiGet(keys, comparator.StringKeyComparator{})
	allGetResults := multiGetResult.Values

	for index, e := range expected {
		if e.Value.AsString() != allGetResults[index].Value.AsString() {
			t.Fatalf("Expected %v, received %v", e.Value.AsString(), allGetResults[index].Value.AsString())
		}
	}
}
