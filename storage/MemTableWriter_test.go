package storage

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/memory"
	"storage-engine-workshop/storage/sst"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "sst")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestMemTableWriterWithSuccessAsStatus(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	key := model.NewSlice([]byte("HDD"))
	value := model.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	directory := tempDirectory()
	defer os.RemoveAll(directory)
	ssTables, _ := sst.NewSSTables(directory)

	memTableWriter := NewMemTableWriter(memTable, ssTables)
	statusChannel := memTableWriter.Write()
	status := <-statusChannel

	if status.status != SUCCESS {
		t.Fatalf("Expected memtable flush status to be SUCCESS but received %v", status)
	}
}

func TestMemTableWriterWithFailureAsStatus(t *testing.T) {
	emptyMemTable := memory.NewMemTable(10, comparator.StringKeyComparator{})

	directory := tempDirectory()
	defer os.RemoveAll(directory)
	ssTables, _ := sst.NewSSTables(directory)

	memTableWriter := NewMemTableWriter(emptyMemTable, ssTables)
	statusChannel := memTableWriter.Write()
	status := <-statusChannel

	if status.status != FAILURE {
		t.Fatalf("Expected memtable flush status to be FAILURE but received %v", status.status)
	}
}
