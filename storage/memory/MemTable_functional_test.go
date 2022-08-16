package memory

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"strconv"
	"testing"
)

func TestPut500KeysValuesAndGetByKeys(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	for count := 1; count <= 500; count++ {
		memTable.Put(keyUsing(count), valueUsing(count))
	}

	for count := 1; count <= 500; count++ {
		getResult := memTable.Get(keyUsing(count))
		expectedValue := valueUsing(count)

		if getResult.Value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
		}
	}
}
