package comparator

import (
	"bytes"
	"storage-engine-workshop/db/model"
)

type ByteWiseComparator struct {
}

func (comparator ByteWiseComparator) Compare(one model.Slice, other model.Slice) int {
	return bytes.Compare(one.GetRawContent(), other.GetRawContent())
}
