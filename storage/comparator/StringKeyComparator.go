package comparator

import (
	"storage-engine-workshop/db/model"
	"strings"
)

type StringKeyComparator struct {
}

func (comparator StringKeyComparator) Compare(one model.Slice, other model.Slice) int {
	return strings.Compare(string(one.GetRawContent()), string(other.GetRawContent()))
}
