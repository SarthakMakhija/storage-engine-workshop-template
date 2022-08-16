package comparator

import (
	"storage-engine-workshop/db/model"
)

type KeyComparator interface {
	Compare(one model.Slice, other model.Slice) int
}
