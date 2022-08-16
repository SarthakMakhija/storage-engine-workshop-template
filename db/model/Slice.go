package model

type Slice struct {
	Contents []byte
}

var emptySlice = Slice{}

func NilSlice() Slice {
	return emptySlice
}

func NewSlice(contents []byte) Slice {
	return Slice{Contents: contents}
}

func (slice Slice) GetRawContent() []byte {
	return slice.Contents
}

func (slice Slice) AsString() string {
	return string(slice.Contents)
}

func (slice Slice) Size() int {
	return len(slice.Contents)
}
