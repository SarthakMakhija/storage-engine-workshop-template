package model

type GetResult struct {
	Key, Value Slice
	Exists     bool
}

type MultiGetResult struct {
	Values []GetResult
}

func (multiGetResult *MultiGetResult) Add(getResult GetResult) {
	multiGetResult.Values = append(multiGetResult.Values, getResult)
}
