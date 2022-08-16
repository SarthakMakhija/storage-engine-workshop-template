package db

import (
	"storage-engine-workshop/db/model"
)

type PutRequest struct {
	Batch           *Batch
	ResponseChannel chan error
}

type GetRequest struct {
	Key             model.Slice
	ResponseChannel chan model.GetResult
}

type MultiGetRequest struct {
	Keys            []model.Slice
	ResponseChannel chan []model.GetResult
}
