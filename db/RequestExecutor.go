package db

import (
	"errors"
	"storage-engine-workshop/db/model"
)

type RequestExecutor struct {
	requestChannel chan interface{}
	workSpace      *Workspace
}

func newRequestExecutor(workSpace *Workspace) *RequestExecutor {
	executor := &RequestExecutor{
		requestChannel: make(chan interface{}),
		workSpace:      workSpace,
	}
	//Assignment:Concurrency:1:init the executor. Init should create a goroutine to "continuously" read from a request channel
	return executor
}

func (executor *RequestExecutor) init() {
	put := func(putRequest PutRequest) {
		//Assignment:Concurrency:2:execute the put request and send the response back
		putRequest.ResponseChannel <- errors.New("please complete the assignment")
		close(putRequest.ResponseChannel)
	}
	get := func(getRequest GetRequest) {
		//Assignment:Concurrency:3:execute the get request and send the response back
		getRequest.ResponseChannel <- model.GetResult{}
		close(getRequest.ResponseChannel)
	}
	multiGet := func(multiGetRequest MultiGetRequest) {
		//Assignment:Concurrency:4:execute the multiGet request and send the response back
		multiGetRequest.ResponseChannel <- []model.GetResult{}
		close(multiGetRequest.ResponseChannel)
	}

	go func() {
		for {
			request := <-executor.requestChannel
			if putRequest, ok := request.(PutRequest); ok {
				put(putRequest)
			} else if getRequest, ok := request.(GetRequest); ok {
				get(getRequest)
			} else if multiGetRequest, ok := request.(MultiGetRequest); ok {
				multiGet(multiGetRequest)
			}
		}
	}()
}

func (executor *RequestExecutor) put(batch *Batch) chan error {
	responseChannel := make(chan error)
	executor.requestChannel <- PutRequest{Batch: batch, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) get(key model.Slice) chan model.GetResult {
	responseChannel := make(chan model.GetResult)
	executor.requestChannel <- GetRequest{Key: key, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) multiGet(keys []model.Slice) chan []model.GetResult {
	responseChannel := make(chan []model.GetResult)
	executor.requestChannel <- MultiGetRequest{Keys: keys, ResponseChannel: responseChannel}
	return responseChannel
}
