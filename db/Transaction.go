package db

import (
	"errors"
	"fmt"
	"storage-engine-workshop/db/model"
)

type Transaction struct {
	executor *RequestExecutor
	batch    *Batch
}

type ReadonlyTransaction struct {
	executor *RequestExecutor
}

const (
	maxSizeAllowedBytes uint16 = 65535
)

func newTransaction(executor *RequestExecutor) *Transaction {
	return &Transaction{
		executor: executor,
		batch:    NewBatch(),
	}
}

func newReadonlyTransaction(executor *RequestExecutor) ReadonlyTransaction {
	return ReadonlyTransaction{
		executor: executor,
	}
}

func (txn *Transaction) Put(key, value model.Slice) error {
	if txn.batch.isTotalSizeGreaterThan(maxSizeAllowedBytes) {
		return errors.New(fmt.Sprintf("can not add more than the total key/value pair size %v in a transaction", maxSizeAllowedBytes))
	}
	//Assignment:Transaction:1:add the key/value to the batch
	return nil
}

func (txn *Transaction) Commit() error {
	if txn.batch.isEmpty() {
		return errors.New("nothing to commit, put key/value before committing")
	}
	//Assignment:Transaction:2:ask the request executor to handle the batch
	return errors.New("complete the assignment")
}

func (txn ReadonlyTransaction) Get(key model.Slice) model.GetResult {
	//Assignment:Transaction:3:ask the request executor to handle the get
	return model.GetResult{}
}

func (txn ReadonlyTransaction) MultiGet(keys []model.Slice) []model.GetResult {
	return <-txn.executor.multiGet(keys)
}
