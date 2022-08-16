package db

type KeyValueDb struct {
	executor *RequestExecutor
}

func NewKeyValueDb(configuration Configuration) (*KeyValueDb, error) {
	workSpace, err := newWorkSpace(configuration)
	if err != nil {
		return nil, err
	}
	return &KeyValueDb{
		executor: newRequestExecutor(workSpace),
	}, nil
}

func (db *KeyValueDb) newTransaction() *Transaction {
	return newTransaction(db.executor)
}

func (db *KeyValueDb) newReadonlyTransaction() ReadonlyTransaction {
	return newReadonlyTransaction(db.executor)
}
