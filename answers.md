### WAL

- Assignment:WAL:1:append to the log
  - log.activeSegment.Append(persistentLogSlice)

- Assignment:WAL:2:append to the segment
  - segment.store.Append(persistentLogSlice)

- Assignment:WAL:3:append to the file
  - persistentLogSlice.GetPersistentContents()


### Memtable
- Assigment:Memtable:1:fill in the 'and' condition
  - keyComparator.Compare(current.forwards[level].key, key) < 0

- Assignment:Memtable:2:generate new level
  - levelGenerator.Generate()

- Assignment:Memtable:3:fill in the 'and' condition
  - keyComparator.Compare(current.forwards[level].key, key) < 0 

### Bloom filter
- Assignment:Bloom filter:1:set bit
  - bloomFilter.store.SetBit(bytePosition, mask)

- Assignment:Bloom filter:2:check the bit
  - bloomFilter.store.GetByte(bytePosition)&mask == 0

### SSTable
- Assignment:SSTable:1:get all key value pairs
  - memTable.AllKeyValues()

- Assignment:SSTable:2:capture the begin-offset of the key to be used in index block
    - beginOffsetByKey[index] = offset

- Assignment:SSTable:3:write the marshalled byte array that represents the key, in the file
    - indexBlock.store.WriteAt(bytes, offset)

- Assignment:SSTable:4:get the offset of the key by using the newly created index block
    - indexBlock.GetKeyOffset(key, keyComparator)

- Assignment:SSTable:5:read at the offset obtained after completing - Assignment:SSTable:4
    - ssTable.readAt(keyOffset)

### Concurrency
- Assignment:Concurrency:1:init the executor. Init should create a goroutine to "continuously" read from a request channel
    - executor.init()

- Assignment:Concurrency:2:execute the put request and send the response back
    - putRequest.ResponseChannel <- executor.workSpace.put(putRequest.Batch)

- Assignment:Concurrency:3:execute the get request and send the response back
    - getRequest.ResponseChannel <- executor.workSpace.get(getRequest.Key)

- Assignment:Concurrency:4:execute the multiGet request and send the response back
    - multiGetRequest.ResponseChannel <- executor.workSpace.multiGet(multiGetRequest.Keys)

### Transaction
- Assignment:Transaction:1:add the key/value to the batch
    - txn.batch.add(key, value)

- Assignment:Transaction:2:ask the request executor to handle the batch
    - <-txn.executor.put(txn.batch)

- Assignment:Transaction:3:ask the request executor to handle the get
    - <-txn.executor.get(key)

- Assignment:Transaction:4:write the transaction header
    - workspace.wal.BeginTransactionHeader(batch.totalSize())

- Assignment:Transaction:5:write the transaction data
    - workspace.wal.Append(batch.allEntriesAsPersistentLogSlice());

- Assignment:Transaction:5:write the transaction footer
    - workspace.wal.MarkTransactionWith(log.TransactionStatusSuccess())

- Assignment:Transaction:6:get from memtables by using `get`
  - var getResult model.GetResult = get(memTable)

- Assignment:Transaction:7:get from sstables
  - workspace.ssTables.Get(key, workspace.configuration.keyComparator)