package filter

import (
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"math"
	"storage-engine-workshop/db/model"
	"unsafe"
)

var aByte byte

const byteSize = int(unsafe.Sizeof(&aByte))

type BloomFilter struct {
	capacity              int
	bitVectorSize         int
	bitsPerHashFunction   int
	numberOfHashFunctions int
	dataSize              int
	fileName              string
	falsePositiveRate     float64
	store                 *Store
}

func newBloomFilter(capacity int, dataSize int, falsePositiveRate float64, fileName string) (*BloomFilter, error) {
	numberOfHashFunctions := numberOfHashFunctions(falsePositiveRate)
	bitVectorSize, bitsPerHashFunction := bitVector(capacity, falsePositiveRate, numberOfHashFunctions)
	bitVectorSize = bitVectorSize / byteSize
	bitVectorSize = bitVectorSize + byteSize

	store, err := NewStore(fileName, dataSize+bitVectorSize)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{
		capacity:              capacity,
		bitVectorSize:         bitVectorSize,
		bitsPerHashFunction:   bitsPerHashFunction,
		numberOfHashFunctions: numberOfHashFunctions,
		dataSize:              dataSize + bitVectorSize,
		fileName:              fileName,
		falsePositiveRate:     falsePositiveRate,
		store:                 store,
	}, nil
}

func (bloomFilter *BloomFilter) Put(key model.Slice) error {
	indices := bloomFilter.keyIndices(key)

	for index := 0; index < len(indices); index++ {
		bytePosition, mask := bloomFilter.bitPositionInByte(indices[index])
		if int(bytePosition) >= bloomFilter.store.Size() {
			return errors.New(fmt.Sprintf("bytePosition %v is greater than bloom filter file size for indices[index] %v", bytePosition, indices[index]))
		}
		//Assignment:Bloom filter:1:set bit
		fmt.Println(mask)
	}
	return nil
}

func (bloomFilter *BloomFilter) Has(key model.Slice) bool {
	indices := bloomFilter.keyIndices(key)

	for index := 0; index < len(indices); index++ {
		bytePosition, mask := bloomFilter.bitPositionInByte(indices[index])
		if int(bytePosition) >= bloomFilter.store.Size() {
			return false
		}
		//Assignment:Bloom filter:2:check the bit
		fmt.Println(mask)
		if false {
			return false
		}
	}
	return true
}

func (bloomFilter *BloomFilter) Close() {
	bloomFilter.store.Close()
}

func (bloomFilter *BloomFilter) bitPositionInByte(keyIndex uint64) (uint64, byte) {
	quotient, remainder := int64(keyIndex)/int64(byteSize), int64(keyIndex)%int64(byteSize)
	valueWithMostSignificantBit := int64(math.Pow(2, float64(byteSize)-1)) //128
	if remainder == 0 {
		if quotient == 0 {
			return uint64(quotient), byte(valueWithMostSignificantBit)
		}
		return uint64(quotient - 1), byte(valueWithMostSignificantBit)
	}
	return uint64(quotient), byte(0x01 << (remainder - 1))
}

// Use the hash function to get all keyIndices of the given key
func (bloomFilter *BloomFilter) keyIndices(key model.Slice) []uint64 {
	indices := make([]uint64, 0, bloomFilter.numberOfHashFunctions)
	runHash := func(key []byte, seed uint32) uint64 {
		hash, _ := murmur3.Sum128WithSeed(key, seed)
		return hash
	}
	indexForHash := func(hash uint64, index int) uint64 {
		return uint64(index*bloomFilter.bitsPerHashFunction) + (hash % uint64(bloomFilter.bitsPerHashFunction))
	}
	for index := 0; index < bloomFilter.numberOfHashFunctions; index++ {
		hash := runHash(key.GetRawContent(), uint32(index))
		indices = append(indices, indexForHash(hash, index))
	}
	return indices
}

//Calculate K
func numberOfHashFunctions(falsePositiveRate float64) int {
	return int(math.Ceil(math.Log2(1.0 / falsePositiveRate)))
}

//Calculate bitVectorSize(M) and bitsPerHashFunction(m)
func bitVector(capacity int, falsePositiveRate float64, numberOfHashFunctions int) (int, int) {
	ln2RaiseTo2 := math.Pow(math.Ln2, 2)
	bitVectorSize := int(float64(capacity) * math.Abs(math.Log(falsePositiveRate)) / ln2RaiseTo2)
	bitsPerHashFunction := bitVectorSize / numberOfHashFunctions

	return bitVectorSize, bitsPerHashFunction
}
