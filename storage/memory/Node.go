package memory

import (
	"sort"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/utils"
)

type Node struct {
	key      model.Slice
	value    model.Slice
	forwards []*Node
}

func NewNode(key model.Slice, value model.Slice, level int) *Node {
	return &Node{
		key:      key,
		value:    value,
		forwards: make([]*Node, level),
	}
}

func (node *Node) Put(key model.Slice, value model.Slice, keyComparator comparator.KeyComparator, levelGenerator utils.LevelGenerator) bool {
	current := node
	positions := make([]*Node, len(node.forwards))

	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && keyComparator.Compare(current.forwards[level].key, key) < 0 {
			//Assigment:Memtable:1:fill in the 'and' condition
			current = current.forwards[level]
		}
		positions[level] = current
	}

	current = current.forwards[0]
	if current == nil || keyComparator.Compare(current.key, key) != 0 {
		newLevel := levelGenerator.Generate()
		//Assignment:Memtable:2:generate new level
		newNode := NewNode(key, value, newLevel)
		for level := 0; level < newLevel; level++ {
			newNode.forwards[level] = positions[level].forwards[level]
			positions[level].forwards[level] = newNode
		}
		return true
	}
	return false
}

func (node *Node) Get(key model.Slice, keyComparator comparator.KeyComparator) model.GetResult {
	node, ok := node.nodeMatching(key, keyComparator)
	if ok {
		return model.GetResult{Key: key, Value: node.value, Exists: ok}
	}
	return model.GetResult{Key: key, Value: model.NilSlice(), Exists: false}
}

func (node *Node) MultiGet(keys []model.Slice, keyComparator comparator.KeyComparator) (model.MultiGetResult, []model.Slice) {
	sort.SliceStable(keys, func(i, j int) bool {
		return keyComparator.Compare(keys[i], keys[j]) < 0
	})
	currentNode := node
	response := model.MultiGetResult{}
	var missingKeys []model.Slice

	for _, key := range keys {
		targetNode, ok := currentNode.nodeMatching(key, keyComparator)
		if ok {
			response.Add(model.GetResult{Key: key, Value: targetNode.value, Exists: ok})
			currentNode = targetNode
		} else {
			missingKeys = append(missingKeys, key)
		}
	}
	return response, missingKeys
}

func (node *Node) AllKeyValues() []model.KeyValuePair {
	level, current := 0, node
	var pairs []model.KeyValuePair

	current = current.forwards[level]
	for current != nil {
		pairs = append(pairs, model.KeyValuePair{Key: current.key, Value: current.value})
		current = current.forwards[level]
	}
	return pairs
}

func (node *Node) nodeMatching(key model.Slice, keyComparator comparator.KeyComparator) (*Node, bool) {
	current := node
	for level := len(node.forwards) - 1; level >= 0; level-- {
		//Assignment:Memtable:3:fill in the 'and' condition
		for current.forwards[level] != nil && keyComparator.Compare(current.forwards[level].key, key) < 0 {
			current = current.forwards[level]
		}
	}
	current = current.forwards[0]
	if current != nil && keyComparator.Compare(current.key, key) == 0 {
		return current, true
	}
	return nil, false
}
