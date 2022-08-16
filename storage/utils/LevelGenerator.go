package utils

import (
	"math/rand"
	"time"
)

type LevelGenerator struct {
	maxLevel   int
	skipFactor int
}

func NewLevelGenerator(maxLevel int) LevelGenerator {
	rand.Seed(time.Now().UnixNano())
	return LevelGenerator{
		maxLevel:   maxLevel,
		skipFactor: 2,
	}
}

func (levelGenerator LevelGenerator) Generate() int {
	level := 1
	newRandom := rand.Float64()
	for level < levelGenerator.GetMaxLevel() && newRandom < 1.0/float64(levelGenerator.skipFactor) {
		level = level + 1
		newRandom = rand.Float64()
	}
	return level
}

func (levelGenerator LevelGenerator) GetMaxLevel() int {
	return levelGenerator.maxLevel
}
