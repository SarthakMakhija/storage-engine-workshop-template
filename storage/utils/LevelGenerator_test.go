package utils

import "testing"

func TestShouldGenerateLevelGreaterThanEqualTo1(t *testing.T) {
	levelGenerator := NewLevelGenerator(10)
	level := levelGenerator.Generate()

	if level < 1 {
		t.Fatalf("Expected generated level to be greater than or equal to 1 but received %v", level)
	}
}
