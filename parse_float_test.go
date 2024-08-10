package main

import (
	"math/rand"
	"strconv"
	"testing"
)

const NUM_CASES = 1_000_000

func generateFloats() [][]byte {
	selection := [][]byte{
		[]byte("12.3"),
		[]byte("-45.6"),
		[]byte("78.9"),
		[]byte("-01.2"),
	}

	floats := make([][]byte, NUM_CASES)
	for i := 0; i < NUM_CASES; i++ {
		floats[i] = selection[rand.Intn(4)]
	}
	return floats
}

var testCases = generateFloats()

func BenchmarkParseFloatBytes(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strconv.ParseFloat(string(testCases[i%len(testCases)]), 64)
	}
}

func BenchmarkParseFloatBytesAlt(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseFloatBytesAlt(testCases[i%len(testCases)])
	}
}
