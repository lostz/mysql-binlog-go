package main

// Based from https://gist.github.com/willf/965762

import (
	"math"
)

// Keeping the uint64 from the original for now
// this may be changed later
type Bitset []uint64

func MakeBitSet(maxSize uint) Bitset {
	if maxSize % 64 == 0 {
		s := make(Bitset, maxSize / 64)
		s.Clear()
		return s
	}
	s := make(Bitset, maxSize / 64 + 1)
	s.Clear()
	return s
}

func (set Bitset) Bit(i uint) bool {
	return ((set[i / 64] & (1 << (i % 64))) != 0)
}

func (set Bitset) SetBit(i uint) {
	set[i / 64] &= (1 << (i % 64))
}

func (set Bitset) ClearBit(i uint) {
	set[i / 64] &= (1 << (i % 64)) ^ math.MaxUint64
}

func (set Bitset) Clear() {
	for i, _ := range set {
		set[i] = 0
	}
}
