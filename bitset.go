package main

// Based from https://gist.github.com/willf/965762

import (
	"fmt"
	"math"
	"log"
)

// Keeping the uint64 from the original for now
// this may be changed later
type Bitset []uint64

func MakeBitset(maxSize uint) Bitset {
	if maxSize % 64 == 0 {
		s := make(Bitset, maxSize / 64)
		s.Clear()
		return s
	}
	s := make(Bitset, maxSize / 64 + 1)
	s.Clear()
	return s
}

func MakeBitsetFromByteArray(bytes []byte, maxSize uint) Bitset {
	if int((maxSize + 7) / 8) > len(bytes) {
		log.Fatal("Bitset maxSize and []byte length mismatch")
	}

	bitset := MakeBitset(maxSize)

	for i := uint(0); i < uint(len(bytes)); i++ {
		block := bytes[i]
		fmt.Println("block:", block)

		for j := uint(0); j < 8; j++ {
			fmt.Println("	", (block) & (1 << (j)))
			if (block) & (1 << (j)) == 1 {
				bitset.SetBit((i * 8) + j)
			}
		}
	}

	return bitset
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

// strconv doesn't force zeroes to print, so hackyness, here I come
func (set Bitset) String() string {
	s := ""

	for i := uint(0); i < uint(len(set) * 64); i++ {
		if set.Bit(i) {
			s += "1"
		} else {
			s += "0"
		}
	}

	return s
}
