package main

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct{
	m uint
	k uint
	hashFunctions []hash.Hash32
	table [][]uint

}

func(cms *CountMinSketch) createTable(){
	cms.table = make([][]uint, cms.k)
	for i:= range cms.table{
		cms.table[i] = make([]uint, cms.m)
	}
	for i:=0;i<len(cms.table);i++{
		for j:=0;j<len(cms.table[i]);j++{
			cms.table[i][j] = 0
		}
	}
}

func(cms *CountMinSketch) addElement(element string){
	for i:=0;i<len(cms.hashFunctions);i++{
		cms.hashFunctions[i].Reset()
		_, err := cms.hashFunctions[i].Write([]byte(element))
		if err != nil {
			return 
		}
		index := cms.hashFunctions[i].Sum32() % uint32(cms.m)
		cms.table[i][index] += 1
	}
}

func(cms *CountMinSketch) frequency(element string) uint{
	R := make([]uint, cms.k, cms.k)
	for i:=0;i<len(cms.hashFunctions);i++{
		cms.hashFunctions[i].Reset()
		_, err := cms.hashFunctions[i].Write([]byte(element))
		if err != nil {
			return 0
		}
		index := cms.hashFunctions[i].Sum32() % uint32(cms.m)
		R[i] = cms.table[i][index]
	}
	min := R[0]
	for i:=1;i<len(R);i++{
		if R[i] < min{
			min = R[i]
		}
	}

	return min
}

func CalculateMCMS(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateKCMS(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func CreateHashFunctionsCMS(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

