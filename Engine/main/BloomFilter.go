package main

import (
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"io"
	"math"
	"os"
	"time"
)

type BloomFilter struct{
	M uint
	K uint32
	BitSet []int
	Timestamp []uint32
	HashFunctions []hash.Hash32

}

func(bf *BloomFilter) initializeBloomFilter(expectedElements int, falsePositiveRate float64) bool{
	bf.M = CalculateM(expectedElements, falsePositiveRate)
	bf.K = CalculateK(expectedElements, bf.M)
	bf.HashFunctions, bf.Timestamp = CreateHashFunctions(bf.K)
	bf.createBitSet()
	return true
}

func(bf *BloomFilter) createBitSet(){
	bf.BitSet = make([]int, bf.M, bf.M)
}

func(bf *BloomFilter) addElement(element []byte){
	for i:=0;i<len(bf.HashFunctions);i++{
		bf.HashFunctions[i].Reset()
		_, err := bf.HashFunctions[i].Write(element)
		if err != nil {
			return
		}
		index := bf.HashFunctions[i].Sum32() % uint32(bf.M)
		bf.BitSet[index] = 1

	}
}
func(bf *BloomFilter) exists(element []byte) bool{
	for i:=0;i<len(bf.HashFunctions);i++{
		bf.HashFunctions[i].Reset()
		_, err := bf.HashFunctions[i].Write(element)
		if err != nil {
			return false
		}
		index := bf.HashFunctions[i].Sum32() % uint32(bf.M)
		if bf.BitSet[index] == 0{
			return false
		}
	}
	return true
}

func (bf *BloomFilter)decodeFilter(filename string){
	file, err := os.Open(filename)
	if err != nil{
		panic(err)
	}

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bf.M)
	err = decoder.Decode(&bf.K)

	err = decoder.Decode(&bf.BitSet)
	err = decoder.Decode(&bf.Timestamp)


	for i:= uint32(0);i<bf.K;i++{
		h := murmur3.New32WithSeed(bf.Timestamp[i])

		err = decoder.Decode(h)
		bf.HashFunctions = append(bf.HashFunctions, h)
		if err != nil && err != io.EOF{
			panic(err)
		}
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}
}


func(bf *BloomFilter) encodeFilter(filename string) bool{
	file, err := os.Create(filename)
	if err != nil{
		return false
	}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf.M)
	err = encoder.Encode(bf.K)
	err = encoder.Encode(bf.BitSet)
	err = encoder.Encode(bf.Timestamp)
	for i:=0;i<len(bf.HashFunctions);i++{
		err = encoder.Encode(bf.HashFunctions[i])
		if err != nil{
			return false
		}
	}
	err = file.Close()
	if err != nil{
		return false
	}
	return true

}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint32 {
	return uint32(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint32) ([]hash.Hash32, []uint32) {
	h := []hash.Hash32{}
	timestamp := []uint32{}
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		timestamp = append(timestamp, ts + i)
		h = append(h, murmur3.New32WithSeed(ts+i))
	}
	return h, timestamp
}
