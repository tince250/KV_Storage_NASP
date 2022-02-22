package main

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type BloomFilter struct{
	M uint
	K uint
	BitSet []byte
	Timestamp uint
	HashFunctions []hash.Hash32

}

func(bf *BloomFilter) initializeBloomFilter(expectedElements int, falsePositiveRate float64) bool{
	bf.M = CalculateM(expectedElements, falsePositiveRate)
	bf.K = CalculateK(expectedElements, bf.M)
	bf.HashFunctions, bf.Timestamp = CreateHashFunctions(bf.K, 0)
	bf.createBitSet()
	return true
}

func(bf *BloomFilter) createBitSet(){
	bf.BitSet = make([]byte, bf.M, bf.M)
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
	err = decoder.Decode(&bf)
	if err != nil {
		fmt.Println(err)
	}
	
	bf.HashFunctions, _ = CreateHashFunctions(bf.K, bf.Timestamp)
	err = file.Close()
	if err != nil {
		panic(err)
	}
}


func(bf *BloomFilter) encodeFilter(filename string) bool{
	
	bf.HashFunctions = nil
	file, err := os.Create(filename)
	if err != nil{
		return false
	}
	
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	if err != nil {
		panic(err)
	}
	
	err = file.Close()
	return true

}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
	
}

func CalculateK(expectedElements int, m uint) uint {
	
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
	
}

func CreateHashFunctions(k uint, t uint) ([]hash.Hash32, uint) {
	
	h := []hash.Hash32{}
	if t == 0 {
		t = uint(time.Now().Unix())
	}
	
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(t+i)))
	}
	
	return h, t
}
