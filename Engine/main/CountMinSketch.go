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

type CountMinSketch struct{
	M uint
	K uint
	HashFunctions []hash.Hash32
	Timestamp []uint32
	Table [][]uint

}

func(cms *CountMinSketch) createTable(){
	cms.Table = make([][]uint, cms.K)
	for i:= range cms.Table{
		cms.Table[i] = make([]uint, cms.M)
	}
	for i:=0;i<len(cms.Table);i++{
		for j:=0;j<len(cms.Table[i]);j++{
			cms.Table[i][j] = 0
		}
	}
}
//TODO: SERIJALIZOVATI I DESERIJALIZOVATI CMS

func(cms *CountMinSketch) serializeCMS(filename string) bool{
	file, err := os.Create(filename)
	if err != nil{
		return false
	}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms.M)
	err = encoder.Encode(cms.K)
	err = encoder.Encode(cms.Table)
	for i:=0;i<len(cms.HashFunctions);i++{
		err = encoder.Encode(cms.HashFunctions[i])
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

func(cms *CountMinSketch) deserializeCMS(filename string) bool{

	file, err := os.Open(filename)
	if err != nil{
		panic(err)
	}

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&cms.M)
	err = decoder.Decode(&cms.K)

	err = decoder.Decode(&cms.Table)

	for i:= uint(0);i<cms.K;i++{
		h := murmur3.New32WithSeed(cms.Timestamp[i])
		err = decoder.Decode(h)
		cms.HashFunctions = append(cms.HashFunctions, h)
		if err != nil && err != io.EOF{
			panic(err)
		}
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}
	return true
}

func(cms *CountMinSketch) addElement(element string){
	for i:=0;i<len(cms.HashFunctions);i++{
		cms.HashFunctions[i].Reset()
		_, err := cms.HashFunctions[i].Write([]byte(element))
		if err != nil {
			return 
		}
		index := cms.HashFunctions[i].Sum32() % uint32(cms.M)
		cms.Table[i][index] += 1
	}
}

func(cms *CountMinSketch) frequency(element string) uint{
	R := make([]uint, cms.K, cms.K)
	for i:=0;i<len(cms.HashFunctions);i++{
		cms.HashFunctions[i].Reset()
		_, err := cms.HashFunctions[i].Write([]byte(element))
		if err != nil {
			return 0
		}
		index := cms.HashFunctions[i].Sum32() % uint32(cms.M)
		R[i] = cms.Table[i][index]
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

func CreateHashFunctionsCMS(k uint32) ([]hash.Hash32, []uint32) {
	h := []hash.Hash32{}
	timestamp := []uint32{}
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		timestamp = append(timestamp, ts + i)
		h = append(h, murmur3.New32WithSeed(ts+i))
	}
	return h, timestamp
}

