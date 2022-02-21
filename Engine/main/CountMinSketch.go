package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type CountMinSketch struct{
	M uint
	K uint
	Table [][]uint
	Timestamp uint
	HashFunctions []hash.Hash32

}

func(cms *CountMinSketch) initializeCountMinSketch(epsilon, delta float64) bool{
	cms.M = CalculateMCMS(epsilon)
	cms.K = CalculateKCMS(delta)
	cms.HashFunctions, cms.Timestamp = CreateHashFunctionsCMS(cms.K, 0)
	cms.createTable()
	return true
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

func(cms *CountMinSketch) encodeCmsToBytes() bytes.Buffer{
	cms.HashFunctions = nil
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(cms)
	if err != nil{
		panic(err)
	}

	return buffer
}
func(cms *CountMinSketch) decodeCMSFromBytes(cmsBytes bytes.Buffer) bool{

	decoder := gob.NewDecoder(&cmsBytes)
	err := decoder.Decode(&cms)
	if err != nil {
		fmt.Println(err)
	}
	cms.HashFunctions, _ = CreateHashFunctionsCMS(cms.K, cms.Timestamp)
	return true
}


func(cms *CountMinSketch) serializeCMS(filename string) bool{
	cms.HashFunctions = nil
	file, err := os.Create(filename)
	if err != nil{
		return false
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)
	if err != nil {
		panic(err)
	}
	err = file.Close()
	return true
}

func(cms *CountMinSketch) deserializeCMS(filename string) bool{
	file, err := os.Open(filename)
	if err != nil{
		panic(err)
	}

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&cms)
	if err != nil {
		fmt.Println(err)
	}
	cms.HashFunctions, _ = CreateHashFunctionsCMS(cms.K, cms.Timestamp)
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

func CreateHashFunctionsCMS(k uint, t uint) ([]hash.Hash32, uint) {
	h := []hash.Hash32{}
	if t == 0 {
		t = uint(time.Now().Unix())
	}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(t+i)))
	}
	return h, t
}

