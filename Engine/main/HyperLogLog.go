package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strconv"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)


func handleError(err error){
	if err != nil{
		panic(err)
	}
}

type HLL struct {
	M   uint64 //duzina niza
	P   uint8 //preciznost
	Reg []uint8 // duzine m i unose se vrednosti poslednjih bita, tj oni su baketi(obicni nizovi)
}
func (hll *HLL) createHLL(p uint8) bool{
	if p<HLL_MIN_PRECISION || p>HLL_MAX_PRECISION{
		fmt.Println("Preciznost nije u opsegu[4, 16]")
		return false
	}
	hll.P = p
	hll.M = uint64(math.Pow(2, float64(hll.P)))
	hll.Reg = make([]uint8, hll.M, hll.M)
	return true
}

func(hll *HLL) encodeHllToBytes() []byte{
	/*
		Pomocu bytes.Buffer enkodira strukturu u niz bajtova i vraca je takvu.
	*/
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(hll)
	if err != nil{
		panic(err)
	}

	return buffer.Bytes()
}
func(hll *HLL) decodeHllFromBytes(hllBytes []byte) bool{
	/*
		Pomocu bytes.Buffer dekodira strukturu iz niza bajtova u samu sebe i vraca true ukoliko je uspesno izvrseno
	*/
	var bytes bytes.Buffer
	bytes.Write(hllBytes)
	decoder := gob.NewDecoder(&bytes)
	err := decoder.Decode(&hll)
	if err != nil {
		fmt.Println(err)
	}
	return true
}

func(hll *HLL) serializeHLL(filename string) bool{
	file, err := os.Create(filename)
	if err != nil{
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(hll)
	if err != nil {
		panic(err)
	}
	err = file.Close()
	return true
}

func(hll *HLL) deseriaLizeHLL(filename string) bool{
	file, err := os.Open(filename)
	if err != nil{
		panic(err)
	}

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&hll)
	if err != nil {
		fmt.Println(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}
	return true

}

func (hll *HLL) addData(data []byte){

	// kreiranje hesh vrednosti i konv u bin broj
	value := createHashValue(data)
	binaryValue := convertToBinary(value)

	// pronalazenje bucketa
	firstPBytes := binaryValue[:hll.P]
	bucket := uint8(convertBinaryToInt(firstPBytes))
	// pronalazenje vrednosti za upis u baket
	var counter uint8 = 0
	// ide od kraja pa dok ne naidje na 1 brojac se povecava

	for i:=len(binaryValue)-1;i>=0;i--{
		if binaryValue[i] != '0'{
			break
		}
		counter++
	}
	// proverava da li je veci od trenutne
	// vred u tom baketu, ako jeste zameni ih
	if counter > hll.Reg[bucket]{
		hll.Reg[bucket] = counter
	}

}

func convertToBinary(value uint32) string{
	return strconv.FormatInt(int64(value), 2)
}

func convertBinaryToInt(value string) int64{
	newValue, _ := strconv.ParseInt(value, 2, 64)

	return newValue
}

func createHashValue(data []byte) uint32{
	hash := fnv.New32()
	_, err := hash.Write(data)
	handleError(err)
	val := hash.Sum32()
	hash.Reset()
	return val

}

func (hll *HLL) emptyCount() int{
	sum := int(0)
	for _, val := range hll.Reg{
		if val == 0{
			sum++
		}
	}
	return sum
}


func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}
