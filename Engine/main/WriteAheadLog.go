package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type ToWriteStruct struct {
	Crc uint32
	Timestamp uint64
	Tombstone byte
	KeySize uint64
	ValueSize uint64
	Key string
	Value []byte
}

type Data struct {
	key string
	value []byte
	ts byte
	timeStamp uint64
}

type Buffer struct {
	data []*Data
}

func bufferedWritter(buffer *Buffer, filepath string) bool{
	file, err := os.OpenFile(filepath, os.O_APPEND, 0777)
	if err != nil{
		return false
	}
	for i:=0;i<len(buffer.data);i++{
		checksum := CRC32(buffer.data[i].value)
		tws:= ToWriteStruct{checksum, uint64(time.Now().Unix()), 0, uint64(len(buffer.data[i].key)), uint64(len(buffer.data[i].value)), buffer.data[i].key, buffer.data[i].value}
		binary.Write(file, binary.LittleEndian, tws.Crc)
		binary.Write(file, binary.LittleEndian, tws.Timestamp)
		binary.Write(file, binary.LittleEndian, tws.Tombstone)
		binary.Write(file, binary.LittleEndian, tws.KeySize)
		binary.Write(file, binary.LittleEndian, tws.ValueSize)
		binary.Write(file, binary.LittleEndian, []byte(tws.Key))
		binary.Write(file, binary.LittleEndian, tws.Value)
	}
	err = file.Close()
	if err != nil{
		return false
	}
	return true
}

func writeData(key string, value []byte, filepath string, tombstone byte, timestamp uint64) bool{
	/*
		Upisuje jedan podatak u WAL.
	*/
	if !existsFile(filepath){
		_, err := os.Create(filepath)
		if err!=nil{
			panic(err)
		}
	}

	// pakujemo podatke u strukturu za ispis
	checksum := CRC32(value)
	tws := ToWriteStruct{checksum, timestamp, tombstone, uint64(len(key)), uint64(len(value)), key, value}
	
	// format sloga:
	//crc(4b) timeStamp(8b) tombstone(1b) keySize(8b) valueSize(8b) key value
	file, err := os.OpenFile(filepath, os.O_APPEND, 0777)
	
	//size := 4 + 8 + 1 + 8 + 8 + tws.keySize + tws.valueSize
	if err!=nil{
		return false
	}
	
	// upis u fajl
	binary.Write(file, binary.LittleEndian, tws.Crc)
	binary.Write(file, binary.LittleEndian, tws.Timestamp)
	binary.Write(file, binary.LittleEndian, tws.Tombstone)
	binary.Write(file, binary.LittleEndian, tws.KeySize)
	binary.Write(file, binary.LittleEndian, tws.ValueSize)
	binary.Write(file, binary.LittleEndian, []byte(tws.Key))
	binary.Write(file, binary.LittleEndian, tws.Value)

	err = file.Close()
	if err != nil {
		return false
	}

	return true
}

func readFullData(filepath string) []*Data{
	/*
		Cita sve podatke iz svih segmenata WAL-a.
	*/

	filenames := readDirectory(filepath)
	data := make([]*Data, 0)
	for _, i:= range filenames {
		// citamo sve bajtove iz fajla i redom ih kovertujemo u podatke
		bytes, err := ioutil.ReadFile(i)

		if len(bytes) == 0{
			return data
		}
		if err != nil {
			panic(err)
		}


		offset := uint64(0)
		iOffset := 0
		//crc(4b) timeStamp(8b) tombstone(1b) keySize(8b) valueSize(8b) key value
		for iOffset < len(bytes) {

			crc := binary.LittleEndian.Uint32(bytes[offset : C_SIZE+offset])
			timeStamp := binary.LittleEndian.Uint64(bytes[C_SIZE+offset : CRC_SIZE+offset])
			ts := bytes[CRC_SIZE+offset : offset+TOMBSTONE_SIZE][0]
			keySize := binary.LittleEndian.Uint64(bytes[TOMBSTONE_SIZE+offset : KEY_SIZE+offset])
			valueSize := binary.LittleEndian.Uint64(bytes[KEY_SIZE+offset : VALUE_SIZE+offset])
			// 29 je velicina fiksnog dela WAL sloga
			offset += 29
			key := bytes[offset : offset+keySize]
			offset = offset + keySize
			value := bytes[offset : offset+valueSize]
			offset = offset + valueSize
			s := strconv.FormatUint(offset, 10)
			iOffset, err = strconv.Atoi(s)
			if err != nil {
				panic(err)
			}
			// provera ispravnosti CRC-a
			if crc == CRC32(value) {
				data = append(data, &Data{string(key), value, ts, timeStamp})

			}
		}

	}

	return data
}

func readData(filepath string, lines int) []*Data{
	/* 
		Cita odredjen broj linja iz WAL-a.
		*Funkcija sa vezbi, nije koriscena u projektu.
	*/
	humanList := make([]*Data, lines, lines)
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
	if err !=nil{
		panic(err)
	}

	offset:=int64(29)

	for i:=0;i<lines;i++ {

		file.Seek(0, 1)

		firstData := make([]byte, 29)

		_, err = file.Read(firstData)

		crc := binary.LittleEndian.Uint32(firstData[:C_SIZE])
		timeStamp := binary.LittleEndian.Uint64(firstData[C_SIZE:CRC_SIZE])
		tombstone := firstData[CRC_SIZE:TOMBSTONE_SIZE][0]
		keySize := binary.LittleEndian.Uint64(firstData[TOMBSTONE_SIZE:KEY_SIZE])
		valueSize := binary.LittleEndian.Uint64(firstData[KEY_SIZE:VALUE_SIZE])

		keyB := make([]byte, keySize)

		file.Seek(offset, 0)
		file.Read(keyB)

		key := string(keyB)
		valueB := make([]byte, valueSize)
		kSizeToStr:= strconv.FormatUint(keySize, 10)
		kSizeInt, err := strconv.Atoi(kSizeToStr)
		if err != nil{
			panic(err)
		}

		file.Seek(offset + int64(kSizeInt), 0)
		file.Read(valueB)

		value := string(valueB)
		vSizeToStr:= strconv.FormatUint(valueSize, 10)
		vSizeInt, err := strconv.Atoi(vSizeToStr)
		if err != nil{
			panic(err)
		}

		file.Seek(offset + int64(vSizeInt) + int64(kSizeInt), 0)
		offset += int64(vSizeInt) + int64(kSizeInt) + 29


		//if crc == CRC32([]byte(value)) {
		//	humanList[i] = &Human{key, value}
		//}
		fmt.Println(key, value)
		fmt.Println(crc)
		fmt.Println(timeStamp)
		fmt.Println(tombstone)

	}
	err = file.Close()
	if err != nil {
		return nil
	}
	return humanList
}




func CRC32(data []byte) uint32 {
	// ChecksumIEEE returns the CRC-32 checksum of data
	// using the IEEE polynomial
	return crc32.ChecksumIEEE(data)
}
