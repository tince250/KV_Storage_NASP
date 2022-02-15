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
	Timestamp int64
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
		tws:= ToWriteStruct{checksum, time.Now().Unix(), 0, uint64(len(buffer.data[i].key)), uint64(len(buffer.data[i].value)), buffer.data[i].key, buffer.data[i].value}
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

func writeData(key string, value []byte, filepath string, tombstone byte) bool{
	if !existsFile(filepath){
		_, err := os.Create(filepath)
		if err!=nil{
			panic(err)
		}
	}

	checksum := CRC32(value)
	tws := ToWriteStruct{checksum, time.Now().Unix(), tombstone, uint64(len(key)), uint64(len(value)), key, value}

	//crc(4b) timeStamp(8b) tombstone(1b) keySize(8b) valueSize(8b) key value
	file, err := os.OpenFile(filepath, os.O_APPEND, 0777)
	//size := 4 + 8 + 1 + 8 + 8 + tws.keySize + tws.valueSize
	if err!=nil{
		return false
	}
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

//func findData(key string, filepath string) *Data{
//	file, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
//	if err !=nil{
//		panic(err)
//	}
//	offset := int64(29)
//	for {
//		file.Seek(0, 1)
//		data := make([]byte, 29)
//		_, err = file.Read(data)
//		if err == io.EOF{
//			break
//		}
//		if err != nil{
//			continue
//		}
//		crc := binary.LittleEndian.Uint32(data[:C_SIZE])
//		keySize := binary.LittleEndian.Uint64(data[TOMBSTONE_SIZE:KEY_SIZE])
//		valueSize := binary.LittleEndian.Uint64(data[KEY_SIZE:VALUE_SIZE])
//		keyB := make([]byte, keySize)
//
//		file.Seek(offset, 0)
//		file.Read(keyB)
//		keyToCompare := string(keyB)
//		valueB := make([]byte, valueSize)
//		kSizeToStr:= strconv.FormatUint(keySize, 10)
//		kSizeInt, err := strconv.Atoi(kSizeToStr)
//		if err != nil{
//			panic(err)
//		}
//
//		file.Seek(offset + int64(kSizeInt), 0)
//		file.Read(valueB)
//
//		value := string(valueB)
//		vSizeToStr:= strconv.FormatUint(valueSize, 10)
//		vSizeInt, err := strconv.Atoi(vSizeToStr)
//		if err != nil{
//			panic(err)
//		}
//		if crc == CRC32([]byte(value)) {
//			if key == keyToCompare {
//				return &Data{key, valueB, 0}
//			}
//		}
//		file.Seek(offset + int64(vSizeInt) + int64(kSizeInt), 0)
//		offset += int64(vSizeInt) + int64(kSizeInt) + 29
//
//	}
//
//	err = file.Close()
//	if err != nil {
//		return nil
//	}
//	return nil
//}

//func convertInputToData(data []byte) []*Data{
//	offset := uint64(0)
//	iOffset := 0
//	newData := make([]*Data, 0)
//	for iOffset < len(data){
//		crc := binary.LittleEndian.Uint32(data[offset:C_SIZE + offset])
//		ts := data[CRC_SIZE + offset:offset + TOMBSTONE_SIZE][0]
//		keySize := binary.LittleEndian.Uint64(data[TOMBSTONE_SIZE + offset :KEY_SIZE + offset])
//		valueSize := binary.LittleEndian.Uint64(data[KEY_SIZE + offset:VALUE_SIZE + offset])
//
//		offset += 29
//		key := data[offset:offset + keySize]
//		offset = offset + keySize
//		value := data[offset:offset+valueSize]
//		offset = offset + valueSize
//		s := strconv.FormatUint(offset, 10)
//		iOffset, _ = strconv.Atoi(s)
//
//		if crc == CRC32(value) {
//			if ts == 0 {
//				newData = append(newData, &Data{string(key), value, 0})
//			}
//		}
//
//	}
//
//	return newData
//}

func readFullData(filepath string) []*Data{
	bytes, err := ioutil.ReadFile(filepath)
	if err != nil{
		panic(err)
	}

	data := make([]*Data, 0)
	offset := uint64(0)
	iOffset := 0
	//crc(4b) timeStamp(8b) tombstone(1b) keySize(8b) valueSize(8b) key value
	for iOffset < len(bytes){
		crc := binary.LittleEndian.Uint32(bytes[offset:C_SIZE + offset])
		timeStamp := binary.LittleEndian.Uint64(bytes[C_SIZE + offset: CRC_SIZE + offset])
		ts := bytes[CRC_SIZE + offset:offset + TOMBSTONE_SIZE][0]
		keySize := binary.LittleEndian.Uint64(bytes[TOMBSTONE_SIZE + offset :KEY_SIZE + offset])
		valueSize := binary.LittleEndian.Uint64(bytes[KEY_SIZE + offset:VALUE_SIZE + offset])

		offset += 29
		key := bytes[offset:offset + keySize]
		offset = offset + keySize
		value := bytes[offset:offset+valueSize]
		offset = offset + valueSize
		s := strconv.FormatUint(offset, 10)
		iOffset, err = strconv.Atoi(s)
		if err != nil{
			panic(err)
		}
		if crc == CRC32(value) {
			data = append(data, &Data{string(key), value, ts, timeStamp})

		}

	}

	return data
}

func readData(filepath string, lines int) []*Data{

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

		//fmt.Println(vSizeInt)
		//fmt.Println(kSizeInt)
		fmt.Println(crc)
		fmt.Println(timeStamp)
		fmt.Println(tombstone)
		//fmt.Println(keySize)
		//fmt.Println(valueSize)
		//fmt.Println(key)
		//fmt.Println(value)

	}
	err = file.Close()
	if err != nil {
		return nil
	}
	return humanList
}

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)



func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
