package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

/*
1.uzimamo broj datoteka svake generacije, posle svake kompakcije krecemo od prve ponovo
2.radimo po dogovorenom algoritmu
*/
func compact() bool {
	i := uint64(1)
	for i < 5 {
		filenames := readDirectory("resources/data/")
		genFilenames := getByGeneration(filenames, i)
		if len(genFilenames) >= 2 {
			index := i + uint64(1)
			gen2Filenames := getByGeneration(filenames, index)
			var nextFileIndex uint64
			if len(gen2Filenames) == 0 {
				nextFileIndex = 0
			} else {
				nextFile := gen2Filenames[len(gen2Filenames)-1]
				first, _ := getFileIndex(nextFile)
				nextFileIndex, _ = strconv.ParseUint(first, 10, 64)

			}

			mergeFiles(genFilenames[0], genFilenames[1], index, nextFileIndex+1)
		}
		if len(genFilenames) < 2 {
			i++
		}

	}

	return true
}

func mergeFiles(first string, second string, newLevel uint64, nextFileIndex uint64) {

	file1, err := os.OpenFile(first, os.O_RDONLY, 0777)

	if err != nil {
		panic(err)
	}

	file2, err := os.OpenFile(second, os.O_RDONLY, 0777)

	nodes := make([]*Data, 0)
	if err != nil {
		panic(err)
	}

	index1 := uint64(0)
	index2 := uint64(0)
	node1 := readRecord(file1, index1)
	node2 := readRecord(file2, index2)

	for node1 != nil && node2 != nil {
		if node1.key < node2.key {
			if node1.ts != 1 {
				nodes = append(nodes, node1)
			}
			node1 = readRecord(file1, index1)

		} else if node1.key > node2.key {
			if node2.ts != 1 {
				nodes = append(nodes, node2)
			}
			node2 = readRecord(file2, index2)

		} else {
			if node1.timeStamp < node2.timeStamp {
				if node2.ts != 1 {
					nodes = append(nodes, node2)
				}

			} else {
				if node1.ts != 1 {
					nodes = append(nodes, node1)
				}
			}
			node2 = readRecord(file2, index2)
			node1 = readRecord(file1, index1)
		}

	}
	if node1 == nil {
		//nastavi node2
		for node2 != nil {
			nodes = append(nodes, node2)
			node2 = readRecord(file2, index2)
		}
	} else if node2 == nil {
		for node1 != nil {
			nodes = append(nodes, node1)
			node1 = readRecord(file1, index1)
		}
	}
	for i := 0; i < len(nodes); i++ {
		fmt.Printf(" ")
		fmt.Printf(nodes[i].key)
	}
	fmt.Println(" ")
	file1.Close()
	file2.Close()
	deleteFile(first)
	deleteFile(second)

	//otvorimo ova fajla
	//uzmemo prvih 29 bitova iz oba
	// zatim odatle se sikujemo za pozicije key value
	// uzimamo te vrednosti i poredimo kljuceve
	// pritom gledamo prvo timestamp pa zatim tombstone

	/*

				...
			//upisemo key u novi file ako nije ts
			//seekujemo se za novi podatak u file u kome je bio key

			if key < key2{
			.....
		}
			}else if key > key2{
				...
			}else{
				if timestamp1 < timestamp2{
			...

			}else{
				if tombostone == 1{
					...
			if tombo

			}
			}
	*/

}

func get(memtable *Memtable, lru *LruCache, key string) *Data {
	node := memtable.skiplist.findNode(key)
	if node == nil {
		newNode := lru.getFromCache(key)
		if newNode == nil {
			filenames := sortByCreationTime(filterFilenames())
			if len(filenames) == 0 {
				return nil
			} else {
				foundNode := checkFilters(filenames, key)
				if foundNode == nil {
					return nil
				} else {
					if foundNode.ts == 1 {
						return nil
					}
					dataNode := &Data{}
					dataNode.key = foundNode.key
					dataNode.value = foundNode.value
					dataNode.ts = foundNode.ts
					dataNode.timeStamp = foundNode.timeStamp
					lru.addDataToCache(dataNode)
					return dataNode
				}
			}

		} else {
			if newNode.ts == 1 {
				return nil
			}
			newNewNode := &Data{}
			newNewNode.key = newNode.key
			newNewNode.value = newNode.value
			newNewNode.ts = newNode.ts
			newNewNode.timeStamp = newNode.timeStamp
			return newNewNode
		}

	} else {
		if node.tombstone == 1 {
			return nil
		}
		newNode := &Data{}
		newNode.key = node.key
		newNode.value = node.Value
		newNode.timeStamp = node.Timestamp
		newNode.ts = node.tombstone
		return newNode
	}

	return nil
}

//
func fileIndex(filename string) string {

	word := ""
	dashCounter := 0
	for i := 0; i < len(filename); i++ {
		if filename[i] == '-' {
			dashCounter++
		}
		word += string(filename[i])
		if dashCounter == 5 {
			return word[17:]
		}
	}
	return ""
}
func checkFilters(filenames []string, key string) *dllNode {

	for i := 0; i < len(filenames); i++ {
		filter := &BloomFilter{}
		filter.decodeFilter(filenames[i])
		if filter.exists([]byte(key)) {
			filename := fileIndex(filenames[i])
			inRange := checkSummaryHeader(key, filename)

			if inRange > 0 {
				indexFilePosition := checkSummary(key, filename, inRange)
				if indexFilePosition != 1 {

					dataFilePosition := checkDataIndex(key, filename, indexFilePosition)

					if dataFilePosition != 1 {
						node := checkDataFile(key, filename, dataFilePosition)
						return node
					}
				}

			} else {
				continue
			}

		} else {
			continue
		}
	}

	return nil
}

func readRecord(file *os.File, indexFilePosition uint64) *Data {

	offset := int64(0)

	file.Seek(int64(indexFilePosition), 1)

	firstData := make([]byte, 29)
	_, err := file.Read(firstData)
	if err != nil {
		if err == io.EOF {
			return nil
		}

	}

	crc := binary.LittleEndian.Uint32(firstData[offset : C_SIZE+offset])
	timeStamp := binary.LittleEndian.Uint64(firstData[C_SIZE+offset : CRC_SIZE+offset])
	ts := firstData[CRC_SIZE+offset : offset+TOMBSTONE_SIZE][0]
	keySize := binary.LittleEndian.Uint64(firstData[TOMBSTONE_SIZE+offset : KEY_SIZE+offset])
	valueSize := binary.LittleEndian.Uint64(firstData[KEY_SIZE+offset : VALUE_SIZE+offset])

	keyB := make([]byte, keySize)

	file.Read(keyB)

	newKey := string(keyB)
	value := make([]byte, valueSize)

	if err != nil {
		panic(err)
	}

	file.Read(value)

	if crc == CRC32(value) {
		node := &Data{}
		node.key = newKey
		node.value = value
		node.timeStamp = timeStamp
		node.ts = ts
		return node
	}
	return nil
}
func checkDataFile(key string, filename string, indexFilePosition uint64) *dllNode {
	dataFilename := "resources/data/" + filename + "Data.db"
	file, err := os.OpenFile(dataFilename, os.O_RDONLY, 0777)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	offset := int64(0)

	file.Seek(int64(indexFilePosition), 0)

	firstData := make([]byte, 29)
	_, err = file.Read(firstData)

	crc := binary.LittleEndian.Uint32(firstData[offset : C_SIZE+offset])
	timeStamp := binary.LittleEndian.Uint64(firstData[C_SIZE+offset : CRC_SIZE+offset])
	ts := firstData[CRC_SIZE+offset : offset+TOMBSTONE_SIZE][0]
	keySize := binary.LittleEndian.Uint64(firstData[TOMBSTONE_SIZE+offset : KEY_SIZE+offset])
	valueSize := binary.LittleEndian.Uint64(firstData[KEY_SIZE+offset : VALUE_SIZE+offset])

	keyB := make([]byte, keySize)

	file.Read(keyB)

	newKey := string(keyB)
	value := make([]byte, valueSize)

	if err != nil {
		panic(err)
	}

	file.Read(value)

	if crc == CRC32(value) {
		node := &dllNode{}
		node.key = newKey
		node.value = value
		node.timeStamp = timeStamp
		node.ts = ts
		return node
	}
	return nil

}

func checkDataIndex(key string, filename string, indexFilePosition uint64) uint64 {
	indexFilename := "resources/index/" + filename + "Index.db"
	file, err := os.OpenFile(indexFilename, os.O_RDONLY, 0777)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	// prvi key
	s := strconv.FormatUint(indexFilePosition, 10)
	indexPosInt, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}

	_, err = file.Seek(int64(indexPosInt), 0)
	if err != nil {
		return 1
	}

	firstData := make([]byte, 8)

	_, err = file.Read(firstData)
	if err != nil {
		panic(err)
	}
	offset := uint64(0)
	keyLength := binary.LittleEndian.Uint64(firstData)
	offset += 8

	keyData := make([]byte, keyLength)
	_, err = file.Read(keyData)
	foundKey := string(keyData)

	pointerData := make([]byte, 8)
	_, err = file.Read(pointerData)
	pointerPosition := binary.LittleEndian.Uint64(pointerData)

	if foundKey == key {
		return pointerPosition
	}
	return 1
}

func checkSummary(key string, filename string, startPosition uint64) uint64 {
	summaryFilename := "resources/summary/" + filename + "Summary.db"

	bytes, err := ioutil.ReadFile(summaryFilename)
	bytes = bytes[startPosition:]

	if len(bytes) == 0 {
		return 1
	}
	if err != nil {
		panic(err)
	}

	offset := uint64(0)
	iOffset := 0
	//kLength(8B)/key/pPosition(8B)
	for iOffset < len(bytes) {
		keyLength := binary.LittleEndian.Uint64(bytes[offset : 8+offset])
		offset += 8
		currentKey := bytes[offset : offset+keyLength]
		offset = offset + keyLength
		pointerPosition := binary.LittleEndian.Uint64(bytes[offset : 8+offset])
		offset += 8

		if string(currentKey) == key {
			return pointerPosition
		}
		s := strconv.FormatUint(offset, 10)
		iOffset, err = strconv.Atoi(s)
		if err != nil {
			panic(err)
		}

	}
	return 1

}

func checkSummaryHeader(key string, word string) uint64 {

	filename := "resources/summary/" + word + "Summary.db"
	file, err := os.OpenFile(filename, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}

	// prvi key
	file.Seek(0, 0)

	firstData := make([]byte, 8)

	_, err = file.Read(firstData)
	if err != nil {
		panic(err)
	}

	firstKeyLength := binary.LittleEndian.Uint64(firstData)

	keyStorage := make([]byte, firstKeyLength)

	file.Seek(8, 0)
	file.Read(keyStorage)

	bottomKey := string(keyStorage)

	if key < bottomKey {
		file.Close()
		return 0
	}

	// drugi key

	firstKeyLengthToString := strconv.FormatUint(firstKeyLength, 10)
	firstKeySizeInt, err := strconv.Atoi(firstKeyLengthToString)

	offset := 8 + int64(firstKeySizeInt)

	file.Seek(offset, 0)

	secondData := make([]byte, 8)
	_, err = file.Read(secondData)
	if err != nil {
		panic(err)
	}

	secondtKeyLength := binary.LittleEndian.Uint64(secondData)

	secondKeyStorage := make([]byte, secondtKeyLength)

	file.Seek(8+offset, 0)
	file.Read(secondKeyStorage)

	topKey := string(secondKeyStorage)

	if key > topKey {
		file.Close()
		return 0
	}
	file.Close()
	return 8 + 8 + firstKeyLength + secondtKeyLength

}

func filterFilenames() []string {
	filenames := readDirectory("resources/filter/")
	return filenames
}
