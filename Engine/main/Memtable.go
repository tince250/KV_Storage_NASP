package main

import (
	"os"
	"strconv"
	"time"
)

type Memtable struct{
	skiplist SkipList
	memtableSize uint64
	threshold uint64
	currentSize uint64
	walThreshold uint64
}

func(memtable *Memtable) initMemtable() bool{

	defVals := &defValues{}
	defVals.getDefaultValues("config/config.yml")
	memtable.memtableSize = defVals.MemtableSize
	memtable.threshold = defVals.Threshold
	memtable.currentSize = 0
	memtable.skiplist.createSkipList(defVals.MaxHeight, memtable.threshold, defVals.WalThreshold)
	memtable.walThreshold = defVals.WalThreshold
	a := readFullData("wal/")
	memtable.reconstructWal(a)
	return true
}

func(memtable *Memtable) reconstructWal(walData []*Data) bool{

	memtable.skiplist.inserFromWal(walData)
	memtable.currentSize = uint64(len(walData))
	return true
}

func(memtable *Memtable) flush() bool{

	/*
		1. Flush memtable u data folder
		2. Brisanje walova i kreiranje jednog praznog
		3. Kreiranje index tabele
		4. Kreiranje summary tabele
		5. Kreiranje bloomFiltera
		6. Praznjenje memtable
		7. Kreiranje TOC
	*/

	/*
		usertable-data-ic-1-1-Data.db
		usertable-data-ic-1-1-Filter.db
	*/
	/*
		ulazimo u folder data/filter/index
		u tom folderu ucitavamo imena fajlova
		i gledamo koliko direktorijum ima elemenata
		i dodamo plus 1
	*/

	filenames := readDirectory("resources/data")
	var nextIndex int = len(filenames) + 1
	baseFilename := "usertable-data-ic-" + strconv.Itoa(nextIndex) + "-1-"

	dataFilename := "resources/data/" + baseFilename + "Data.db"
	filterFilename := "resources/filter/" + baseFilename + "Filter.db"
	indexFilename := "resources/index/" + baseFilename + "Index.db"
	summaryFilename := "resources/summary/" + baseFilename + "Summary.db"
	tocFilename := "resources/toc/" + baseFilename + "TOC.txt"
	metadataFilename := "resources/metadata/" + baseFilename + "metadata.db"

	listOfFilenames := [6]string{dataFilename, filterFilename, indexFilename, summaryFilename, tocFilename, metadataFilename}

	for _, i:=range listOfFilenames{
		file, err := os.Create(i)
		if err != nil{
			panic(err)
		}
		file.Close()
	}
	/*
		Upisemo u data, popunimo bloomfilter za taj elem, upisemo tekuci index u index, upisujemo index indexa u summary (start i end na kraju u footer),
		Na kraju merkle i TOC

		start: anastijsa
		end: zqwewqewqeqwrtretre

	*/

	listOfValues := memtable.flushMainPart(listOfFilenames[0:4])
	memtable.flushMetadata(metadataFilename, listOfValues)
	memtable.flushTOC(listOfFilenames)

	return true
}

func(memtable *Memtable) flushMainPart(filenames []string) [][]byte{

	fileData, err := os.OpenFile(filenames[0], os.O_RDWR, 0666)
	if err != nil{
		panic(err)
	}
	defer fileData.Close()


	fileIndex, err := os.OpenFile(filenames[2], os.O_RDWR, 0666)
	if err != nil{
		panic(err)
	}
	defer fileIndex.Close()

	fileSummary, err := os.OpenFile(filenames[3], os.O_RDWR, 0666)
	if err != nil{
		panic(err)
	}
	defer fileSummary.Close()


	bloomFilter := &BloomFilter{}
	bloomFilter.initializeBloomFilter(int(memtable.skiplist.size), 0.4) // TODO:proveriti rate


	listOfValues := make([][]byte, 0)
	node := memtable.skiplist.head.next[0]

	var indexPosition uint64 = 0
	var summaryPosition uint64 = 0

	firstNodeKey := node.key
	lastNodeKey := node.key

	listOfSummaryData := make([]byte, 0)


	for node != nil{
		lastNodeKey = node.key

		listOfValues = append(listOfValues, node.Value)

		// data deo
		data := createWalData(node.key, node.Value, node.tombstone, node.Timestamp)

		appendData(fileData, data)

		// filter deo
		bloomFilter.addElement([]byte(node.key))

		// index deo
		// key size|key value|pointerSize|pointerValue
		// 8 B | 8 B | kljuc | pokazivac
		indexData := createIndexData(node.key, indexPosition)
		appendData(fileIndex, indexData)

		// summary
		// key size | pointer size| key value| pointer value
		summaryData := createIndexData(node.key, summaryPosition)
		listOfSummaryData = append(listOfSummaryData, summaryData...)

		indexPosition += uint64(len(data)) // mozda + 1
		summaryPosition += uint64(len(indexData))
		node = node.next[0]
	}
	listOfSummaryWithHeader := make([]byte, 0)

	// prvo dodati heder
	// key size (8B)| key value
	// key size (8B)| key value
	startIndex := createHeaderData(firstNodeKey)
	endIndex := createHeaderData(lastNodeKey)
	listOfSummaryWithHeader = append(listOfSummaryWithHeader, startIndex...)
	listOfSummaryWithHeader = append(listOfSummaryWithHeader, endIndex...)
	// pa dodati listOfSummaryData
	listOfSummaryWithHeader = append(listOfSummaryWithHeader, listOfSummaryData...)

	appendData(fileSummary, listOfSummaryWithHeader)


	bloomFilter.encodeFilter(filenames[1])

	return listOfValues
}

func(memtable *Memtable) flushTOC(filenames [6]string) bool{
	//	listOfFilenames := [6]string{dataFilename, filterFilename,
	//	indexFilename, summaryFilename, tocFilename, metadataFilename}
	f, err := os.OpenFile(filenames[4], os.O_APPEND, 0666)
	if err != nil{
		panic(err)
	}
	defer f.Close()

	var filenamesToPut string = ""
	for i:=0;i<len(filenames);i++{
		filenamesToPut += filenames[i] + "\n"
	}
	_, err2 := f.WriteString(filenamesToPut)
	if err2 != nil{
		panic(err2)
	}
	return true
}

func(memtable *Memtable) flushMetadata(filename string, listOfValues [][]byte)  bool{
	merkle := MerkleRoot{}
	merkle.formMerkle(listOfValues)
	merkle.serializeMerkle(filename)
	return true
}

func(memtable *Memtable) insertToMemtable(key string, value []byte, indicator int) bool{
	// dodamo u wal
	// pozoveno addnode iz skipliste
	// proverimo da li postoji/ne postoji  indikator 0
	// indicator 0 - add, 1 - edit, 2 - delete

	var node *SkipListNode
	if indicator != 0{
		node = memtable.skiplist.findNode(key)
		if node == nil{
			if indicator == 1 {
				indicator = 0

			} else
			{return false }
		}
	}
	filenames := readDirectory("wal/")
	filename := filenames[len(filenames) - 1]
	file1, err := os.Stat(filename)
	if err !=nil{
		panic(err)
	}
	if uint64(file1.Size()) >= memtable.walThreshold{
		filename = setNewFilenameBasedOnOffsets(splitOffests(filenames))
		file2 := createFile(filename)
		file2.Close()
	}

	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil{
		panic(err)
	}

	if err != nil{
		panic(err)
	}

	/// ovde se insertuje u wal, i onda se radi sa skiplistom dalje
	timestamp := uint64(time.Now().Unix())
	var ts byte = 1
	if indicator == 0 || indicator == 1{
		ts = 0
	}

	if appendData(file, createWalData(key, value, ts, timestamp)) == nil {
		//skiplist add/logical delete/edit
		if indicator == 0{
			memtable.skiplist.addNode(key, value, timestamp)
		}else if indicator == 1{
			memtable.skiplist.editNode(value, timestamp, node)
		}else{
			memtable.skiplist.logicalDelete(timestamp, node)
		}
	}
	file.Close()

	memtable.currentSize++
	if float64(memtable.currentSize) > float64((memtable.threshold * memtable.memtableSize)/100.0){
		memtable.flush()

		/*
			Isprazniti memtable
			Wal izbrisati i kreirati novi
		*/
		resetWal("wal/")
		maxHeight := memtable.skiplist.maxHeight
		walThreshold := memtable.skiplist.walThreshold
		memtable.skiplist = SkipList{}
		memtable.skiplist.createSkipList(maxHeight, memtable.threshold, walThreshold)
	}
	return true
}







