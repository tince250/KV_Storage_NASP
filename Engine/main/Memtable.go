package main

import (
	"fmt"
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

func(memtable *Memtable) initMemtableWithPassedValues(memtableSize, threshold, walThreshold, maxHeight uint64) bool{
	/*
		Inicijalizacija memtabele sa defValue parsiranim argumentima
	*/
	memtable.memtableSize =memtableSize
	memtable.threshold = threshold
	memtable.currentSize = 0
	memtable.skiplist.createSkipList(maxHeight, memtable.threshold, walThreshold)
	memtable.walThreshold = walThreshold
	a := readFullData("wal/")
	memtable.reconstructWal(a)
	return true
}

func(memtable *Memtable) initMemtable() bool{
	/*
		Inicijalizacija memtabele bez argumenata
	*/
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
	/*
		Rekonstrukcija memtabele na osnovu wal segmenata
	*/
	memtable.skiplist.inserFromWal(walData)
	memtable.currentSize = uint64(len(walData))
	return true
}

func(memtable *Memtable) flush() bool {
	/*
		Metoda za flushovanje memtable-a i kreiranje sstabele
		Prvo se procita direktorijum i odredi se redni broj sledeceg fajla, tj koji ce mu biti naziv
		Kreiraju se fajlovi za celu sstabelu sa tim odredjenim indeksima
		Zatim se flusha glavni deo(data/index/summary/bf), a nakon toga metadata fajl i toc
	*/
	filenames := readDirectory("resources/data")
	filenamesForGeneration := getByGeneration(filenames, 1)
	var nextIndex int = 0

	if len(filenamesForGeneration) == 0 {
		nextIndex = 1
	} else {
		sortedFilenames := sortByCreationTime(filenamesForGeneration)
		first, _ := getFileIndex(sortedFilenames[0])
		nextIndex, _ = strconv.Atoi(first)
		nextIndex = nextIndex + 1
	}

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


	listOfValues := memtable.flushMainPart(listOfFilenames[0:4])
	memtable.flushMetadata(metadataFilename, listOfValues)
	memtable.flushTOC(listOfFilenames)

	return true
}

func(memtable *Memtable) flushMainPart(filenames []string) [][]byte{
	/*
		Flushuje u fajlove data/index/summary/bloomFilter

		Ide kroz elemente skipliste i flusha ih u data deo sstabele
		pritom cuva odredjene vrednosti za value(zbog metadata fajla)
		kao i prvi i poslednji kljuc zbog summary headera. Dok
		upisuje u data uporedo radi i index, i upisuje kljuceve u bloom filter
		a nakon prolaska kroz celu listu upisuje summary header i ostale elemente

	*/

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
	/*
		Flushuje nazive novokreiranih fajlova u tabelu TOC
	*/
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
	/*
		Popunjava merkle stablo i flushuje ga na osnovu vrednosti prikupljenih u mainFlushu
	*/
	merkle := MerkleRoot{}
	merkle.formMerkle(listOfValues)
	merkle.serializeMerkle(filename)
	return true
}

func(memtable *Memtable) insertHllToMemtable(key string, lru *LruCache) bool{

	/*
		Trazi prvo da li kljuc postoji u memtabeli
		Ukoliko postoji za dodavanje raditi izmenu, a za brisanje klasicno logicko brisanje
		Ukoliko ne postoji uraditi dodavanje kad se radi put, a brisanje ignorisati
		Dodavanje radimo tako sto kreiramo novu hll tabelu,a izmenu tako sto je ucitamo iz memorije
		i dodamo nove vrednosti u nju
	*/


	key = key + "_hll"

	// ovde ide pitanje sta hoce put ili delete
	fmt.Println("1. Put")
	fmt.Println("2. Delete")
	var decision string
	fmt.Print("\nChoose option:\n>> ")
	// Taking input from user
	fmt.Scanln(&decision)

	if decision == "1"{
		node := get(memtable, lru, key)
		if node == nil{
			// kreiramo novi HLL
			fmt.Println("Enter P [4, 16]")
			var p uint8
			_, err := fmt.Scanln(&p)
			if err != nil {
				panic(err)
			}
			if p < 4 || p > 16{
				fmt.Println("P not in boundaries!")
				return false
			}
			hll := &HLL{}
			hll.createHLL(p)
			fmt.Println("Enter values to put in HLL until x is pressed.")
			var value string = ""
			fmt.Println(">> ")
			_, err = fmt.Scanln(&value)
			if err != nil{
				panic(err)
			}

			for value != "x"{
				hll.addData([]byte(value))
				fmt.Println(">> ")
				_, err := fmt.Scanln(&value)
				if err != nil {
					panic(err)
				}
			}
			fmt.Printf("Hll with name: %s sucesfully created.\n", key)
			memtable.insertToMemtable(key, hll.encodeHllToBytes(), 0)
		}else{
			// deserijalizujemo i dodajemo nove vrednosti
			hll := &HLL{}
			hll.decodeHllFromBytes(node.value)
			fmt.Println("Enter values to put in HLL until x is pressed.")
			var value string = ""
			fmt.Println(">> ")
			_, err := fmt.Scanln(&value)
			if err != nil{
				panic(err)
			}

			for value != "x"{
				hll.addData([]byte(value))
				fmt.Println(">> ")
				_, err := fmt.Scanln(&value)
				if err != nil {
					panic(err)
				}
			}
			memtable.insertToMemtable(key, hll.encodeHllToBytes(), 1)
		}

	}else if decision == "2"{
		// odraditi delete
		node := get(memtable, lru, key)
		if node == nil{
			return true
		}else{
			memtable.insertToMemtable(key, []byte(""), 2)
		}
	}else{
		fmt.Println("Neispravan unos.")
		return false
	}

	return true
}

func(memtable *Memtable) insertCmsToMemtable(key string, lru *LruCache) bool{
	/*
		Trazi od korisnika da unese odredjene parametre za CMS tabelu(epsilon delta)
		Standardne vrednosti za njih su 0.01 i 0.01
		Zatim korisnik redom popunjava tabelu i ona se dodaje u memtable
	*/

	key = key + "_cms"

	// ovde ide pitanje sta hoce put ili delete
	fmt.Println("1. Put")
	fmt.Println("2. Delete")
	var decision string
	fmt.Print("\nChoose option:\n>> ")
	// Taking input from user
	fmt.Scanln(&decision)

	if decision == "1"{
		node := get(memtable, lru, key)
		if node == nil{
			fmt.Println("Enter Epsilon:\n>> ")
			var epsilon, delta float64
			_, err := fmt.Scanln(&epsilon)
			if err != nil {
				panic(err)
			}
			fmt.Println("Enter Delta:\n>> ")
			_, err = fmt.Scanln(&delta)
			if err != nil {
				panic(err)
			}

			cms := &CountMinSketch{}
			cms.initializeCountMinSketch(epsilon, delta)

			fmt.Println("Enter values to put in CMS until x is pressed.")
			var value string = ""
			fmt.Println(">> ")
			_, err = fmt.Scanln(&value)
			if err != nil{
				panic(err)
			}
			for value != "x"{
				cms.addElement(value)
				fmt.Println(">> ")
				_, err := fmt.Scanln(&value)
				if err != nil {
					panic(err)
				}
			}
			fmt.Printf("CMS with name: %s sucesfully created.\n", key)

			memtable.insertToMemtable(key, cms.encodeCmsToBytes(), 0)
		}else{
			// deserijalizujemo i dodajemo nove vrednosti
			// deserijalizujemo i dodajemo nove vrednosti
			cms := &CountMinSketch{}
			cms.decodeCMSFromBytes(node.value)
			fmt.Println("Enter values to put in CMS until x is pressed.")
			var value string = ""
			fmt.Println(">> ")
			_, err := fmt.Scanln(&value)
			if err != nil{
				panic(err)
			}

			for value != "x"{
				cms.addElement(value)
				fmt.Println(">> ")
				_, err := fmt.Scanln(&value)
				if err != nil {
					panic(err)
				}
			}
			memtable.insertToMemtable(key, cms.encodeCmsToBytes(), 1)
		}

	}else if decision == "2"{
		// odraditi delete
		node := get(memtable, lru, key)
		if node == nil{
			return true
		}else{
			memtable.insertToMemtable(key, []byte(""), 2)
		}
	}else{
		fmt.Println("Neispravan unos.")
		return false
	}

	return true


	return true
}


func(memtable *Memtable) insertToMemtable(key string, value []byte, indicator int) bool{
	/*
		Put komanda za memtable koja proverava vrednost indikatora i na osnovu toga da li postoji element
		sa odredjenim kljucem u memtable taj indikator izmenjuje kako bi primenili adekvatnu funkciju
		add/edit/delete from skiplist, gde je delete logicko brisanje
		Takodje prati da li je predjena granica dozvoljenosti, tj threshold, ako jeste flushuje memtable
		pravi novi i prazni wal segmente.
	*/

	var node *SkipListNode
	if indicator != 0{
		node = memtable.skiplist.findNode(key)
		if node == nil{
			if indicator == 1 {
				indicator = 0

			} else
			{
				node = memtable.skiplist.addNode(key, value, uint64(time.Now().Unix()))
			}
		}
	}else{
		node = memtable.skiplist.findNode(key)
		if node != nil{

			indicator = 1

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

		resetWal("wal/")
		maxHeight := memtable.skiplist.maxHeight
		walThreshold := memtable.skiplist.walThreshold
		memtable.currentSize = 0
		memtable.skiplist = SkipList{}
		memtable.skiplist.createSkipList(maxHeight, memtable.threshold, walThreshold)
	}
	return true
}







