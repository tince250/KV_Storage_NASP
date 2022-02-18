package main

import (
	"fmt"
)

func menu(){
	//PUT X
	//GET
	//DELETE X
	//START COMPACTION
	defVals := defValues{}
	defVals.getDefaultValues("config/config.yml")
	memtable := &Memtable{}
	memtable.initMemtableWithPassedValues(defVals.MemtableSize, defVals.Threshold, defVals.WalThreshold, defVals.MaxHeight)
	lru := &LruCache{}
	lru.initializeLRU(defVals.CacheSize) //TODO: DEFAULT VREDNOSTI ZA CACHE
	tb := &TokenBucket{}
	tb.initTokenBucket(5,5)
	for{

		fmt.Println("1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("4. COMPACT")
		fmt.Println("5. EXIT")

		var decision string
		fmt.Print("\nChoose option:\n>> ")
		// Taking input from user
		fmt.Scanln(&decision)
		if decision == "1"{
			fmt.Println("PUT")
			fmt.Println("Enter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			fmt.Println("Enter Value:\n>> ")
			var value string
			fmt.Scanln(&value)
			memtable.insertToMemtable(key, []byte(value), 0)
			if tb.addToken() {
				memtable.insertToMemtable(key, []byte(value), 0)
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "2"{
			fmt.Println("GET")
			fmt.Println("Enter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				node := get(memtable, lru, key)
				if node != nil {
					fmt.Println(node.key)
					fmt.Println(node.value)
				}
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "3"{
			fmt.Println("DELETE")
			fmt.Println("Enter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				memtable.insertToMemtable(key, []byte(""), 2)
				lru.deleteFromCache(key)
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "4"{
			fmt.Println("COMPACT FILES")
		}else if decision == "5"{
			fmt.Println("Exiting app...")
			break
		}else{
			fmt.Println("Invalid input. Try Again.\n")
		}

	}
}

func main(){
	menu()
}
