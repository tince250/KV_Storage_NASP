package main

import (
	"fmt"
)

func menu(){
	defVals := defValues{}
	defVals.getDefaultValues("config/config.yml")
	memtable := &Memtable{}
	memtable.initMemtableWithPassedValues(defVals.MemtableSize, defVals.Threshold, defVals.WalThreshold, defVals.MaxHeight)
	lru := &LruCache{}
	lru.initializeLRU(defVals.CacheSize)
	tb := &TokenBucket{}
	tb.initTokenBucket(defVals.TokenNumber,defVals.TokenReset)

	fmt.Println("------------Wlecome to the Team7 engine------------")
	for{
		fmt.Println("\n1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("4. COMPACT")
		fmt.Println("5. HLL CONTROL")
		fmt.Println("6. CMS CONTROL")
		fmt.Println("7. EXIT")

		var decision string
		fmt.Print("\nChoose option:\n>> ")
		// Taking input from user
		fmt.Scanln(&decision)
		if decision == "1"{
			fmt.Println("PUT")
			fmt.Print("Enter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			fmt.Print("Enter Value:\n>> ")
			var value string
			fmt.Scanln(&value)
			if tb.addToken() {
				memtable.insertToMemtable(key, []byte(value), 0)
				fmt.Println("Data successfully recorded.")
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "2"{
			fmt.Println("GET")
			fmt.Println("*If u want to get Hll elements or CMS elements at the end of the name add _hll/_cms.\nExample: NaspTeam_hll")
			fmt.Print("\nEnter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				node := get(memtable, lru, key)
				if node != nil {
					fmt.Println(node.key)
					fmt.Println(node.value)
				}else{
					fmt.Println("No data found.")
				}
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "3"{
			fmt.Println("DELETE")
			fmt.Print("Enter Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				memtable.insertToMemtable(key, []byte(""), 2)
				lru.deleteFromCache(key)
				fmt.Println("Data successfully deleted.")
			}else{
				fmt.Println("Token bucket full.")
			}
		}else if decision == "4"{
			fmt.Println("COMPACT FILES")
			compact(defVals.LsmLevel, defVals.MaxTablesPerLevel)

		}else if decision == "5"{
			fmt.Print("Enter HLL Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				memtable.insertHllToMemtable(key, lru)
			}else{
				fmt.Println("Token bucket full.")
			}

		}else if decision == "6"{
			fmt.Print("Enter CMS Key:\n>> ")
			var key string
			fmt.Scanln(&key)
			if tb.addToken() {
				memtable.insertCmsToMemtable(key, lru)
			}else{
				fmt.Println("Token bucket full.")
			}

		} else if decision == "7"{
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
