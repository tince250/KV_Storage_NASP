package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type SimHash struct{
	text string
	words map[string]int
	fingerprint []int

}
func readFromFile(path string) []string{
	file, err := os.Open(path)
	if err != nil{
		panic(err)
	}
	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan(){
		line:= scanner.Text()
		lines = append(lines, line)
	}
	err = file.Close()
	if err != nil {
		return nil
	}
	return lines
}

func readFileToString(path string) string{
	b, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Print(err)
	}

	str := string(b)
	return str
}

func(sh *SimHash) createFingerprint(){

	stopwords := readFromFile("resources/stop_words_english.txt")
	words := strings.Split(sh.text, " ")
	for i:=0;i<len(words);i++{
		words[i] = strings.ToLower(words[i])
		if strings.LastIndexByte(words[i], ',') == len(words[i]) -1 {
			words[i] = strings.TrimSuffix(words[i], ",")
		}else if strings.LastIndexByte(words[i], '.') == len(words[i]) -1 {
			words[i] = strings.TrimSuffix(words[i], ".")
		}else if strings.LastIndexByte(words[i], ')') == len(words[i]) -1 {
			words[i] = strings.TrimSuffix(words[i], ".")
		}else if strings.LastIndexByte(words[i], '(') == len(words[i]) -1 {
			words[i] = strings.TrimSuffix(words[i], ".")
		}


	}
	sh.words = make(map[string]int)
	for i:=0;i<len(words);i++{
		isValid := true
		for j:=0;j<len(stopwords);j++{
			if words[i] == stopwords[j]{
				isValid = false
				break
			}
		}
		if isValid{
			sh.words[words[i]] += 1
		}
	}


	table := make([][]string, len(sh.words))
	for i := range table{
		table[i] = make([]string, 256)
	}
	index := 0
	for i := range sh.words{
		bh := ToBinary(GetMD5Hash(i))

		for j:=0;j<len(bh);j++{
			if bh[j]=='0'{
				table[index][j] = "-1"
			}else{
				table[index][j] = "1"
			}

		}
		index++
	}

	columnSum := make([]int, 256)
	index = 0

	for _, j:=range sh.words{
		for k:=0;k<len(table[index]);k++{
			weight, err := strconv.Atoi(table[index][k])
			if err != nil{
				panic(err)
			}
			columnSum[k] += weight * j

		}
		index++
	}

	for i:=0;i<len(columnSum);i++{
		if columnSum[i] > 0{
			columnSum[i] = 1
		}else{
			columnSum[i] = 0
		}
	}
	sh.fingerprint = columnSum
}

func hemmingDistance(fgpt1, fgpt2 []int) int{
	counter := 0
	for i:=0;i<len(fgpt2);i++{
		if (fgpt1[i]==0 && fgpt2[i]==1) || (fgpt2[i]==0 && fgpt1[i]==1){
			counter ++
		}
	}
	return counter

}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

