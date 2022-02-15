package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (merkle *MerkleRoot) formMerkle(data [][]byte){

	nodeArray := make([]*Node, len(data))
	for i:=0;i<len(nodeArray);i++{
		nodeArray[i] = &Node{Hash(data[i]), nil, nil}

	}
	merkle.formMerklePrivate(nodeArray)
}

func(merkle *MerkleRoot) formMerklePrivate(data []*Node){
	// da li su samo 2? ako jesu onda je root
	if len(data) == 2{
		merkle.root = &Node{Hash(append(data[0].data[:], data[1].data[:]...)), data[0], data[1]}
	} else{
		if len(data) % 2 !=0 {
			data = append(data, &Node{[20]byte{}, nil, nil})
		}
		newData := make([]*Node, uint32(len(data)/2))
		k := 0
		for i := 0; i < len(data) - 1; i += 2 {
			newData[k] = &Node{Hash(append(data[i].data[:], data[i+1].data[:]...)), data[i], data[i+1]}
			k += 1
		}
		merkle.formMerklePrivate(newData)
	}
}



func(merkle *MerkleRoot) serializeMerkle(filename string) {

	file, err := os.OpenFile(filename, os.O_WRONLY, 0777)
	if err!=nil{

		panic(err)
	}

	height := getHeight(merkle.root)
	for i:=1;i<=height;i++{
		writeCurrentLevel(merkle.root, i, file)
	}
	err = file.Close()
	if err != nil {
		return
	}

}
func writeCurrentLevel(node *Node, level int, file *os.File){
	if node == nil{
		return
	}
	if level == 1{
		//err := binary.Write(file, binary.LittleEndian, node.String())
		byteSlice := []byte(node.String() + " ")
		_, err := file.Write(byteSlice)
		if err != nil{
			log.Fatal()
		}

	}else if level > 1{
		writeCurrentLevel(node.left, level - 1, file)
		writeCurrentLevel(node.right, level - 1, file)
	}
}

func(merkle *MerkleRoot) buildMerkleFromFile(filename string){
	listOfData := loadMerkleFile(filename)
	decodedData := make([][]byte, len(listOfData), len(listOfData))
	for i:=0;i<len(listOfData);i++{
		decodedData[i] = decodeString(listOfData[i])
		fmt.Println(decodedData[i])
	}
}

func loadMerkleFile(filename string) []string{
	file1, err := os.Stat(filename)
	if err !=nil{
		panic(err)
	}
	size := file1.Size()

	file, err := os.OpenFile(filename, os.O_RDONLY, 0777)
	if err!=nil{

		panic(err)
	}
	data := make([]byte, size)
	_, err = file.ReadAt(data, 0)
	if err != nil {
		return nil
	}
	file.Close()
	listOfData := make([]string, size/41, size/41)
	k:= 0
	for i:=0;i<len(data);i+=41{
		toAppend := string(data[i:i + 40])
		listOfData[k] = toAppend
		k++
	}

	return listOfData

}


func getHeight(node *Node) int{
	if node == nil{
		return 0
	}else{
		leftHeight := getHeight(node.left)
		rightHeight := getHeight(node.right)
		if leftHeight > rightHeight{
			return leftHeight + 1
		}else{
			return rightHeight + 1
		}
	}
}

func(merkle *MerkleRoot) printBreadthFirst(root *Node){
	height := getHeight(root)
	for i:=1;i<=height;i++{
		printCurrentLvl(root, i)
	}
}

func printCurrentLvl(root *Node, level int){
	if root == nil{
		return
	}
	if level == 1{
		fmt.Println(root.data)
	}else if level > 1{
		printCurrentLvl(root.left, level - 1)
		printCurrentLvl(root.right, level - 1)
	}
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}
func decodeString(data string) []byte{
	decoded, err := hex.DecodeString(data)
	if err != nil{
		panic(err)
	}
	return decoded
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}
