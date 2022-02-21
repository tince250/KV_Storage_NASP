package main

import (
	"fmt"
	"math/rand"
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


type SkipList struct {
	maxHeight uint64
	height    int
	threshold  uint64
	size      uint64
	walThreshold uint64
	head      *SkipListNode
}

type SkipListNode struct {
	Crc uint32
	Timestamp uint64
	Tombstone byte
	KeySize uint64
	ValueSize uint64
	key string
	Value []byte
	tombstone byte
	next      []*SkipListNode
}

func (s *SkipList) createSkipList(maxHeight uint64, treshold uint64, walThreshold uint64){
	s.maxHeight = maxHeight
	s.head = &SkipListNode{}
	s.head.key = ""
	s.head.tombstone = 0
	s.walThreshold = walThreshold
	s.head.next = make([]*SkipListNode, s.maxHeight)
	s.size = 0
	s.threshold = treshold
}

func(s *SkipList) findNode(key string) *SkipListNode{
	if key == s.head.key{
		return s.head
	}
	node:= s.head
	for i:=s.height;i>=0;i--{
		for node.next[i] != nil && node.next[i].key < key{
			node = node.next[i]
		}
	}
	node = node.next[0]
	if node != nil {
		if node.key == key {
			return node
		}
	}
	return nil
}


func(s *SkipList) editNode(value []byte, currentTime uint64, node *SkipListNode) bool{
		node.Value = value
		node.ValueSize = uint64(len(node.Value))
		node.Timestamp = currentTime
		return true
}

func(s *SkipList) inserFromWal(a []*Data) bool{
	for i:=0;i<len(a);i++{
		s.addFromWal(a[i])
	}

	return true
}

func(s* SkipList) addFromWal(a *Data) bool{

	// da li vec postoji? ako da promeniti odredjene vrednosti
	existingNode := s.findNode(a.key)
	if existingNode != nil{

		existingNode.Value = a.value
		existingNode.tombstone = a.ts
		existingNode.Timestamp = a.timeStamp
		return true
	}


	node := &SkipListNode{}
	node.key = a.key
	node.Value = a.value
	node.tombstone = a.ts
	node.Timestamp = a.timeStamp
	current := s.head
	previousArray := make([]*SkipListNode, s.maxHeight, s.maxHeight)

	// ide kroz redove i trazi elemente koji su
	// pre naseg kljuca
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < node.key {
			current = current.next[i]
		}
		previousArray[i] = current

	}

	// current je trenutno na poslednjem nivou, tj na nultom
	// i sad on pokazuje na element pre nase pozicije
	// na koju zelimo da ubacimo
	current = current.next[0] // prvi sledeci je nasa poz
	if current == nil || current.key != node.key {

		newNodeLevel := s.roll()
		node.next = make([]*SkipListNode, newNodeLevel+1, newNodeLevel+1)

		if newNodeLevel > s.height {

			for i := s.height + 1; i < newNodeLevel+1; i++ {
				previousArray[i] = s.head
				// povecavamo visinu i na pocetak
				// stavljamo header
			}
			s.height = newNodeLevel
		}
		for i := 0; i <= newNodeLevel; i++ {
			if previousArray[i] != nil {
				if previousArray[i].next[i] != nil {
					node.next[i] = previousArray[i].next[i]
				}
				previousArray[i].next[i] = node
			}

		}
		s.size++

	}
	return true
}


func(s *SkipList) addNode(key string, value []byte, timestamp uint64) *SkipListNode{

	node := &SkipListNode{}
	node.key = key
	node.Value = value
	node.tombstone = 0
	node.Timestamp = timestamp

	current := s.head
	previousArray := make([]*SkipListNode, s.maxHeight, s.maxHeight)

		// ide kroz redove i trazi elemente koji su
		// pre naseg kljuca
		for i := s.height; i >= 0; i-- {
			for current.next[i] != nil && current.next[i].key < key {
				current = current.next[i]
			}
			previousArray[i] = current

		}

		// current je trenutno na poslednjem nivou, tj na nultom
		// i sad on pokazuje na element pre nase pozicije
		// na koju zelimo da ubacimo
		current = current.next[0] // prvi sledeci je nasa poz
		if current == nil || current.key != key {

			newNodeLevel := s.roll()
			node.next = make([]*SkipListNode, newNodeLevel+1, newNodeLevel+1)

			if newNodeLevel > s.height {

				for i := s.height + 1; i < newNodeLevel+1; i++ {
					previousArray[i] = s.head
					// povecavamo visinu i na pocetak
					// stavljamo header
				}
				s.height = newNodeLevel
			}
			for i := 0; i <= newNodeLevel; i++ {
				if previousArray[i] != nil {
					if previousArray[i].next[i] != nil {
						node.next[i] = previousArray[i].next[i]
					}
					previousArray[i].next[i] = node
				}

			}
			s.size++

			return node
		}

	return nil
}

func(s *SkipList) logicalDelete(timestamp uint64, nodeToDelete *SkipListNode) bool{
	nodeToDelete.tombstone = 1
	nodeToDelete.Timestamp = timestamp
	return true

}

func(s *SkipList) printList(){
	fmt.Println("---------------- SKIP LISTA --------------")
	for i:=0;i<s.height;i++{
		node := s.head.next[i]
		fmt.Printf("Nivo %d.\n", i)
		for node != nil{
			fmt.Printf("%s [%s] [%b]  %d | ", node.key, node.Value, node.tombstone, node.Timestamp)
			node = node.next[i]

		}
		fmt.Println("")
	}
}


func (s *SkipList) roll() int {
	level := 0 // alwasy start from level 0

	// We roll until we don't get 1 from rand function and we did not
	// outgrow maxHeight. BUT rand can give us 0, and if that is the case
	// than we will just increase level, and wait for 1 from rand!
	rand_numb  := rand.Int31n(2)

	for ; rand_numb == 1; level++ {
		rand_numb  = rand.Int31n(2)
		if level > s.height {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			s.height = level
			return level
		}

	}
	return level
}
