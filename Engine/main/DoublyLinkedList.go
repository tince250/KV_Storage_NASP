	package main

import "fmt"

type dllNode struct {
	key string
	value []byte
	ts byte
	timeStamp uint64
	prev *dllNode
	next *dllNode
}

type doublyLinkedList struct {
	maxSize uint32
	len  uint32
	tail *dllNode
	head *dllNode
}

func (d *doublyLinkedList) initList(maxSize uint32) {
	d.maxSize = maxSize
	d.head = &dllNode{"",  []byte(""), 0, 0, nil, nil}
	d.tail = &dllNode{"",  []byte(""), 0, 0, nil, nil}
}

func(d *doublyLinkedList) putToLastPosition(node *dllNode){
	key := node.key
	value := node.value
	ts := node.ts
	timestamp := node.timeStamp
	d.deleteDllNode(node)
	d.AddEndNodeDLL(key, value, ts, timestamp)
}

//func (d *doublyLinkedList) AddFrontNodeDLL(data string) {
//	if d.len >= d.maxSize - 1{
//		d.rotateDLL()
//	}
//	newNode := &dllNode{
//		data: data,
//	}
//	if d.head == nil {
//		d.head = newNode
//		d.tail = newNode
//	} else {
//		newNode.next = d.head
//		d.head.prev = newNode
//		d.head = newNode
//	}
//	d.len++
//	return
//}

func (d *doublyLinkedList) AddEndNodeDLL(key string, value []byte, toombstone byte, timestamp uint64) *dllNode{
	if d.len >= d.maxSize - 1{
		d.rotateDLL()
	}
	newNode := &dllNode{key, value, toombstone, timestamp, nil, nil}
	if d.head.next == nil {
		d.head.next = newNode
		d.tail.prev = newNode
		newNode.prev = d.head
		newNode.next = d.tail
	} else {
		newNode.next = d.tail
		newNode.prev = d.tail.prev
		d.tail.prev.next = newNode
		d.tail.prev = newNode

	}
	d.len++
	return newNode
}

func(d *doublyLinkedList) rotateDllRight(){
	current := d.tail.prev
	d.tail.prev = d.tail.prev.prev
	d.tail.prev.next = d.tail
	d.len--
	current.next = nil
	current.prev = nil
	current = nil
}

func(d *doublyLinkedList) rotateDLL(){
	current := d.head.next
	d.head.next = d.head.next.next
	d.head.next.prev = d.head
	d.len--
	current.next = nil
	current.prev = nil
	current = nil
}

func(d *doublyLinkedList) isEmpty() bool{
	return d.head == d.tail
}

//func(d *doublyLinkedList) nodeExists(key string) *dllNode{
//	start := d.head
//	for start != nil{
//		if start.key == key{
//			return start
//		}
//		start = start.next
//	}
//	return nil
//}

func(d *doublyLinkedList) deleteDllNode(node *dllNode) bool{
	if node != nil{
		previousNode := node.prev
		nextNode := node.next
		previousNode.next = nextNode
		nextNode.prev = previousNode
		node.prev = nil
		node.next = nil
		node = nil
		d.len--
		return true

	} else{
		return false
	}

}

func (d *doublyLinkedList) TraverseForward() error {
	if d.head == nil {
		return fmt.Errorf("TraverseError: List is empty")
	}
	temp := d.head
	for temp != nil {
		fmt.Printf("value = %v\n", temp.key)

		temp = temp.next
	}

	fmt.Println()
	return nil
}

func (d *doublyLinkedList) TraverseReverse() error {
	if d.head == nil {
		return fmt.Errorf("TraverseError: List is empty")
	}
	temp := d.tail
	for temp != nil {
		if temp.key != "" {
			fmt.Printf("value = %v\n", temp.key)
		}
		temp = temp.prev
	}
	fmt.Println()
	return nil
}

func (d *doublyLinkedList) Size() uint32 {
	return d.len
}