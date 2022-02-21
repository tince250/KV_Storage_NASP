package main

type LruCache struct {
	keyMap map[string]*dllNode
	dll doublyLinkedList
	size uint32

}

func(lru *LruCache) initializeLRU(size uint32){
	/*
		Konstruktor za inicijalizaciju Lru-a, parametar je dozvoljena velicina
	*/
	lru.size = size
	lru.dll.initList(lru.size)
	lru.keyMap = make(map[string]*dllNode)

}

func (lru *LruCache) getFromCache(key string) *dllNode{
	/*
		Vraca element iz kesa sa kljucem ako postoji.
		Proverava u mapi da li je tu, ako jeste onda ga stavlja na poslednju poziciju u skiplisti
		tj na poziciju koja predstavlja nedavno koriscen element
	*/
	value, found := lru.keyMap[key]
	if found{
		lru.dll.putToLastPosition(value)
		return value

	}else{
		return nil
	}

}

func(lru *LruCache) deleteFromCache(key string) bool{
	/*
		Ako element postoji brise ga prvo iz mape, i zatim iz liste
	*/
	value, found := lru.keyMap[key]
	if found{
		delete(lru.keyMap, key)
		lru.dll.deleteDllNode(value)
		return true
	}

	return false
}

func(lru *LruCache) addToCache(key string, value []byte, toombstone byte, timestamp uint64) bool{
	/*
		Ako postoji vec element u kesu samo ga stavlja na posl poziciju u listi
		a u suprotnom kreira novi i dodaje ga na posl poziciju i stavlja ga u mapu
	*/
	node, found := lru.keyMap[key]
	if found {
		lru.dll.putToLastPosition(node)
	}else{
		node := lru.dll.AddEndNodeDLL(key, value, toombstone, timestamp)
		lru.keyMap[key] = node
	}
	return true

}

func(lru *LruCache) addDataToCache(data *Data) bool{
	/*
		Ako postoji vec element u kesu samo ga stavlja na posl poziciju u listi
		a u suprotnom kreira novi i dodaje ga na posl poziciju i stavlja ga u mapu
	*/
	node, found := lru.keyMap[data.key]
	if found {
		lru.dll.putToLastPosition(node)
	}else{
		node := lru.dll.AddEndNodeDLL(data.key, data.value, data.ts, data.timeStamp)
		lru.keyMap[data.key] = node
	}
	return true

}