package main

import (
	"time"
)

type TokenBucket struct {
	initTokens uint64
	tokens     uint64
	resetTimer uint64
	time       time.Time
}

func (tb *TokenBucket) initTokenBucket(tokens uint64, resetTimer uint64) {
	tb.tokens = tokens
	tb.initTokens = tokens
	tb.resetTimer = resetTimer

	//fmt.Print(tb.time)
}

func (tb *TokenBucket) checkTime() bool {
	newTime := time.Now().Local()
	//fmt.Println(newTime)
	//fmt.Println(tb.time)
	if newTime.After(tb.time) {
		tb.tokens = tb.initTokens
		tb.time = time.Now().Local().Add(time.Second * time.Duration(tb.resetTimer))
		return true
	}
	return false
}
func (tb *TokenBucket) checkTokens() bool {
	tb.checkTime()
	if tb.tokens == 0 {
		return false
	}
	return true
}

func (tb *TokenBucket) addToken() bool {
	if tb.tokens == tb.initTokens {
		tb.time = time.Now().Local().Add(time.Second * time.Duration(tb.resetTimer))
	}
	if tb.checkTokens() {
		tb.tokens--
	} else {

		return false
	}
	return true
}
