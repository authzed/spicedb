package _goodmutexes

import (
	"fmt"
	"sync"
)

type MyGoodStruct struct {
	mu    sync.Mutex
	field int // GUARDED_BY(mu)
}

type MyGoodStruct2 struct {
	mu1, mu2 sync.Mutex
	field1   int // GUARDED_BY(mu1)
	field2   int // GUARDED_BY(mu2)
}

type MyGoodStruct3 struct {
	field int // GUARDED_BY(mu)
	mu    sync.Mutex
}

type MyGoodStruct4 struct {
	field int // GUARDED_BY(Mutex)
	sync.Mutex
}

type MyGoodStruct5 struct {
	field int // GUARDED_BY(RWMutex)
	sync.RWMutex
}

type MyGoodStruct6 struct {
	mu sync.RWMutex

	// GUARDED_BY(mu)
	field int
}

type MyGoodStruct7 struct {
	a     fmt.Formatter // GUARDED_BY(mutex)
	b     fmt.Formatter // GUARDED_BY(mutex)
	mutex sync.Mutex
}

type MyGoodStruct8 struct {
	a     map[string]int // GUARDED_BY(mutex)
	b     map[string]int // GUARDED_BY(mutex)
	mutex sync.Mutex
}

type MyGoodStruct9 struct {
	a     sync.Pool // GUARDED_BY(mutex)
	b     sync.Pool // GUARDED_BY(mutex)
	mutex sync.Mutex
}

type MyGoodStruct10 struct {
	a        sync.Pool // GUARDED_BY(mutexPtr)
	mutexPtr *sync.Mutex
}

type MyGoodStruct11 struct {
	a sync.Pool // GUARDED_BY(Mutex)
	*sync.Mutex
}

type MyGoodStruct12 struct {
	a *int // GUARDED_BY(Mutex)
	*sync.Mutex
}
