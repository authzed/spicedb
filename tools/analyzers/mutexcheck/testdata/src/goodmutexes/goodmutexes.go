package _goodmutexes

import "sync"

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
