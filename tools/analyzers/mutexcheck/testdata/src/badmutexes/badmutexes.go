package _badmutexes

import "sync"

type MyBadStruct0 struct {
	mu      sync.Mutex // want "mutex isn't guarding anything, please annotate one of the fields"
	integer int
}

type MyBadStruct1 struct {
	mu      sync.RWMutex // want "mutex isn't guarding anything, please annotate one of the fields"
	integer int
}

type MyBadStruct2 struct {
	sync.Mutex // want "mutex isn't guarding anything, please annotate one of the fields"
	integer    int
}

type MyBadStruct3 struct {
	sync.Mutex     // GUARDED_BY(Mutex) // want "mutex shouldn't be guarded by a mutex"
	integer    int // GUARDED_BY(Mutex)
}

type MyBadStruct4 struct {
	sync.Mutex     // want "mutex isn't guarding anything, please annotate one of the fields"
	integer    int // GUARDED_BY(unknown) // want "field is guarded by an unknown mutex"
}

type MyBadStruct5 struct {
	sync.Mutex // want "mutex isn't guarding anything, please annotate one of the fields"
	// GUARDED_BY(unknown)
	integer int // want "field is guarded by an unknown mutex"
}
