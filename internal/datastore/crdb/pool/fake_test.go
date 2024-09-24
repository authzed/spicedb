package pool

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

var (
	_ balanceablePool[*FakePoolConn[*FakeConn], *FakeConn] = &FakePool{}
	_ balancePoolConn[*FakeConn]                           = &FakePoolConn[*FakeConn]{}
)

type FakeConn struct {
	closed bool
}

func NewFakeConn() *FakeConn {
	return &FakeConn{closed: false}
}

func (f FakeConn) IsClosed() bool {
	return f.closed
}

type FakePoolConn[C balanceConn] struct {
	conn C
}

func (f FakePoolConn[C]) Conn() C {
	return f.conn
}

func (f FakePoolConn[C]) Release() {}

type FakePool struct {
	sync.Mutex
	id          string
	maxConns    uint32
	gc          map[*FakeConn]uint32
	nodeForConn map[*FakeConn]uint32
}

func NewFakePool(maxConns uint32) *FakePool {
	return &FakePool{
		id:          uuid.NewString(),
		maxConns:    maxConns,
		gc:          make(map[*FakeConn]uint32, 0),
		nodeForConn: make(map[*FakeConn]uint32, 0),
	}
}

func (f *FakePool) ID() string {
	return f.id
}

func (f *FakePool) AcquireAllIdle(_ context.Context) []*FakePoolConn[*FakeConn] {
	conns := make([]*FakePoolConn[*FakeConn], 0, len(f.nodeForConn))
	f.Range(func(conn *FakeConn, value uint32) {
		conns = append(conns, &FakePoolConn[*FakeConn]{conn: conn})
	})
	return conns
}

func (f *FakePool) Node(conn *FakeConn) uint32 {
	f.Lock()
	defer f.Unlock()
	id := f.nodeForConn[conn]
	return id
}

func (f *FakePool) GC(conn *FakeConn) {
	f.Lock()
	defer f.Unlock()
	id := f.nodeForConn[conn]
	delete(f.nodeForConn, conn)
	f.gc[conn] = id
}

func (f *FakePool) MaxConns() uint32 {
	return f.maxConns
}

func (f *FakePool) Range(fn func(conn *FakeConn, nodeID uint32)) {
	f.Lock()
	defer f.Unlock()
	for k, v := range f.nodeForConn {
		fn(k, v)
	}
}
