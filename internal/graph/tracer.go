package graph

import (
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

type DebugTracerEmitter interface {
	EmitForTrace(tracer DebugTracer)
}

type DebugTracer interface {
	Child(name string) DebugTracer
	Childf(format string, a ...interface{}) DebugTracer
	ChildONR(onr *pb.ObjectAndRelation) DebugTracer
	Add(name string, value DebugTracerEmitter)
	String() string
}

func NewNullTracer() DebugTracer {
	return nullTracer{}
}

type nullTracer struct {
}

func (nt nullTracer) Child(name string) DebugTracer {
	return nt
}

func (nt nullTracer) Childf(format string, a ...interface{}) DebugTracer {
	return nt
}

func (nt nullTracer) ChildONR(onr *pb.ObjectAndRelation) DebugTracer {
	return nt
}

func (nt nullTracer) Add(name string, value DebugTracerEmitter) {
}

func (nt nullTracer) String() string {
	return ""
}

func NewPrinterTracer() DebugTracer {
	return &printerTracer{treeprinter.New()}
}

type printerTracer struct {
	tpNode treeprinter.Node
}

func (pt *printerTracer) Child(name string) DebugTracer {
	return &printerTracer{pt.tpNode.Child(name)}
}

func (pt *printerTracer) ChildONR(onr *pb.ObjectAndRelation) DebugTracer {
	return pt.Child(tuple.StringONR(onr))
}

func (pt *printerTracer) Childf(format string, a ...interface{}) DebugTracer {
	return &printerTracer{pt.tpNode.Childf(format, a...)}
}

func (pt *printerTracer) Add(name string, value DebugTracerEmitter) {
	ct := pt.Child(name)
	value.EmitForTrace(ct)
}

func (pt *printerTracer) String() string {
	return pt.tpNode.String()
}
