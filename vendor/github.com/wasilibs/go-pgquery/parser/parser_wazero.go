//go:build !tinygo && !pgquery_cgo

package parser

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/wasilibs/go-pgquery/internal/wasix_32v1"
	"github.com/wasilibs/go-pgquery/internal/wasm"
	"github.com/wasilibs/wazero-helpers/allocator"
)

var (
	errFailedWrite = errors.New("failed to write to wasm memory")
	errFailedRead  = errors.New("failed to read from wasm memory")
)

// TODO(anuraaga): Use shared memory with child modules instead of fresh runtimes per call.
func newRT() (wazero.Runtime, wazero.CompiledModule) {
	ctx := context.Background()

	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithCompilationCache(wazero.NewCompilationCache()).
		WithCoreFeatures(api.CoreFeaturesV2|experimental.CoreFeaturesThreads))

	wasi_snapshot_preview1.MustInstantiate(ctx, rt)
	wasix_32v1.MustInstantiate(ctx, rt)

	code, err := rt.CompileModule(ctx, wasm.LibPGQuery)
	if err != nil {
		panic(err)
	}

	return rt, code
}

// ParseToJSON - Parses the given SQL statement into a parse tree (JSON format)
func ParseToJSON(input string) (result string, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryParse(inputC)
}

// ParseToProtobuf - Parses the given SQL statement into a parse tree (Protobuf format)
func ParseToProtobuf(input string) (result []byte, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryParseProtobuf(inputC)
}

// DeparseFromProtobuf - Deparses the given Protobuf format parse tree into a SQL statement
func DeparseFromProtobuf(input []byte) (result string, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCStringFromBytes(input)
	defer inputC.Close()

	return abi.pgQueryDeParseFromProtobuf(inputC)
}

// Scans the given SQL statement into a protobuf ScanResult
func ScanToProtobuf(input string) (result []byte, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryScanProtobuf(inputC)
}

// ParsePlPgSqlToJSON - Parses the given PL/pgSQL function statement into a parse tree (JSON format)
func ParsePlPgSqlToJSON(input string) (result string, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryParsePlPgSqlToJSON(inputC)
}

// Normalize the passed SQL statement to replace constant values with ? characters
func Normalize(input string) (result string, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryNormalize(inputC)
}

// FingerprintToUInt64 - Fingerprint the passed SQL statement using the C extension and returns result as uint64
func FingerprintToUInt64(input string) (result uint64, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryFingerprintToUint64(inputC)
}

// FingerprintToHexStr - Fingerprint the passed SQL statement using the C extension and returns result as hex string
func FingerprintToHexStr(input string) (result string, err error) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCString(input)
	defer inputC.Close()

	return abi.pgQueryFingerprintToHexStr(inputC)
}

// HashXXH3_64 - Helper method to run XXH3 hash function (64-bit variant) on the given bytes, with the specified seed
func HashXXH3_64(input []byte, seed uint64) (result uint64) {
	abi := getABI()
	defer abi.Close()

	inputC := abi.newCStringFromBytes(input)
	defer inputC.Close()

	return uint64(abi.pgQueryHashXXH364(inputC, seed))
}

var (
	abiPool   = list.New()
	abiPoolMu sync.Mutex
)

func newABI() *abi {
	ctx := context.Background()
	ctx = experimental.WithMemoryAllocator(ctx, allocator.NewNonMoving())

	rt, code := newRT()
	cfg := wazero.NewModuleConfig().WithSysNanotime().WithStdout(os.Stdout).WithStderr(os.Stderr).WithStartFunctions("_initialize")
	mod, err := rt.InstantiateModule(ctx, code, cfg)
	if err != nil {
		panic(err)
	}
	res := &abi{
		fPgQueryInit:                    newLazyFunction(rt, mod, "pg_query_init"),
		fPgQueryParse:                   newLazyFunction(rt, mod, "pg_query_parse"),
		fPgQueryFreeParseResult:         newLazyFunction(rt, mod, "pg_query_free_parse_result"),
		fPgQueryParseProtobuf:           newLazyFunction(rt, mod, "pg_query_parse_protobuf"),
		fPgQueryFreeProtobufParseResult: newLazyFunction(rt, mod, "pg_query_free_protobuf_parse_result"),
		fPgQueryParsePlpgsql:            newLazyFunction(rt, mod, "pg_query_parse_plpgsql"),
		fPgQueryFreePlpgsqlParseResult:  newLazyFunction(rt, mod, "pg_query_free_plpgsql_parse_result"),
		fPgQueryScan:                    newLazyFunction(rt, mod, "pg_query_scan"),
		fPgQueryFreeScanResult:          newLazyFunction(rt, mod, "pg_query_free_scan_result"),
		fPgQueryNormalize:               newLazyFunction(rt, mod, "pg_query_normalize"),
		fPgQueryFreeNormalizeResult:     newLazyFunction(rt, mod, "pg_query_free_normalize_result"),
		fPgQueryFingerprint:             newLazyFunction(rt, mod, "pg_query_fingerprint"),
		fPgQueryFreeFingerprintResult:   newLazyFunction(rt, mod, "pg_query_free_fingerprint_result"),
		fPgQueryDeparseProtobuf:         newLazyFunction(rt, mod, "pg_query_deparse_protobuf"),
		fPgQueryFreeDeparseResult:       newLazyFunction(rt, mod, "pg_query_free_deparse_result"),
		hashXXH364:                      newLazyFunction(rt, mod, "XXH3_64bits_withSeed"),

		malloc: newLazyFunction(rt, mod, "malloc"),
		free:   newLazyFunction(rt, mod, "free"),

		mod:        mod,
		wasmMemory: mod.Memory(),
		rt:         rt,
	}

	res.pgQueryInit()
	runtime.SetFinalizer(res, func(r *abi) {
		r.rt.Close(context.Background())
	})

	return res
}

func getABI() *abi {
	abiPoolMu.Lock()

	e := abiPool.Front()
	if e == nil {
		abiPoolMu.Unlock()
		return newABI()
	}

	abiPool.Remove(e)
	abiPoolMu.Unlock()
	return e.Value.(*abi)
}

type abi struct {
	fPgQueryInit                    lazyFunction
	fPgQueryParse                   lazyFunction
	fPgQueryFreeParseResult         lazyFunction
	fPgQueryParseProtobuf           lazyFunction
	fPgQueryFreeProtobufParseResult lazyFunction
	fPgQueryParsePlpgsql            lazyFunction
	fPgQueryFreePlpgsqlParseResult  lazyFunction
	fPgQueryScan                    lazyFunction
	fPgQueryFreeScanResult          lazyFunction
	fPgQueryNormalize               lazyFunction
	fPgQueryFreeNormalizeResult     lazyFunction
	fPgQueryFingerprint             lazyFunction
	fPgQueryFreeFingerprintResult   lazyFunction
	fPgQueryDeparseProtobuf         lazyFunction
	fPgQueryFreeDeparseResult       lazyFunction
	hashXXH364                      lazyFunction

	malloc lazyFunction
	free   lazyFunction

	wasmMemory api.Memory

	mod api.Module
	rt  wazero.Runtime
}

func (abi *abi) Close() error {
	abiPoolMu.Lock()
	abiPool.PushBack(abi)
	abiPoolMu.Unlock()
	return nil
}

func (abi *abi) pgQueryInit() {
	abi.fPgQueryInit.Call0(context.Background())
}

func (abi *abi) pgQueryParse(input cString) (result string, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 12)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryParse.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeParseResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 12)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[8:])
	if errPtr != 0 {
		return "", newPgQueryError(abi.mod, errPtr)
	}

	result = readCStringPtr(abi.wasmMemory, uint32(resPtr))

	return
}

func (abi *abi) pgQueryParseProtobuf(input cString) (result []byte, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 16)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryParseProtobuf.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeProtobufParseResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 16)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[12:])
	if errPtr != 0 {
		return nil, newPgQueryError(abi.mod, errPtr)
	}

	pgQueryProtobufLen := binary.LittleEndian.Uint32(resBuf)
	pgQueryProtobufData := binary.LittleEndian.Uint32(resBuf[4:])

	buf, ok := abi.wasmMemory.Read(pgQueryProtobufData, pgQueryProtobufLen)
	if !ok {
		panic(errFailedRead)
	}

	result = bytes.Clone(buf)

	return
}

func (abi *abi) pgQueryDeParseFromProtobuf(input cString) (result string, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 8)
	defer abi.free.Call1(ctx, resPtr)

	paramPtr := abi.malloc.Call1(ctx, 8)
	defer abi.free.Call1(ctx, paramPtr)

	abi.wasmMemory.WriteUint32Le(uint32(paramPtr), uint32(input.length))
	abi.wasmMemory.WriteUint32Le(uint32(paramPtr+4), uint32(input.ptr))

	abi.fPgQueryDeparseProtobuf.Call2(ctx, resPtr, paramPtr)
	defer abi.fPgQueryFreeDeparseResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 8)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[4:])
	if errPtr != 0 {
		return "", newPgQueryError(abi.mod, errPtr)
	}

	result = readCStringPtr(abi.wasmMemory, uint32(resPtr))

	return
}

func (abi *abi) pgQueryScanProtobuf(input cString) (result []byte, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 16)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryScan.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeScanResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 16)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[12:])
	if errPtr != 0 {
		return nil, newPgQueryError(abi.mod, errPtr)
	}

	pgQueryProtobufLen := binary.LittleEndian.Uint32(resBuf)
	pgQueryProtobufData := binary.LittleEndian.Uint32(resBuf[4:])

	buf, ok := abi.wasmMemory.Read(pgQueryProtobufData, pgQueryProtobufLen)
	if !ok {
		panic(errFailedRead)
	}

	result = bytes.Clone(buf)

	return
}

func (abi *abi) pgQueryNormalize(input cString) (result string, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 8)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryNormalize.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeNormalizeResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 8)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[4:])
	if errPtr != 0 {
		return "", newPgQueryError(abi.mod, errPtr)
	}

	result = readCStringPtr(abi.wasmMemory, uint32(resPtr))

	return
}

func (abi *abi) pgQueryParsePlPgSqlToJSON(input cString) (result string, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 8)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryParsePlpgsql.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreePlpgsqlParseResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 8)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[4:])
	if errPtr != 0 {
		return "", newPgQueryError(abi.mod, errPtr)
	}

	result = readCStringPtr(abi.wasmMemory, uint32(resPtr))

	return
}

func (abi *abi) pgQueryFingerprintToUint64(input cString) (result uint64, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 20)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryFingerprint.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeFingerprintResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 20)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[16:])
	if errPtr != 0 {
		return 0, newPgQueryError(abi.mod, errPtr)
	}

	result = binary.LittleEndian.Uint64(resBuf)

	return
}

func (abi *abi) pgQueryFingerprintToHexStr(input cString) (result string, err error) {
	ctx := wasix_32v1.BackgroundContext()

	resPtr := abi.malloc.Call1(ctx, 20)
	defer abi.free.Call1(ctx, resPtr)

	abi.fPgQueryFingerprint.Call2(ctx, resPtr, uint64(input.ptr))
	defer abi.fPgQueryFreeFingerprintResult.Call1(ctx, resPtr)

	resBuf, ok := abi.wasmMemory.Read(uint32(resPtr), 20)
	if !ok {
		panic(errFailedRead)
	}

	errPtr := binary.LittleEndian.Uint32(resBuf[16:])
	if errPtr != 0 {
		return "", newPgQueryError(abi.mod, errPtr)
	}

	result = readCStringPtr(abi.wasmMemory, uint32(resPtr)+8)

	return
}

func (abi *abi) pgQueryHashXXH364(input cString, seed uint64) int {
	ctx := wasix_32v1.BackgroundContext()

	res := abi.hashXXH364.Call3(ctx, uint64(input.ptr), uint64(input.length), seed)
	return int(res)
}

func newPgQueryError(mod api.Module, errPtr uint32) error {
	message := readCStringPtr(mod.Memory(), errPtr)
	funcname := readCStringPtr(mod.Memory(), errPtr+4)
	filename := readCStringPtr(mod.Memory(), errPtr+8)
	lineno, ok := mod.Memory().ReadUint32Le(errPtr + 12)
	if !ok {
		panic(errFailedRead)
	}
	cursorpos, ok := mod.Memory().ReadUint32Le(errPtr + 16)
	if !ok {
		panic(errFailedRead)
	}
	context := readCStringPtr(mod.Memory(), errPtr+20)

	return &Error{
		Message:   message,
		Funcname:  funcname,
		Filename:  filename,
		Lineno:    int(lineno),
		Cursorpos: int(cursorpos),
		Context:   context,
	}
}

func readCStringPtr(mem api.Memory, ptrptr uint32) string {
	ptr, ok := mem.ReadUint32Le(ptrptr)
	if !ok {
		panic(errFailedRead)
	}
	s := ""
	if ptr == 0 {
		return s
	}
	endPtr := ptr
	for {
		if b, ok := mem.ReadByte(endPtr); !ok {
			panic(errFailedRead)
		} else if b == 0 {
			break
		}
		endPtr++
	}
	buf, ok := mem.Read(ptr, endPtr-ptr)
	if !ok {
		panic(errFailedRead)
	}
	return string(buf)
}

type lazyFunction struct {
	f    api.Function
	rt   wazero.Runtime
	name string
	mod  api.Module
}

func newLazyFunction(rt wazero.Runtime, mod api.Module, name string) lazyFunction {
	return lazyFunction{rt: rt, mod: mod, name: name}
}

func (f *lazyFunction) Call0(ctx context.Context) uint64 {
	var callStack [1]uint64
	return f.callWithStack(ctx, callStack[:])
}

func (f *lazyFunction) Call1(ctx context.Context, arg1 uint64) uint64 {
	var callStack [1]uint64
	callStack[0] = arg1
	return f.callWithStack(ctx, callStack[:])
}

func (f *lazyFunction) Call2(ctx context.Context, arg1 uint64, arg2 uint64) uint64 {
	var callStack [2]uint64
	callStack[0] = arg1
	callStack[1] = arg2
	return f.callWithStack(ctx, callStack[:])
}

func (f *lazyFunction) Call3(ctx context.Context, arg1 uint64, arg2 uint64, arg3 uint64) uint64 {
	var callStack [3]uint64
	callStack[0] = arg1
	callStack[1] = arg2
	callStack[2] = arg3
	return f.callWithStack(ctx, callStack[:])
}

func (f *lazyFunction) Call8(ctx context.Context, arg1 uint64, arg2 uint64, arg3 uint64, arg4 uint64, arg5 uint64, arg6 uint64, arg7 uint64, arg8 uint64) uint64 {
	var callStack [8]uint64
	callStack[0] = arg1
	callStack[1] = arg2
	callStack[2] = arg3
	callStack[3] = arg4
	callStack[4] = arg5
	callStack[5] = arg6
	callStack[6] = arg7
	callStack[7] = arg8
	return f.callWithStack(ctx, callStack[:])
}

func (f *lazyFunction) callWithStack(ctx context.Context, callStack []uint64) uint64 {
	if f.f == nil {
		f.f = f.mod.ExportedFunction(f.name)
	}
	if err := f.f.CallWithStack(ctx, callStack); err != nil {
		panic(err)
	}
	return callStack[0]
}

type cString struct {
	ptr    uint32
	length int
	abi    *abi
}

func (abi *abi) newCString(s string) cString {
	ptr := uint32(abi.malloc.Call1(context.Background(), uint64(len(s)+1)))
	if !abi.wasmMemory.WriteString(ptr, s) {
		panic(errFailedWrite)
	}
	if !abi.wasmMemory.WriteByte(ptr+uint32(len(s)), 0) {
		panic(errFailedWrite)
	}
	return cString{
		ptr:    ptr,
		length: len(s),
		abi:    abi,
	}
}

func (abi *abi) newCStringFromBytes(s []byte) cString {
	ptr := uint32(abi.malloc.Call1(context.Background(), uint64(len(s)+1)))
	if !abi.wasmMemory.Write(ptr, s) {
		panic(errFailedWrite)
	}
	if !abi.wasmMemory.WriteByte(ptr+uint32(len(s)), 0) {
		panic(errFailedWrite)
	}
	return cString{
		ptr:    ptr,
		length: len(s),
		abi:    abi,
	}
}

func (s cString) Close() error {
	s.abi.free.Call1(context.Background(), uint64(s.ptr))
	return nil
}
