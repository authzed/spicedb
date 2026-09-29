package checkbaseline

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

var benchmarkMu sync.Mutex

// measurePair uses equal iteration counts, alternates engine order and creates
// fresh per-request state. Benchmark harness GC and plan setup are outside timing.
func measurePair(ctx context.Context, engines []Engine, c Case, count int, timeout time.Duration) ([2][]Sample, error) {
	var out [2][]Sample
	if count < 10 || len(engines) != 2 {
		return out, fmt.Errorf("need two engines and at least ten samples")
	}
	benchmarkMu.Lock()
	defer benchmarkMu.Unlock()
	testing.Init()
	old := flag.Lookup("test.benchtime").Value.String()
	defer flag.Set("test.benchtime", old)
	slowest := time.Duration(1)
	for _, engine := range engines {
		start := time.Now()
		qctx, cancel := context.WithTimeout(ctx, timeout)
		d, err := engine.Check(qctx, c)
		cancel()
		if err != nil {
			return out, err
		}
		if d.Outcome != c.Expected.Outcome {
			return out, fmt.Errorf("%s warmup outcome %s", engine.Name(), d.Outcome)
		}
		slowest = max(slowest, time.Since(start))
	}
	n := max(1, min(512, int(5*time.Millisecond/slowest)))
	if err := flag.Set("test.benchtime", fmt.Sprintf("%dx", n)); err != nil {
		return out, err
	}
	for sample := 0; sample < count; sample++ {
		for j := 0; j < 2; j++ {
			index := (j + sample) % 2
			engine := engines[index]
			var failure error
			br := testing.Benchmark(func(b *testing.B) {
				b.StopTimer()
				decisions := make([]Decision, b.N)
				b.ReportAllocs()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					qctx, cancel := context.WithTimeout(ctx, timeout)
					decision, err := engine.Check(qctx, c)
					cancel()
					if err != nil {
						failure = err
						b.StopTimer()
						return
					}
					decisions[i] = decision
				}
				b.StopTimer()
				for _, decision := range decisions {
					if decision.Outcome != c.Expected.Outcome {
						failure = fmt.Errorf("%s changed outcome during timing: expected %s, got %s", engine.Name(), c.Expected.Outcome, decision.Outcome)
						b.StopTimer()
						return
					}
				}
			})
			if failure != nil {
				return out, failure
			}
			ns := float64(br.T.Nanoseconds()) / float64(br.N)
			if math.IsNaN(ns) || ns <= 0 {
				return out, fmt.Errorf("invalid benchmark duration")
			}
			out[index] = append(out[index], Sample{NSPerOp: ns, BytesPerOp: float64(br.MemBytes) / float64(br.N), AllocsPerOp: float64(br.MemAllocs) / float64(br.N), Iterations: br.N})
		}
	}
	return out, nil
}
