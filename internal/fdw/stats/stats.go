package stats

import (
	"sync"

	"github.com/bmizerany/perks/quantile"

	"github.com/authzed/spicedb/internal/fdw/explain"
)

// Package collects query execution statistics for a specific table and operation.
// It uses quantile streams to track cost, row count, and result width metrics.
type Package struct {
	sync.Mutex

	table     string
	operation string

	cost  *quantile.Stream // GUARDED_BY(Mutex)
	rows  *quantile.Stream // GUARDED_BY(Mutex)
	width *quantile.Stream // GUARDED_BY(Mutex)
}

const median = 0.5

// NewPackage creates a new statistics package for a table and operation.
func NewPackage(table, operation string) *Package {
	return &Package{
		table:     table,
		operation: operation,
		cost:      quantile.NewTargeted(0.5),
		rows:      quantile.NewTargeted(0.5),
		width:     quantile.NewTargeted(0.5),
	}
}

// AddWidthSample records a result width sample.
func (p *Package) AddWidthSample(width uint) {
	p.Lock()
	defer p.Unlock()
	p.width.Insert(float64(width))
}

// AddSample records a cost and row count sample from a query execution.
func (p *Package) AddSample(cost float32, rows uint) {
	p.Lock()
	defer p.Unlock()
	p.cost.Insert(float64(cost))
	p.rows.Insert(float64(rows))
}

// Explain generates an EXPLAIN plan string using collected statistics.
// If no samples have been collected, returns a default plan.
// TODO(jschorr): Support getting cost estimates from Materialize.
func (p *Package) Explain() string {
	p.Lock()
	defer p.Unlock()
	if p.cost.Count() == 0 {
		return explain.Default(p.operation, p.table).String()
	}

	return explain.Explain{
		Operation:                    p.operation,
		TableName:                    p.table,
		StartupCost:                  float32(p.cost.Query(median)),
		TotalCost:                    float32(p.cost.Query(median)),
		EstimatedRows:                int32(p.rows.Query(median)),
		EstimatedAverageWidthInBytes: int32(p.width.Query(median)),
	}.String()
}
