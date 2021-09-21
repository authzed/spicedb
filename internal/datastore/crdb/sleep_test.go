package crdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
)

func smartSleepJake(maxOffset time.Duration, now, rev datastore.Revision) (int64, datastore.Revision) {
	evaluationSnapshot := rev.Add(decimal.NewFromInt(maxOffset.Nanoseconds()))
	minNodeTime := evaluationSnapshot.Add(decimal.NewFromInt(maxOffset.Nanoseconds()))
	sleepTime := minNodeTime.Sub(now)

	fmt.Println(evaluationSnapshot.IntPart(), minNodeTime.IntPart(), sleepTime.IntPart())
	if sleepTime.LessThan(decimal.Zero) {
		sleepTime = decimal.Zero
	}

	return sleepTime.IntPart(), evaluationSnapshot
}

func smartSleepEvan(maxOffset time.Duration, now, rev datastore.Revision) (int64, datastore.Revision) {
	evaluationSnapshot := rev.Add(decimal.NewFromInt(maxOffset.Nanoseconds()))
	diff := now.Sub(rev).Abs().IntPart()
	if diff > maxOffset.Nanoseconds() {
		return 0, evaluationSnapshot
	}
	return 2*maxOffset.Nanoseconds() - diff, evaluationSnapshot
}

func TestCompareSmartSleep(t *testing.T) {
	type test struct {
		name      string
		maxOffset time.Duration
		now       datastore.Revision
		rev       datastore.Revision
	}

	tests := []test{
		{
			name:      "rev from slow node",
			maxOffset: 500 * time.Millisecond,
			now:       decimal.NewFromInt(int64(time.Now().Nanosecond())),
			rev:       decimal.NewFromInt(int64(time.Now().Nanosecond()) - (200 * time.Millisecond).Nanoseconds()),
		},
		{
			name:      "now behind rev",
			maxOffset: 500 * time.Millisecond,
			now:       decimal.NewFromInt(int64(time.Now().Nanosecond()) - (200 * time.Millisecond).Nanoseconds()),
			rev:       decimal.NewFromInt(int64(time.Now().Nanosecond())),
		},
		{
			name:      "after maxoffset",
			maxOffset: 500 * time.Millisecond,
			now:       decimal.NewFromInt(int64(time.Now().Nanosecond())),
			rev:       decimal.NewFromInt(int64(time.Now().Nanosecond()) - (600 * time.Millisecond).Nanoseconds()),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name+"-evan", func(t *testing.T) {
			fmt.Println(smartSleepEvan(tc.maxOffset, tc.now, tc.rev))
		})
		t.Run(tc.name+"-jake", func(t *testing.T) {
			fmt.Println(smartSleepJake(tc.maxOffset, tc.now, tc.rev))
		})
	}
}
