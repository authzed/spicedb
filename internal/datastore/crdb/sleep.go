package crdb

import (
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
)

func (cds *crdbDatastore) smartSleep(now, rev datastore.Revision) datastore.Revision {
	evaluationSnapshot := rev.Add(decimal.NewFromInt(cds.maxOffset.Nanoseconds()))
	minNodeTime := evaluationSnapshot.Add(decimal.NewFromInt(cds.maxOffset.Nanoseconds()))
	sleepTime := minNodeTime.Sub(now)

	if sleepTime.GreaterThan(decimal.Zero) {
		log.Debug().Stringer("sleep", sleepTime).Msg("sleeping until time")
		time.Sleep(time.Duration(sleepTime.IntPart()))
	}

	return evaluationSnapshot
}

func (cds *crdbDatastore) smartSleepSymmetric(now, rev datastore.Revision) datastore.Revision {
	log.Debug().Msgf("checking if we need to sleep")
	// nothing to do if the revision is older than MaxOffset
	diff := now.Sub(rev).Abs().IntPart()
	if diff > cds.maxOffset.Nanoseconds() {
		log.Debug().Msgf("don't need to sleep. now: %d, rev: %d", now.IntPart(), rev.IntPart())
		return rev.Add(decimal.NewFromInt(cds.maxOffset.Nanoseconds()))
	}
	sleep := 2*cds.maxOffset.Nanoseconds() - diff
	log.Debug().Msgf("sleeping for %d ns", sleep)
	time.Sleep(time.Duration(sleep))
	return rev.Add(decimal.NewFromInt(cds.maxOffset.Nanoseconds()))
}
