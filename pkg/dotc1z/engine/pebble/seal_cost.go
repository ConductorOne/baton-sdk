package pebble

import "time"

type SealCost struct {
	LedgerDiscard time.Duration
	LedgerArchive time.Duration
	LedgerScrub   time.Duration
	LedgerPurge   time.Duration
}

// LastSealCost reports the last finalize attempt that returned, including
// failures. Test instrumentation: production has no reader.
func (e *Engine) LastSealCost() SealCost {
	cost := e.test.sealCost.Load()
	if cost == nil {
		return SealCost{}
	}
	return *cost
}
