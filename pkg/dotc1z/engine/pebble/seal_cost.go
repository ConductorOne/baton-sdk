package pebble

import "time"

type SealCost struct {
	LedgerScrub time.Duration
	LedgerPurge time.Duration
}

// LastSealCost reports the last finalize attempt that returned, including failures.
func (e *Engine) LastSealCost() SealCost {
	cost := e.sealCost.Load()
	if cost == nil {
		return SealCost{}
	}
	return *cost
}
