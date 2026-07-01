package pebble

type ExpandWritePathStats struct {
	ExpandedCalls    int64
	ExpandedRows     int64
	SynthesizedCalls int64
	SynthesizedRows  int64
}

func (e *Engine) ExpandWritePathStats() ExpandWritePathStats {
	return ExpandWritePathStats{
		ExpandedCalls:    e.expandedWriteCalls.Load(),
		ExpandedRows:     e.expandedWriteRows.Load(),
		SynthesizedCalls: e.synthesizedWriteCalls.Load(),
		SynthesizedRows:  e.synthesizedWriteRows.Load(),
	}
}
