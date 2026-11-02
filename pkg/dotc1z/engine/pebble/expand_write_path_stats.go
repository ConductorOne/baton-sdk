package pebble

type Expansion struct {
	e *Engine
}

func (e *Engine) Expansion() *Expansion { return &e.expansion }

type ExpandWritePathStats struct {
	ExpandedCalls    int64
	ExpandedRows     int64
	SynthesizedCalls int64
	SynthesizedRows  int64
}

func (x *Expansion) ExpandWritePathStats() ExpandWritePathStats {
	return ExpandWritePathStats{
		ExpandedCalls:    x.e.expandedWriteCalls.Load(),
		ExpandedRows:     x.e.expandedWriteRows.Load(),
		SynthesizedCalls: x.e.synthesizedWriteCalls.Load(),
		SynthesizedRows:  x.e.synthesizedWriteRows.Load(),
	}
}
