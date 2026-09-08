package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// syncTestHooks is the syncer's set of test seams: observation and
// fault-injection points the verification harnesses attach to in order to
// make an ordering-sensitive boundary reachable from a test. Every field is
// nil in production, so a seam costs one pointer comparison on the path it
// guards — the same cost these had as individual syncer fields.
//
// They live together so a reader can find every place a test can perturb a
// sync in one file, and so that adding a seam does not widen syncer. No
// production code path may set one of these; the only writers are in
// _test.go files, which is what the type name asserts. The fields therefore
// carry no `test` prefix of their own — any `test`-prefixed field elsewhere
// in the package is a seam that escaped this struct.
type syncTestHooks struct {
	// ingestHaltHook, when non-nil, fires at named seams of the
	// ingestion-invariant pass (see ingestInvariantHaltStages);
	// returning an error fails the sync at exactly that boundary. The
	// halt sweep uses it to prove crash/resume equivalence at every
	// ordering-sensitive point.
	ingestHaltHook func(stage string) error
	// checkpointHook, when non-nil, observes every durably written
	// checkpoint token. The cut harness uses it to count checkpoints
	// and to simulate a crash immediately after a chosen one.
	checkpointHook func(token string)
	// queueAudit, when non-nil, records every parallelActionQueue
	// event (seed/dequeue/commit/abort/done) for post-hoc verification
	// of the queue contract.
	queueAudit *queueAudit
}
