// Harness probe: the engine's own raft projection derivation run
// through OUR pipeline. If this fails while the engine's in-repo axiom
// test passes, the pipeline here is missing a step; if it passes, a
// failing sync_protocol projection is a module problem, not a harness
// problem. Kept permanently as the harness's positive control.
package host_test

import "testing"

func TestRaftProbe(t *testing.T) {
	lhs := `exists P; P = require("projection"); exists R; R = require("raft"); P.P_leader(R.election)`
	rhs := `exists P; P = require("projection"); exists R; R = require("raft"); P.io_recv(R.request_vote) pipe P.local(R.tally_votes) pipe P.io_send(R.announce_leader)`
	if !equivalent(t, []string{"raft"}, nil, lhs, rhs) {
		t.Fatalf("raft leader projection did not derive through this pipeline — harness gap")
	}
}
