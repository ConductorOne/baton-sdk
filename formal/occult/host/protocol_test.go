// Deliverable 8: projection derivations for the syncer↔connector
// source-cache protocol (../src/sync_protocol.occult) — the engine's
// Raft leader/follower derivation-test pattern applied to our contract.
// SYNCER = P_leader, CONNECTOR = P_follower. Positives prove each
// role's projected IO chain; negatives pin the polarity (a projection
// that confused the roles or erased a bounce must be refused).
//
// Stuck-freedom and bounce-cap VIOLATION checking are open engine work
// (session-types TODO); the cap is structural in the protocol module
// (no five-bounce session term exists) and its Go-side mirror is
// sourcecache.MaxLookupBouncesPerRequest.
package host_test

import "testing"

var protocolRequires = []string{"protocol", "projection"}

var protocolModules = []localModule{
	{"sync_protocol", "sync_protocol.occult"},
}

const protoPrelude = `exists P; P = require("projection"); exists S; S = require("sync_protocol"); `

func protoEquivalent(t *testing.T, lhs, rhs string) bool {
	t.Helper()
	return equivalent(t, protocolRequires, protocolModules, protoPrelude+lhs, protoPrelude+rhs)
}

var protocolCases = []lawCase{
	// Structural expansion of the one-bounce session.
	{"expand_ask1",
		`S.session_ask1`,
		`S.ask_first pipe S.send(S.req_answers) pipe S.recv(S.page_record)`},
	// Direct (no-ask) page service, both roles.
	{"syncer_direct",
		`P.P_leader(S.session_direct)`,
		`P.io_send(S.req_offer) pipe P.io_recv(S.page_record)`},
	{"connector_direct",
		`P.P_follower(S.session_direct)`,
		`P.io_recv(S.req_offer) pipe P.io_send(S.page_record)`},
	{"connector_direct_replay",
		`P.P_follower(S.session_direct_replay)`,
		`P.io_recv(S.req_offer) pipe P.io_send(S.page_replay)`},
	// One ask bounce, both roles.
	{"syncer_ask1",
		`P.P_leader(S.session_ask1)`,
		`P.io_send(S.req_offer) pipe P.io_recv(S.lookup_ask) pipe P.local(S.resolve_answers) pipe P.io_send(S.req_answers) pipe P.io_recv(S.page_record)`},
	{"connector_ask1",
		`P.P_follower(S.session_ask1)`,
		`P.io_recv(S.req_offer) pipe P.io_send(S.lookup_ask) pipe P.local(S.resolve_answers) pipe P.io_recv(S.req_answers) pipe P.io_send(S.page_record)`},
	// The maximal legal session (bounce cap 4), syncer side. pipe has no
	// associativity axiom (composition is a raw binary tree), so the
	// expected term parenthesizes each bounce to mirror the session's
	// grouping exactly.
	{"syncer_ask4",
		`P.P_leader(S.session_ask4)`,
		`(P.io_send(S.req_offer) pipe P.io_recv(S.lookup_ask) pipe P.local(S.resolve_answers)) pipe (P.io_send(S.req_answers) pipe P.io_recv(S.lookup_ask) pipe P.local(S.resolve_answers)) pipe (P.io_send(S.req_answers) pipe P.io_recv(S.lookup_ask) pipe P.local(S.resolve_answers)) pipe (P.io_send(S.req_answers) pipe P.io_recv(S.lookup_ask) pipe P.local(S.resolve_answers)) pipe P.io_send(S.req_answers) pipe P.io_recv(S.page_record)`},
}

var protocolControls = []lawCase{
	// Role polarity: the two projections of one session must differ.
	{"roles_differ", `P.P_leader(S.session_direct)`, `P.P_follower(S.session_direct)`},
	// A bounce is not erasable.
	{"bounce_not_erasable", `S.session_ask1`, `S.session_direct`},
	// Cap shape: four bounces are not one bounce.
	{"cap_shape", `P.P_leader(S.session_ask4)`, `P.P_leader(S.session_ask1)`},
	// Annotation exchange: record and replay legs differ.
	{"record_not_replay", `S.session_direct`, `S.session_direct_replay`},
}

func TestProtocolProjections(t *testing.T) {
	for _, c := range protocolCases {
		t.Run(c.name, func(t *testing.T) {
			if !protoEquivalent(t, c.lhs, c.rhs) {
				t.Errorf("projection %s: expected %q equivalent to %q", c.name, c.lhs, c.rhs)
			}
		})
	}
}

func TestProtocolControls(t *testing.T) {
	for _, c := range protocolControls {
		t.Run(c.name, func(t *testing.T) {
			if protoEquivalent(t, c.lhs, c.rhs) {
				t.Errorf("control %s: %q must NOT be equivalent to %q", c.name, c.lhs, c.rhs)
			}
		})
	}
}
