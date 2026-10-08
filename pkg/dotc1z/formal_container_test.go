// TestFormalContainer replays the container family of the cases that the
// Lean model in formal/c1z emits (formal/c1z/ORACLE_SCHEMA.md,
// "container") through the public store API: NewStore on a temp path,
// Close, and NewStore again on the same path. The other families replay
// in engine/pebble/formal_conformance_test.go, which cannot reach this
// package because this package imports the engine package; that file
// decodes the container family only for the counts check. This file is
// in package dotc1z for unpackExistingPebbleC1Z, which the bad_engine
// damage needs, and copies the few helpers it shares with that file.
package dotc1z

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

const fcOracleVersion = 1

// fcHex decodes a lowercase-hex JSON string. present distinguishes an
// absent field from an empty byte string.
type fcHex struct {
	b       string
	present bool
}

func (h *fcHex) UnmarshalJSON(data []byte) error {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == nil {
		return errors.New("hex field is null")
	}
	b, err := hex.DecodeString(*s)
	if err != nil {
		return err
	}
	h.b = string(b)
	h.present = true
	return nil
}

func (h fcHex) MarshalJSON() ([]byte, error) {
	if !h.present {
		return []byte("null"), nil
	}
	return json.Marshal(hex.EncodeToString([]byte(h.b)))
}

type fcEntRef struct {
	RT  fcHex `json:"rt"`
	RID fcHex `json:"rid"`
	Ext fcHex `json:"ext"`
}

type fcGrant struct {
	Ent   *fcEntRef `json:"ent"`
	PRT   fcHex     `json:"prt"`
	PRID  fcHex     `json:"prid"`
	ExtID fcHex     `json:"ext_id"`
}

// fcOp is one container op. Result is the expected result of every op
// that has one; Grants is the expected collection of an ok read.
type fcOp struct {
	Op       string     `json:"op"`
	ID       *string    `json:"id"`
	Type     *string    `json:"type"`
	Result   *string    `json:"result"`
	Batch    *[]fcGrant `json:"batch"`
	Days     *int       `json:"days"`
	ReadOnly *bool      `json:"readonly"`
	Damage   *string    `json:"damage"`
	PRT      fcHex      `json:"prt"`
	PRID     fcHex      `json:"prid"`
	Grants   *[]fcGrant `json:"grants"`
}

type fcCase struct {
	Name string `json:"name"`
	Ops  []fcOp `json:"ops"`
}

// fcCases is the oracle document. Every family but container is kept raw:
// engine/pebble/formal_conformance_test.go replays them.
type fcCases struct {
	Version   *int           `json:"version"`
	Counts    map[string]int `json:"counts"`
	Container []fcCase       `json:"container"`

	Keys              json.RawMessage `json:"keys"`
	EntitlementStrip  json.RawMessage `json:"entitlement_strip"`
	Writes            json.RawMessage `json:"writes"`
	Pagination        json.RawMessage `json:"pagination"`
	BareID            json.RawMessage `json:"bare_id"`
	Sync              json.RawMessage `json:"sync"`
	GrantWrites       json.RawMessage `json:"grant_writes"`
	EntitlementWrites json.RawMessage `json:"entitlement_writes"`
	GrantList         json.RawMessage `json:"grant_list"`
	GrantsByPrincipal json.RawMessage `json:"grants_by_principal"`
	GrantBareID       json.RawMessage `json:"grant_bare_id"`
	Stream            json.RawMessage `json:"stream"`
	Digest            json.RawMessage `json:"digest"`
	Reopen            json.RawMessage `json:"reopen"`
	Views             json.RawMessage `json:"views"`
}

func decodeFCCases(r io.Reader) (*fcCases, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var cases fcCases
	if err := dec.Decode(&cases); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode: trailing data after the JSON document")
	}
	if cases.Version == nil || *cases.Version != fcOracleVersion {
		return nil, fmt.Errorf("oracle version = %v, want %d", cases.Version, fcOracleVersion)
	}
	n, ok := cases.Counts["container"]
	if !ok {
		return nil, errors.New("counts is missing family \"container\"")
	}
	if n != len(cases.Container) {
		return nil, fmt.Errorf("counts[\"container\"] = %d, but the array has %d cases", n, len(cases.Container))
	}
	return &cases, nil
}

func fcCasesPath(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("C1Z_FORMAL_CASES"); path != "" {
		return path
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate formal/c1z/generated/cases.json")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "formal", "c1z", "generated", "cases.json")
}

func TestFormalContainer(t *testing.T) {
	path := fcCasesPath(t)
	t.Logf("reading oracle cases from %s", path)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v (regenerate with `make formal-c1z-oracle`)", path, err)
	}
	defer f.Close()
	cases, err := decodeFCCases(f)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if len(cases.Container) == 0 {
		t.Fatal("family \"container\" is empty")
	}
	replayFCCases(t, cases.Container, nil)
}

// replayFCCases runs every case as a parallel subtest; each case has its
// own temp dir and store.
func replayFCCases(t *testing.T, cases []fcCase, onFail func(t *testing.T, i int)) {
	t.Helper()
	seen := map[string]bool{}
	for _, c := range cases {
		if c.Name == "" || seen[c.Name] {
			t.Fatalf("container: empty or duplicate case name %q", c.Name)
		}
		seen[c.Name] = true
	}
	t.Run("container", func(t *testing.T) {
		for i, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				t.Parallel()
				if onFail != nil {
					t.Cleanup(func() {
						if t.Failed() {
							onFail(t, i)
						}
					})
				}
				runFCCase(t, c)
			})
		}
	})
}

func fcRequireHex(t *testing.T, field string, h fcHex) string {
	t.Helper()
	if !h.present {
		t.Fatalf("required hex field %q is absent", field)
	}
	if !utf8.ValidString(h.b) {
		t.Fatalf("field %q is not valid UTF-8 (%x); proto3 string fields cannot carry it", field, h.b)
	}
	return h.b
}

// fcGrantParts is a decoded fcGrant: the five identity components and the
// stored external_id.
type fcGrantParts struct {
	rt, rid, ext, prt, prid, extID string
}

func (p fcGrantParts) String() string {
	return fmt.Sprintf("(%x,%x,%x)/%x/%x#%x", p.rt, p.rid, p.ext, p.prt, p.prid, p.extID)
}

func (p fcGrantParts) identity() [5]string { return [5]string{p.rt, p.rid, p.ext, p.prt, p.prid} }

func fcRequireGrants(t *testing.T, field string, gs []fcGrant) []fcGrantParts {
	t.Helper()
	out := make([]fcGrantParts, 0, len(gs))
	for i, g := range gs {
		f := fmt.Sprintf("%s[%d]", field, i)
		if g.Ent == nil {
			t.Fatalf("required object %q is absent", f+".ent")
		}
		out = append(out, fcGrantParts{
			rt:    fcRequireHex(t, f+".ent.rt", g.Ent.RT),
			rid:   fcRequireHex(t, f+".ent.rid", g.Ent.RID),
			ext:   fcRequireHex(t, f+".ent.ext", g.Ent.Ext),
			prt:   fcRequireHex(t, f+".prt", g.PRT),
			prid:  fcRequireHex(t, f+".prid", g.PRID),
			extID: fcRequireHex(t, f+".ext_id", g.ExtID),
		})
	}
	return out
}

func fcV2Grants(ps []fcGrantParts) []*v2.Grant {
	out := make([]*v2.Grant, 0, len(ps))
	for _, p := range ps {
		out = append(out, v2.Grant_builder{
			Id: p.extID,
			Entitlement: v2.Entitlement_builder{
				Id:       p.ext,
				Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: p.rt, Resource: p.rid}.Build()}.Build(),
			}.Build(),
			Principal: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: p.prt, Resource: p.prid}.Build()}.Build(),
		}.Build())
	}
	return out
}

// fcStoredExtIDs maps every stored grant identity to its stored
// external_id. v2 Grant.Id is the rebuilt public id when the stored one
// is empty, so the returned grants cannot say which it was.
func fcStoredExtIDs(ctx context.Context, t *testing.T, s *pebbleStore) map[[5]string]string {
	t.Helper()
	out := map[[5]string]string{}
	err := s.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		ent, pr := r.GetEntitlement(), r.GetPrincipal()
		out[[5]string{ent.GetResourceTypeId(), ent.GetResourceId(), ent.GetEntitlementId(), pr.GetResourceTypeId(), pr.GetResourceId()}] = r.GetExternalId()
		return true
	})
	if err != nil {
		t.Fatalf("IterateGrants: %v", err)
	}
	return out
}

// fcGotGrants reads each returned grant's identity through its refs and
// its stored external_id from extIDs, and checks the v2 Grant.Id rule
// from ORACLE_SCHEMA.md.
func fcGotGrants(t *testing.T, extIDs map[[5]string]string, gs []*v2.Grant) []fcGrantParts {
	t.Helper()
	out := make([]fcGrantParts, 0, len(gs))
	for _, g := range gs {
		ent := g.GetEntitlement()
		p := fcGrantParts{
			rt: ent.GetResource().GetId().GetResourceType(), rid: ent.GetResource().GetId().GetResource(), ext: ent.GetId(),
			prt: g.GetPrincipal().GetId().GetResourceType(), prid: g.GetPrincipal().GetId().GetResource(),
		}
		extID, ok := extIDs[p.identity()]
		if !ok {
			t.Fatalf("returned grant %v has no stored record", p)
		}
		p.extID = extID
		wantID := extID
		if wantID == "" {
			wantID = p.ext + ":" + p.prt + ":" + p.prid
		}
		if g.GetId() != wantID {
			t.Fatalf("returned grant %v has v2 Id %x, want %x", p, g.GetId(), wantID)
		}
		out = append(out, p)
	}
	return out
}

func fcRequireGrantList(t *testing.T, what string, got, want []fcGrantParts) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d grants %v, want %d %v", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %v, want %v\n got  %v\n want %v", what, i, got[i], want[i], got, want)
		}
	}
}

// fcSyncIDs maps the oracle's symbolic sync ids to the ids the store
// mints, and back. A symbolic id is rebound by every accepted start_new.
type fcSyncIDs struct {
	toEngine   map[string]string
	fromEngine map[string]string
}

func (m *fcSyncIDs) engineID(symbolic string) string {
	if id, ok := m.toEngine[symbolic]; ok {
		return id
	}
	return ksuid.New().String()
}

func (m *fcSyncIDs) bind(symbolic, id string) {
	m.toEngine[symbolic] = id
	m.fromEngine[id] = symbolic
}

func fcSyncType(t *testing.T, s string, allowAny bool) connectorstore.SyncType {
	t.Helper()
	switch s {
	case "full":
		return connectorstore.SyncTypeFull
	case "partial":
		return connectorstore.SyncTypePartial
	case "resources_only":
		return connectorstore.SyncTypeResourcesOnly
	case "any":
		if allowAny {
			return connectorstore.SyncTypeAny
		}
	}
	t.Fatalf("unknown sync type %q", s)
	return ""
}

// fcReadOnlyRefusal reports whether err is the read-only engine's write
// refusal. checkWritableAllowSealedLocked returns a plain errors.New with
// no sentinel, so this matches its message.
func fcReadOnlyRefusal(err error) bool {
	return err != nil && strings.Contains(err.Error(), "pebble engine: opened read-only")
}

// fcHandle is the store a case drives; save_reopen replaces s. tmpDir is
// the case directory under C1Z_FORMAL_TMPDIR, or empty when it is unset.
type fcHandle struct {
	path   string
	tmpDir string
	s      *pebbleStore
}

// fcCaseDir returns a fresh directory for one case's .c1z file and
// extraction directories, and whether it is under C1Z_FORMAL_TMPDIR.
func fcCaseDir(t *testing.T) (string, bool) {
	t.Helper()
	root := os.Getenv("C1Z_FORMAL_TMPDIR")
	if root == "" {
		return t.TempDir(), false
	}
	dir, err := os.MkdirTemp(root, "formal-")
	if err != nil {
		t.Fatalf("C1Z_FORMAL_TMPDIR: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil { //nolint:gosec // dir comes from C1Z_FORMAL_TMPDIR, set by the developer.
			t.Errorf("remove %s: %v", dir, err)
		}
	})
	return dir, true
}

func (h *fcHandle) open(ctx context.Context, t *testing.T, readOnly bool) error {
	t.Helper()
	tmpDir := h.tmpDir
	if tmpDir == "" {
		tmpDir = t.TempDir()
	}
	st, err := NewStore(ctx, h.path, WithReadOnly(readOnly), WithTmpDir(tmpDir))
	if err != nil {
		h.s = nil
		return err
	}
	ps, ok := st.(*pebbleStore)
	if !ok {
		_ = st.Close(ctx)
		t.Fatalf("NewStore(%s) returned %T, want *pebbleStore", h.path, st)
	}
	h.s = ps
	return nil
}

// fcDamage mutates the saved file at path as ORACLE_SCHEMA.md "container"
// describes for damage.
func fcDamage(t *testing.T, path, damage string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("damage %s: read %s (was a file saved?): %v", damage, path, err)
	}
	const headerLen = 9 // 5-byte C1Z3 magic + big-endian u32 manifest length
	if len(raw) < headerLen {
		t.Fatalf("damage %s: file has %d bytes, want at least %d", damage, len(raw), headerLen)
	}
	write := func(b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0o600); err != nil { //nolint:gosec // path is the case's own file under fcCaseDir.
			t.Fatalf("damage %s: write: %v", damage, err)
		}
	}
	switch damage {
	case "truncate_header":
		if err := os.Truncate(path, 4); err != nil {
			t.Fatalf("damage %s: %v", damage, err)
		}
	case "bad_magic":
		raw[0] ^= 0xff
		write(raw)
	case "flip_payload_byte":
		off := headerLen + int(binary.BigEndian.Uint32(raw[5:headerLen])) + 100
		if len(raw) <= off {
			t.Fatalf("damage %s: file has %d bytes, want more than %d", damage, len(raw), off)
		}
		raw[off] ^= 0xff
		write(raw)
	case "truncate_tail":
		// The indexed footer is the last 28 bytes.
		if len(raw) < headerLen+28 {
			t.Fatalf("damage %s: file has %d bytes, too short for a footer", damage, len(raw))
		}
		if err := os.Truncate(path, int64(len(raw)-28)); err != nil {
			t.Fatalf("damage %s: %v", damage, err)
		}
	case "bad_engine":
		dbDir, err := os.MkdirTemp(filepath.Dir(path), "damage-")
		if err != nil {
			t.Fatalf("damage %s: %v", damage, err)
		}
		if _, _, _, err := unpackExistingPebbleC1Z(path, dbDir, 0, 0, nil); err != nil {
			t.Fatalf("damage %s: unpack: %v", damage, err)
		}
		var buf bytes.Buffer
		manifest := c1zv3.C1ZManifestV3_builder{Engine: "bogus"}.Build()
		if err := formatv3.WriteEnvelope(&buf, manifest, dbDir); err != nil {
			t.Fatalf("damage %s: WriteEnvelope: %v", damage, err)
		}
		write(buf.Bytes())
	default:
		t.Fatalf("unknown damage %q", damage)
	}
}

// fcListResolving pages list to exhaustion. It returns "no_current_sync"
// when a page fails with ErrNoCurrentSync and fails the test on any
// other error.
func fcListResolving(t *testing.T, what string, list func(token string) ([]*v2.Grant, string, error)) ([]*v2.Grant, string) {
	t.Helper()
	var out []*v2.Grant
	token := ""
	for {
		gs, next, err := list(token)
		if errors.Is(err, pebble.ErrNoCurrentSync) {
			return nil, "no_current_sync"
		}
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		out = append(out, gs...)
		token = next
		if token == "" {
			return out, "ok"
		}
	}
}

// fcPageSize is small so a read crosses page boundaries.
const fcPageSize = 2

func runFCCase(t *testing.T, c fcCase) {
	if len(c.Ops) == 0 {
		t.Fatal("container case with no ops")
	}
	ctx := context.Background()
	dir, under := fcCaseDir(t)
	h := &fcHandle{path: filepath.Join(dir, "formal.c1z")}
	if under {
		h.tmpDir = dir
	}
	if err := h.open(ctx, t, false); err != nil {
		t.Fatalf("NewStore on a new path: %v", err)
	}
	t.Cleanup(func() {
		if h.s != nil {
			if err := h.s.Close(ctx); err != nil {
				t.Errorf("store Close: %v", err)
			}
		}
	})
	ids := &fcSyncIDs{toEngine: map[string]string{}, fromEngine: map[string]string{}}
	// recordID is the id of the last accepted start_new: the id the file's
	// single sync-run record carries.
	recordID := ""
	for i, op := range c.Ops {
		s := h.s
		field := fmt.Sprintf("ops[%d]", i)
		what := fmt.Sprintf("op %d (%s)", i, op.Op)
		present := map[string]bool{
			"id": op.ID != nil, "type": op.Type != nil, "result": op.Result != nil, "batch": op.Batch != nil,
			"days": op.Days != nil, "readonly": op.ReadOnly != nil, "prt": op.PRT.present, "prid": op.PRID.present,
			"grants": op.Grants != nil,
		}
		allow := func(fields ...string) {
			t.Helper()
			for _, f := range fields {
				delete(present, f)
			}
			for f, p := range present {
				if p {
					t.Fatalf("%s: field %q must be absent", what, f)
				}
			}
		}
		if op.Damage != nil && op.Op != "save_reopen" {
			t.Fatalf("%s: field \"damage\" must be absent", what)
		}
		want := ""
		requireResult := func(allowed ...string) {
			t.Helper()
			if op.Result == nil {
				t.Fatalf("%s: required field \"result\" is absent", what)
			}
			for _, a := range allowed {
				if *op.Result == a {
					want = a
					return
				}
			}
			t.Fatalf("%s: unknown result %q", what, *op.Result)
		}
		requireID := func() string {
			t.Helper()
			if op.ID == nil {
				t.Fatalf("%s: required field \"id\" is absent", what)
			}
			return *op.ID
		}
		requireType := func() string {
			t.Helper()
			if op.Type == nil {
				t.Fatalf("%s: required field \"type\" is absent", what)
			}
			return *op.Type
		}
		got := ""
		switch op.Op {
		case "start_new":
			allow("id", "type", "result")
			requireResult("ok", "sync_in_progress")
			id, st := requireID(), fcSyncType(t, requireType(), false)
			// ResetForNewSync returns an unwrapped error while a sync
			// started by StartNewSync is open; the flag is read before
			// the call.
			fresh := s.IsFreshSync()
			engineID, err := s.StartNewSync(ctx, st, "")
			switch {
			case err == nil:
				ids.bind(id, engineID)
				recordID = engineID
				got = "ok"
			case fresh:
				got = "sync_in_progress"
			default:
				t.Fatalf("%s: StartNewSync: %v", what, err)
			}
		case "write":
			allow("result")
			requireResult("allowed", "no_current_sync", "engine_sealed", "read_only")
			err := s.PutResources(ctx, v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
				DisplayName: "w",
			}.Build())
			switch {
			case err == nil:
				got = "allowed"
			case errors.Is(err, pebble.ErrEngineSealed):
				got = "engine_sealed"
			case errors.Is(err, pebble.ErrNoCurrentSync):
				got = "no_current_sync"
			case fcReadOnlyRefusal(err):
				got = "read_only"
			default:
				t.Fatalf("%s: PutResources: unexpected error: %v", what, err)
			}
		case "end":
			allow("result")
			requireResult("ok", "no_current_sync")
			// EndSync returns an unwrapped error, not ErrNoCurrentSync,
			// when no sync is bound; the binding is read before the call.
			unbound := s.CurrentSyncID() == ""
			err := s.EndSync(ctx)
			switch {
			case err == nil:
				got = "ok"
			case unbound || errors.Is(err, pebble.ErrNoCurrentSync):
				got = "no_current_sync"
			default:
				t.Fatalf("%s: EndSync: unexpected error: %v", what, err)
			}
		case "resume":
			allow("id", "result")
			requireResult("ok", "not_found")
			id := requireID()
			_, err := s.ResumeSync(ctx, connectorstore.SyncTypeAny, ids.engineID(id))
			switch {
			case err == nil:
				got = "ok"
			case errors.Is(err, cpebble.ErrNotFound):
				got = "not_found"
			default:
				t.Fatalf("%s: ResumeSync(%q): unexpected error: %v", what, id, err)
			}
		case "latest_finished":
			allow("type", "result")
			if op.Result == nil {
				t.Fatalf("%s: required field \"result\" is absent", what)
			}
			want = *op.Result
			id, err := s.LatestFinishedSyncID(ctx, fcSyncType(t, requireType(), true))
			if err != nil {
				t.Fatalf("%s: LatestFinishedSyncID: %v", what, err)
			}
			symbolic, known := ids.fromEngine[id]
			switch {
			case id == "":
				got = "none"
			case known:
				got = symbolic
			default:
				t.Fatalf("%s: LatestFinishedSyncID returned %q, which no start_new named", what, id)
			}
		case "put", "put_deferred":
			allow("batch")
			if op.Batch == nil {
				t.Fatalf("%s: required field \"batch\" is absent", what)
			}
			grants := fcV2Grants(fcRequireGrants(t, field+".batch", *op.Batch))
			if len(grants) == 0 {
				continue
			}
			var err error
			if op.Op == "put" {
				err = s.PutGrants(ctx, grants...)
			} else {
				// The store's deferred write: StoreExpandedGrants marks the
				// store dirty and calls PutExpandedGrantRecords.
				err = s.Grants().StoreExpandedGrants(ctx, grants...)
			}
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			continue
		case "age_sync":
			allow("days")
			if op.Days == nil || *op.Days < 0 {
				t.Fatalf("%s: \"days\" must be present and >= 0", what)
			}
			if recordID == "" {
				t.Fatalf("%s: age_sync before any accepted start_new; there is no sync-run record to age", what)
			}
			rec, err := s.GetSyncRunRecord(ctx, recordID)
			if err != nil {
				t.Fatalf("%s: GetSyncRunRecord(%s): %v", what, recordID, err)
			}
			rec.SetStartedAt(timestamppb.New(time.Now().Add(-time.Duration(*op.Days) * 24 * time.Hour)))
			if err := s.PutSyncRunRecord(ctx, rec); err != nil {
				t.Fatalf("%s: PutSyncRunRecord: %v", what, err)
			}
			// age_sync stands in for wall-clock time passing, which a
			// real store does not write; MarkDirty makes Close save the
			// aged record so the next open sees it.
			s.MarkDirty()
			continue
		case "save_reopen":
			allow("readonly", "result")
			if op.ReadOnly == nil {
				t.Fatalf("%s: required field \"readonly\" is absent", what)
			}
			requireResult("ok", "open_error")
			if err := s.Close(ctx); err != nil {
				t.Fatalf("%s: Close: %v", what, err)
			}
			h.s = nil
			if op.Damage != nil {
				fcDamage(t, h.path, *op.Damage)
			}
			err := h.open(ctx, t, *op.ReadOnly)
			got = "ok"
			if err != nil {
				got = "open_error"
			}
			if got != want {
				t.Fatalf("%s: NewStore(readonly=%v, damage=%v) = %s (err=%v), want %s", what, *op.ReadOnly, fcDamageName(op.Damage), got, err, want)
			}
			if got == "open_error" {
				if i != len(c.Ops)-1 {
					t.Fatalf("%s: open_error is not the last op", what)
				}
				return
			}
			continue
		case "list_grants", "read_by_principal":
			requireResult("ok", "no_current_sync")
			var gs []*v2.Grant
			if op.Op == "list_grants" {
				allow("result", "grants")
				gs, got = fcListResolving(t, what+": ListGrants", func(token string) ([]*v2.Grant, string, error) {
					resp, err := s.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: fcPageSize, PageToken: token}.Build())
					return resp.GetList(), resp.GetNextPageToken(), err
				})
			} else {
				allow("prt", "prid", "result", "grants")
				prt, prid := fcRequireHex(t, field+".prt", op.PRT), fcRequireHex(t, field+".prid", op.PRID)
				gs, got = fcListResolving(t, what+": ListGrantsForPrincipal", func(token string) ([]*v2.Grant, string, error) {
					resp, err := s.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
						PrincipalId: v2.ResourceId_builder{ResourceType: prt, Resource: prid}.Build(),
						PageSize:    fcPageSize,
						PageToken:   token,
					}.Build())
					return resp.GetList(), resp.GetNextPageToken(), err
				})
			}
			if got != want {
				break
			}
			if want == "no_current_sync" {
				if op.Grants != nil && len(*op.Grants) > 0 {
					t.Fatalf("%s: no_current_sync with a non-empty \"grants\"", what)
				}
				continue
			}
			if op.Grants == nil {
				t.Fatalf("%s: an ok read requires \"grants\"", what)
			}
			// Exact comparison: a reopened store that silently lost
			// records fails here.
			fcRequireGrantList(t, what, fcGotGrants(t, fcStoredExtIDs(ctx, t, s), gs), fcRequireGrants(t, field+".grants", *op.Grants))
			continue
		default:
			t.Fatalf("%s: unknown container op", what)
		}
		if got != want {
			t.Fatalf("%s: got %q, want %q", what, got, want)
		}
	}
}

func fcDamageName(d *string) string {
	if d == nil {
		return "none"
	}
	return *d
}

// fcGrantRequest and fcOpRequest are the `c1z-oracle --respond` request
// shapes for container: the case without expected fields. Byte strings
// are hex already.
type fcEntRefRequest struct {
	RT  string `json:"rt"`
	RID string `json:"rid"`
	Ext string `json:"ext"`
}

type fcGrantRequest struct {
	Ent   fcEntRefRequest `json:"ent"`
	PRT   string          `json:"prt"`
	PRID  string          `json:"prid"`
	ExtID string          `json:"ext_id"`
}

type fcOpRequest struct {
	Op       string           `json:"op"`
	ID       *string          `json:"id,omitempty"`
	Type     *string          `json:"type,omitempty"`
	Batch    []fcGrantRequest `json:"batch,omitempty"`
	Days     *int             `json:"days,omitempty"`
	ReadOnly *bool            `json:"readonly,omitempty"`
	// Damage is null or a damage name on save_reopen and absent elsewhere.
	Damage json.RawMessage `json:"damage,omitempty"`
	PRT    *string         `json:"prt,omitempty"`
	PRID   *string         `json:"prid,omitempty"`
}

type fcCaseRequest struct {
	Name string        `json:"name"`
	Ops  []fcOpRequest `json:"ops"`
}

type fcRequest struct {
	Version   int             `json:"version"`
	Container []fcCaseRequest `json:"container"`
}

func fcHexStr(s string) string { return hex.EncodeToString([]byte(s)) }

func fcHexPtr(s string) *string {
	h := fcHexStr(s)
	return &h
}

func fcPtr[T any](v T) *T { return &v }

type fcIdentity struct{ rt, rid, ext, prt, prid string }

type fcGen struct{ r *rand.Rand }

// identities returns up to n distinct well-formed grant identities from
// small pools, so they share entitlements and principals.
func (g fcGen) identities(n int) []fcIdentity {
	owners := [][2]string{{"app", "1"}, {"app", "é"}, {"group", "g:1"}}
	exts := []string{"member", "admin", "日本"}
	prts := []string{"user", "group"}
	prids := []string{"a", "b", "ß"}
	seen := map[fcIdentity]bool{}
	var out []fcIdentity
	for attempts := 0; len(out) < n && attempts < 20*n; attempts++ {
		o := owners[g.r.IntN(len(owners))]
		id := fcIdentity{rt: o[0], rid: o[1], ext: exts[g.r.IntN(len(exts))], prt: prts[g.r.IntN(len(prts))], prid: prids[g.r.IntN(len(prids))]}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func (g fcGen) grant(id fcIdentity) fcGrantRequest {
	extID := ""
	switch g.r.IntN(4) {
	case 0:
		extID = id.ext + ":" + id.prt + ":" + id.prid
	case 1:
		extID = []string{"g1", "g2"}[g.r.IntN(2)]
	}
	return fcGrantRequest{
		Ent: fcEntRefRequest{RT: fcHexStr(id.rt), RID: fcHexStr(id.rid), Ext: fcHexStr(id.ext)},
		PRT: fcHexStr(id.prt), PRID: fcHexStr(id.prid), ExtID: fcHexStr(extID),
	}
}

func (g fcGen) batch(pool []fcIdentity) []fcGrantRequest {
	out := make([]fcGrantRequest, 1+g.r.IntN(3))
	for j := range out {
		out[j] = g.grant(pool[g.r.IntN(len(pool))])
	}
	return out
}

var fcDamages = []string{"truncate_header", "bad_magic", "bad_engine", "flip_payload_byte", "truncate_tail"}

// containerCase tracks enough state to keep requests inside the schema:
// put needs a bound sync and a writable store, age_sync a sync-run record
// and a writable store, damage a saved file, and after a read-only open
// only reads, write (which the store refuses), and save_reopen follow.
// save_reopen needs a sync-run record: the store writes the .c1z only
// when dirty, the model does not track dirtiness, and the oracle refuses
// a save_reopen before any start_new.
func (g fcGen) containerCase(i int) fcCaseRequest {
	ids := []string{"s1", "s2"}
	startTypes := []string{"full", "partial", "resources_only"}
	pool := g.identities(2 + g.r.IntN(3))
	c := fcCaseRequest{Name: fmt.Sprintf("property/container/%d", i)}
	// bound: a sync is bound. fresh: start_new would be refused. record:
	// the symbolic id of the sync-run record; once set, a Close has
	// written or will write a file. finished, stale: the record's
	// state, which decides whether an open binds it. readOnly: the store
	// was opened read-only.
	bound, fresh, record := false, false, ""
	finished, stale, readOnly := false, false, false
	n := 3 + g.r.IntN(9)
	for j := 0; j < n; j++ {
		k := g.r.IntN(20)
		if j == 0 && g.r.IntN(4) != 0 {
			k = 0
		}
		if readOnly && k < 12 && k != 10 {
			k = 12 + g.r.IntN(8)
		}
		switch {
		case k < 3:
			id := ids[g.r.IntN(len(ids))]
			c.Ops = append(c.Ops, fcOpRequest{Op: "start_new", ID: fcPtr(id), Type: fcPtr(startTypes[g.r.IntN(len(startTypes))])})
			if !fresh {
				record, fresh, finished, stale = id, true, false, false
			}
			bound = true
		case k < 8 && bound:
			op := "put"
			if g.r.IntN(3) == 0 {
				op = "put_deferred"
			}
			c.Ops = append(c.Ops, fcOpRequest{Op: op, Batch: g.batch(pool)})
		case k < 9:
			c.Ops = append(c.Ops, fcOpRequest{Op: "end"})
			if bound {
				finished = true
			}
			bound, fresh = false, false
		case k < 10 && record != "":
			days := []int{0, 1, 6, 8, 30}[g.r.IntN(5)]
			c.Ops = append(c.Ops, fcOpRequest{Op: "age_sync", Days: fcPtr(days)})
			stale = days > 7
		case k < 11:
			c.Ops = append(c.Ops, fcOpRequest{Op: "write"})
		case k < 12:
			id := ids[g.r.IntN(len(ids))]
			if g.r.IntN(5) == 0 {
				id = "s9"
			}
			c.Ops = append(c.Ops, fcOpRequest{Op: "resume", ID: fcPtr(id)})
			if id == record {
				bound, fresh = true, false
			}
		case k < 15 && record != "":
			ro := g.r.IntN(3) == 0
			op := fcOpRequest{Op: "save_reopen", ReadOnly: fcPtr(ro), Damage: json.RawMessage("null")}
			if g.r.IntN(6) == 0 {
				op.Damage = json.RawMessage(strconv.Quote(fcDamages[g.r.IntN(len(fcDamages))]))
				c.Ops = append(c.Ops, op)
				return c
			}
			c.Ops = append(c.Ops, op)
			// The public open binds the default sync when one resolves.
			bound = !ro && record != "" && (finished || !stale)
			fresh, readOnly = false, ro
		case k < 17:
			c.Ops = append(c.Ops, fcOpRequest{Op: "list_grants"})
		case k < 19:
			id := pool[g.r.IntN(len(pool))]
			c.Ops = append(c.Ops, fcOpRequest{Op: "read_by_principal", PRT: fcHexPtr(id.prt), PRID: fcHexPtr(id.prid)})
		default:
			c.Ops = append(c.Ops, fcOpRequest{Op: "latest_finished", Type: fcPtr("any")})
		}
	}
	// End with a round trip so every case checks what the file kept.
	if record != "" {
		c.Ops = append(c.Ops, fcOpRequest{Op: "save_reopen", ReadOnly: fcPtr(g.r.IntN(2) == 0), Damage: json.RawMessage("null")}, fcOpRequest{Op: "list_grants"})
	}
	return c
}

func fcEnvInt(t *testing.T, name string, def uint64) uint64 {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		t.Fatalf("%s=%q, want a positive integer", name, s)
	}
	return v
}

func fcRunOracle(t *testing.T, oracle string, request []byte) []byte {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), oracle, "--respond") //nolint:gosec // The path comes from C1Z_FORMAL_ORACLE, set by the developer.
	cmd.Stdin = bytes.NewReader(request)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s --respond: %v\nstderr:\n%s", oracle, err, stderr.String())
	}
	if stdout.Len() == 0 {
		t.Fatalf("%s --respond wrote nothing to stdout\nstderr:\n%s", oracle, stderr.String())
	}
	return stdout.Bytes()
}

// TestFormalContainerProperty generates random container requests, has
// the Lean model compute the expected results through `c1z-oracle
// --respond`, and replays them. It reads the same environment as
// TestFormalProperty in engine/pebble (ORACLE_SCHEMA.md "Go-side
// environment").
func TestFormalContainerProperty(t *testing.T) {
	oracle := os.Getenv("C1Z_FORMAL_ORACLE")
	if oracle == "" {
		t.Skip("C1Z_FORMAL_ORACLE is unset; set it to the c1z-oracle binary (formal/c1z/.lake/build/bin/c1z-oracle) to run")
	}
	n := int(fcEnvInt(t, "C1Z_FORMAL_PROPERTY_N", 200)) //nolint:gosec // A test case count.
	seed := fcEnvInt(t, "C1Z_FORMAL_PROPERTY_SEED", uint64(time.Now().UnixNano()))
	// The container family is disk-bound (Close checkpoints and envelope
	// extraction), so it runs a tenth of N unless set.
	containerN := int(fcEnvInt(t, "C1Z_FORMAL_CONTAINER_N", uint64(max(1, n/10)))) //nolint:gosec // A test case count.
	t.Logf("C1Z_FORMAL_PROPERTY_SEED=%d C1Z_FORMAL_PROPERTY_N=%d C1Z_FORMAL_CONTAINER_N=%d", seed, n, containerN)

	g := fcGen{r: rand.New(rand.NewPCG(seed, seed))} //nolint:gosec // A seeded generator makes a failing case replayable.
	req := &fcRequest{Version: fcOracleVersion}
	for i := range containerN {
		req.Container = append(req.Container, g.containerCase(i))
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := decodeFCCases(bytes.NewReader(fcRunOracle(t, oracle, body)))
	if err != nil {
		t.Fatalf("oracle response: %v", err)
	}
	for family, got := range resp.Counts {
		want := 0
		if family == "container" {
			want = containerN
		}
		if got != want {
			t.Fatalf("counts[%q] = %d, want %d", family, got, want)
		}
	}
	for i := range req.Container {
		if resp.Container[i].Name != req.Container[i].Name {
			t.Fatalf("container[%d]: response name %q, request name %q", i, resp.Container[i].Name, req.Container[i].Name)
		}
	}
	replayFCCases(t, resp.Container, func(t *testing.T, i int) {
		rq, errA := json.Marshal(req.Container[i])
		rs, errB := json.Marshal(resp.Container[i])
		t.Logf("seed %d, container[%d]\nrequest:  %s\nresponse: %s (marshal errors: %v)", seed, i, rq, rs, errors.Join(errA, errB))
	})
}
