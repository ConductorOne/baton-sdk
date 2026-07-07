package pebble

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

func TestEmitWhaleGrantIdentityHashes(t *testing.T) {
	path := os.Getenv("BATON_INSPECT_C1Z")
	layout := os.Getenv("BATON_INSPECT_LAYOUT")
	outPath := os.Getenv("BATON_HASH_OUT")
	if path == "" || layout == "" || outPath == "" {
		t.Skip("BATON_INSPECT_C1Z, BATON_INSPECT_LAYOUT, BATON_HASH_OUT required")
	}
	db, closeDB := openInspectDB(t, path)
	defer closeDB()
	if layout == "legacy" {
		emitLegacyGrantIdentityHashesText(t, db, layout, outPath)
		return
	}

	out, err := os.Create(outPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()

	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantByEntitlementLowerBound(),
		UpperBound: GrantByEntitlementUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok := grantIdentityFromByEntitlementKey(iter.Key(), layout)
		if !ok {
			continue
		}
		fmt.Fprintln(w, identityHash(identity))
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s emitted=%d out=%s", layout, count, outPath)
}

func TestEmitSortedWhaleGrantIdentityHashBin(t *testing.T) {
	path := os.Getenv("BATON_INSPECT_C1Z")
	layout := os.Getenv("BATON_INSPECT_LAYOUT")
	outPath := os.Getenv("BATON_HASH_BIN_OUT")
	if path == "" || layout == "" || outPath == "" {
		t.Skip("BATON_INSPECT_C1Z, BATON_INSPECT_LAYOUT, BATON_HASH_BIN_OUT required")
	}
	db, closeDB := openInspectDB(t, path)
	defer closeDB()
	if layout == "legacy" {
		emitSortedLegacyGrantIdentityHashBin(t, db, layout, outPath)
		return
	}
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantByEntitlementLowerBound(),
		UpperBound: GrantByEntitlementUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	hashes := make([]identityHash128, 0, 60_000_000)
	scanned := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok := grantIdentityFromByEntitlementKey(iter.Key(), layout)
		if !ok {
			continue
		}
		hashes = append(hashes, identityHashBytes(identity))
		scanned++
		if scanned%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: scanned=%d hashes=%d\n", layout, scanned, len(hashes))
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	sortAndWriteIdentityHashes(t, layout, outPath, hashes)
}

func TestDiffSortedWhaleGrantIdentityHashBin(t *testing.T) {
	oldPath := os.Getenv("BATON_OLD_HASH_BIN")
	newPath := os.Getenv("BATON_NEW_HASH_BIN")
	if oldPath == "" || newPath == "" {
		t.Skip("BATON_OLD_HASH_BIN and BATON_NEW_HASH_BIN required")
	}
	oldFile, err := os.Open(oldPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close()
	newFile, err := os.Open(newPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer newFile.Close()
	oldR := bufio.NewReaderSize(oldFile, 1<<20)
	newR := bufio.NewReaderSize(newFile, 1<<20)
	oldHash, oldOK, err := readHash128(oldR)
	if err != nil {
		t.Fatal(err)
	}
	newHash, newOK, err := readHash128(newR)
	if err != nil {
		t.Fatal(err)
	}
	onlyOld, onlyNew, both := 0, 0, 0
	var onlyOldSample, onlyNewSample []string
	for oldOK || newOK {
		switch {
		case !newOK || (oldOK && oldHash.less(newHash)):
			onlyOld++
			if len(onlyOldSample) < 20 {
				onlyOldSample = append(onlyOldSample, oldHash.hex())
			}
			oldHash, oldOK, err = readHash128(oldR)
		case !oldOK || newHash.less(oldHash):
			onlyNew++
			if len(onlyNewSample) < 20 {
				onlyNewSample = append(onlyNewSample, newHash.hex())
			}
			newHash, newOK, err = readHash128(newR)
		default:
			both++
			oldHash, oldOK, err = readHash128(oldR)
			if err != nil {
				t.Fatal(err)
			}
			newHash, newOK, err = readHash128(newR)
		}
		if err != nil {
			t.Fatal(err)
		}
		total := onlyOld + onlyNew + both
		if total%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "diff: compared=%d both=%d only_old=%d only_new=%d\n", total, both, onlyOld, onlyNew)
		}
	}
	t.Logf("both=%d only_old=%d only_new=%d", both, onlyOld, onlyNew)
	t.Logf("only_old_sample=%v", onlyOldSample)
	t.Logf("only_new_sample=%v", onlyNewSample)
}

func TestDiffWhaleGrantIdentitiesOneShot(t *testing.T) {
	oldPath := os.Getenv("BATON_OLD_C1Z")
	newPath := os.Getenv("BATON_NEW_C1Z")
	oldLayout := os.Getenv("BATON_OLD_LAYOUT")
	newLayout := os.Getenv("BATON_NEW_LAYOUT")
	if oldPath == "" || newPath == "" || oldLayout == "" || newLayout == "" {
		t.Skip("BATON_OLD_C1Z, BATON_NEW_C1Z, BATON_OLD_LAYOUT, BATON_NEW_LAYOUT required")
	}
	dir := t.TempDir()
	oldBin := filepath.Join(dir, "old.u16")
	newBin := filepath.Join(dir, "new.u16")
	emitSortedGrantIdentityHashBin(t, oldPath, oldLayout, oldBin)
	runtime.GC()
	emitSortedGrantIdentityHashBin(t, newPath, newLayout, newBin)
	diffSortedGrantIdentityHashBin(t, oldBin, newBin)
}

func TestPrintWhaleGrantIdentityHash(t *testing.T) {
	path := os.Getenv("BATON_INSPECT_C1Z")
	layout := os.Getenv("BATON_INSPECT_LAYOUT")
	hash := os.Getenv("BATON_IDENTITY_HASH")
	if path == "" || layout == "" || hash == "" {
		t.Skip("BATON_INSPECT_C1Z, BATON_INSPECT_LAYOUT, BATON_IDENTITY_HASH required")
	}
	db, closeDB := openInspectDB(t, path)
	defer closeDB()
	if layout == "legacy" {
		printLegacyGrantIdentityHash(t, db, layout, hash)
		return
	}
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantByEntitlementLowerBound(),
		UpperBound: GrantByEntitlementUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	matches := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok := grantIdentityFromByEntitlementKey(iter.Key(), layout)
		if !ok || identityHash(identity) != hash {
			continue
		}
		matches++
		rec, err := fetchGrantForByEntitlementKey(db, iter.Key(), layout)
		if err != nil {
			t.Fatal(err)
		}
		discovered := "<nil>"
		if rec.GetDiscoveredAt() != nil {
			discovered = rec.GetDiscoveredAt().AsTime().String()
		}
		t.Logf("MATCH %d identity=%q external_id=%q ent=(%q,%q,%q) principal=(%q,%q) needs_expansion=%v sources=%d discovered=%s",
			matches, identity, rec.GetExternalId(),
			rec.GetEntitlement().GetResourceTypeId(), rec.GetEntitlement().GetResourceId(), rec.GetEntitlement().GetEntitlementId(),
			rec.GetPrincipal().GetResourceTypeId(), rec.GetPrincipal().GetResourceId(),
			rec.GetNeedsExpansion(), len(rec.GetSources()), discovered)
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s hash=%s matches=%d", layout, hash, matches)
}

func openInspectDB(t *testing.T, path string) (*cpebble.DB, func()) {
	t.Helper()
	tmp := t.TempDir()
	f, err := os.Open(path) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, _, err := formatv3.ExtractEnvelopePayload(f, tmp); err != nil {
		t.Fatal(err)
	}
	db, err := cpebble.Open(filepath.Clean(tmp), &cpebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { _ = db.Close() }
}

func emitLegacyGrantIdentityHashesText(t *testing.T, db *cpebble.DB, layout, outPath string) {
	t.Helper()
	out, err := os.Create(outPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok, err := grantIdentityFromLegacyGrantValue(iter.Value())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		fmt.Fprintln(w, identityHash(identity))
		count++
		if count%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: scanned=%d hashes=%d\n", layout, count, count)
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s emitted=%d out=%s", layout, count, outPath)
}

func emitSortedGrantIdentityHashBin(t *testing.T, path, layout, outPath string) {
	t.Helper()
	db, closeDB := openInspectDB(t, path)
	defer closeDB()
	if layout == "legacy" {
		emitSortedLegacyGrantIdentityHashBin(t, db, layout, outPath)
		return
	}
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantByEntitlementLowerBound(),
		UpperBound: GrantByEntitlementUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	hashes := make([]identityHash128, 0, 60_000_000)
	scanned := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok := grantIdentityFromByEntitlementKey(iter.Key(), layout)
		if !ok {
			continue
		}
		hashes = append(hashes, identityHashBytes(identity))
		scanned++
		if scanned%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: scanned=%d hashes=%d\n", layout, scanned, len(hashes))
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "emit %s: scan complete total=%d; sorting...\n", layout, len(hashes))
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].less(hashes[j])
	})
	fmt.Fprintf(os.Stderr, "emit %s: sort complete; writing unique hashes to %s...\n", layout, outPath)
	out, err := os.Create(outPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 1<<20)
	unique := 0
	var prev identityHash128
	for i, h := range hashes {
		if i > 0 && h == prev {
			continue
		}
		if err := h.write(w); err != nil {
			t.Fatal(err)
		}
		prev = h
		unique++
		if unique%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: wrote unique=%d\n", layout, unique)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s total=%d unique=%d out=%s", layout, len(hashes), unique, outPath)
}

func printLegacyGrantIdentityHash(t *testing.T, db *cpebble.DB, layout, hash string) {
	t.Helper()
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	matches := 0
	scanned := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok, err := grantIdentityFromLegacyGrantValue(iter.Value())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		scanned++
		if identityHash(identity) != hash {
			if scanned%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "print %s: scanned=%d matches=%d\n", layout, scanned, matches)
			}
			continue
		}
		matches++
		var rec v3.GrantRecord
		if err := unmarshalRecord(iter.Value(), &rec); err != nil {
			t.Fatal(err)
		}
		discovered := "<nil>"
		if rec.GetDiscoveredAt() != nil {
			discovered = rec.GetDiscoveredAt().AsTime().String()
		}
		t.Logf("MATCH %d identity=%q external_id=%q ent=(%q,%q,%q) principal=(%q,%q) needs_expansion=%v sources=%d discovered=%s",
			matches, identity, rec.GetExternalId(),
			rec.GetEntitlement().GetResourceTypeId(), rec.GetEntitlement().GetResourceId(), rec.GetEntitlement().GetEntitlementId(),
			rec.GetPrincipal().GetResourceTypeId(), rec.GetPrincipal().GetResourceId(),
			rec.GetNeedsExpansion(), len(rec.GetSources()), discovered)
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s hash=%s scanned=%d matches=%d", layout, hash, scanned, matches)
}

func emitSortedLegacyGrantIdentityHashBin(t *testing.T, db *cpebble.DB, layout, outPath string) {
	t.Helper()
	iter, err := db.NewIter(&cpebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	hashes := make([]identityHash128, 0, 60_000_000)
	scanned := 0
	for iter.First(); iter.Valid(); iter.Next() {
		identity, ok, err := grantIdentityFromLegacyGrantValue(iter.Value())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		hashes = append(hashes, identityHashBytes(identity))
		scanned++
		if scanned%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: scanned=%d hashes=%d\n", layout, scanned, len(hashes))
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	sortAndWriteIdentityHashes(t, layout, outPath, hashes)
}

func sortAndWriteIdentityHashes(t *testing.T, layout, outPath string, hashes []identityHash128) {
	t.Helper()
	fmt.Fprintf(os.Stderr, "emit %s: scan complete total=%d; sorting...\n", layout, len(hashes))
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].less(hashes[j])
	})
	fmt.Fprintf(os.Stderr, "emit %s: sort complete; writing unique hashes to %s...\n", layout, outPath)
	out, err := os.Create(outPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 1<<20)
	unique := 0
	var prev identityHash128
	for i, h := range hashes {
		if i > 0 && h == prev {
			continue
		}
		if err := h.write(w); err != nil {
			t.Fatal(err)
		}
		prev = h
		unique++
		if unique%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "emit %s: wrote unique=%d\n", layout, unique)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Logf("layout=%s total=%d unique=%d out=%s", layout, len(hashes), unique, outPath)
}

func diffSortedGrantIdentityHashBin(t *testing.T, oldPath, newPath string) {
	t.Helper()
	oldFile, err := os.Open(oldPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close()
	newFile, err := os.Open(newPath) // #nosec G304,G703 -- test-controlled env path.
	if err != nil {
		t.Fatal(err)
	}
	defer newFile.Close()
	oldR := bufio.NewReaderSize(oldFile, 1<<20)
	newR := bufio.NewReaderSize(newFile, 1<<20)
	oldHash, oldOK, err := readHash128(oldR)
	if err != nil {
		t.Fatal(err)
	}
	newHash, newOK, err := readHash128(newR)
	if err != nil {
		t.Fatal(err)
	}
	onlyOld, onlyNew, both := 0, 0, 0
	var onlyOldSample, onlyNewSample []string
	for oldOK || newOK {
		switch {
		case !newOK || (oldOK && oldHash.less(newHash)):
			onlyOld++
			if len(onlyOldSample) < 20 {
				onlyOldSample = append(onlyOldSample, oldHash.hex())
			}
			oldHash, oldOK, err = readHash128(oldR)
		case !oldOK || newHash.less(oldHash):
			onlyNew++
			if len(onlyNewSample) < 20 {
				onlyNewSample = append(onlyNewSample, newHash.hex())
			}
			newHash, newOK, err = readHash128(newR)
		default:
			both++
			oldHash, oldOK, err = readHash128(oldR)
			if err != nil {
				t.Fatal(err)
			}
			newHash, newOK, err = readHash128(newR)
		}
		if err != nil {
			t.Fatal(err)
		}
		total := onlyOld + onlyNew + both
		if total%1_000_000 == 0 {
			fmt.Fprintf(os.Stderr, "diff: compared=%d both=%d only_old=%d only_new=%d\n", total, both, onlyOld, onlyNew)
		}
	}
	t.Logf("both=%d only_old=%d only_new=%d", both, onlyOld, onlyNew)
	t.Logf("only_old_sample=%v", onlyOldSample)
	t.Logf("only_new_sample=%v", onlyNewSample)
}

func grantIdentityFromByEntitlementKey(key []byte, layout string) (string, bool) {
	prefix := []byte{versionV3, typeIndex, idxGrantByEntitlement, 0x00}
	switch layout {
	case "legacy":
		return "", false
	case "structured":
		parts, ok := decodeTupleComponents(key, prefix, 6)
		if !ok {
			return "", false
		}
		return strings.Join(parts, "\x1f"), true
	default:
		return "", false
	}
}

func grantIdentityFromLegacyGrantValue(value []byte) (string, bool, error) {
	entRT, entRID, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(value)
	if err != nil {
		return "", false, err
	}
	if entRT == "" || entRID == "" || entID == "" || principalRT == "" || principalID == "" {
		return "", false, nil
	}
	ent := entitlementIdentityFromParts(entRT, entRID, entID)
	return strings.Join([]string{ent.resourceTypeID, ent.resourceID, ent.flagComponent(), ent.tail, principalRT, principalID}, "\x1f"), true, nil
}

func identityHash(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:16])
}

type identityHash128 struct {
	hi uint64
	lo uint64
}

func (h identityHash128) less(other identityHash128) bool {
	if h.hi != other.hi {
		return h.hi < other.hi
	}
	return h.lo < other.lo
}

func (h identityHash128) write(w io.Writer) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], h.hi)
	binary.BigEndian.PutUint64(buf[8:], h.lo)
	_, err := w.Write(buf[:])
	return err
}

func (h identityHash128) hex() string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], h.hi)
	binary.BigEndian.PutUint64(buf[8:], h.lo)
	return hex.EncodeToString(buf[:])
}

func identityHashBytes(identity string) identityHash128 {
	sum := sha256.Sum256([]byte(identity))
	return identityHash128{
		hi: binary.BigEndian.Uint64(sum[:8]),
		lo: binary.BigEndian.Uint64(sum[8:16]),
	}
}

func readHash128(r *bufio.Reader) (identityHash128, bool, error) {
	var buf [16]byte
	_, err := io.ReadFull(r, buf[:])
	if err == nil {
		return identityHash128{
			hi: binary.BigEndian.Uint64(buf[:8]),
			lo: binary.BigEndian.Uint64(buf[8:]),
		}, true, nil
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return identityHash128{}, false, nil
	}
	return identityHash128{}, false, err
}

func fetchGrantForByEntitlementKey(db *cpebble.DB, key []byte, layout string) (*v3.GrantRecord, error) {
	var primary []byte
	prefix := []byte{versionV3, typeIndex, idxGrantByEntitlement, 0x00}
	switch layout {
	case "legacy":
		parts, ok := decodeTupleComponents(key, prefix, 4)
		if !ok {
			return nil, fmt.Errorf("decode legacy by_entitlement key")
		}
		primary = encodeGrantKey(parts[3])
	case "structured":
		parts, ok := decodeTupleComponents(key, prefix, 6)
		if !ok {
			return nil, fmt.Errorf("decode structured by_entitlement key")
		}
		primary = encodeGrantIdentityKey(grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: parts[0],
				resourceID:     parts[1],
				stripped:       parts[2] == idFlagStripped,
				tail:           parts[3],
			},
			principalTypeID: parts[4],
			principalID:     parts[5],
		})
	default:
		return nil, fmt.Errorf("unknown layout %q", layout)
	}
	val, closer, err := db.Get(primary)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var rec v3.GrantRecord
	if err := unmarshalRecord(val, &rec); err != nil {
		return nil, err
	}
	_ = context.Background()
	return &rec, nil
}
