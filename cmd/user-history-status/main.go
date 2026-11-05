// INC-1154 helper: read a CSV export of C1 user object-history rows
// (columns user_id, changed_at, change_kind, snapshot_b64, where the snapshot
// is a zstd-compressed c1.models.user.v2.User) and print each user's status
// per snapshot plus a verdict relative to a campaign's preparation date.
// Reads the protobuf wire format directly, so it needs no C1 schema.
// Not for merge.
//
//	go run ./cmd/user-history-status -file export.csv -prepared 2026-08-13
package main

import (
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protowire"
)

// Field numbers of c1.models.user.v2.User.
const (
	fieldID              = 2
	fieldDisplayName     = 7
	fieldEmail           = 8
	fieldStatus          = 9
	fieldDirectoryStatus = 25
)

var statusNames = map[uint64]string{0: "UNSPECIFIED", 1: "ENABLED", 2: "DISABLED", 3: "DELETED"}

type snapshot struct {
	at, id, name, email, status, directory string
}

func decodeUser(b []byte) (snapshot, error) {
	var s snapshot
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return s, protowire.ParseError(n)
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return s, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case fieldStatus:
				s.status = statusName(v)
			case fieldDirectoryStatus:
				s.directory = statusName(v)
			}
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return s, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case fieldID:
				s.id = string(v)
			case fieldDisplayName:
				s.name = string(v)
			case fieldEmail:
				s.email = string(v)
			}
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return s, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	if s.status == "" {
		s.status = statusNames[0]
	}
	return s, nil
}

func statusName(v uint64) string {
	if n, ok := statusNames[v]; ok {
		return n
	}
	return fmt.Sprintf("STATUS_%d", v)
}

func main() {
	path := flag.String("file", "", "CSV export: user_id, changed_at, change_kind, snapshot_b64")
	prepared := flag.String("prepared", "2026-08-13", "campaign preparation date (YYYY-MM-DD)")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "usage: -file export.csv [-prepared YYYY-MM-DD]")
		os.Exit(2)
	}
	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.LazyQuotes = true
	header, err := r.Read()
	if err != nil {
		fmt.Fprintln(os.Stderr, "header:", err)
		os.Exit(1)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(strings.ToLower(h))] = i
	}
	for _, need := range []string{"user_id", "changed_at", "snapshot_b64"} {
		if _, ok := col[need]; !ok {
			fmt.Fprintf(os.Stderr, "missing column %q; have %v\n", need, header)
			os.Exit(1)
		}
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	byUser := map[string][]snapshot{}
	rows := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "row:", err)
			os.Exit(1)
		}
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rec[col["snapshot_b64"]]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "row %d base64: %v\n", rows+2, err)
			os.Exit(1)
		}
		plain, err := dec.DecodeAll(raw, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "row %d zstd: %v\n", rows+2, err)
			os.Exit(1)
		}
		s, err := decodeUser(plain)
		if err != nil {
			fmt.Fprintf(os.Stderr, "row %d proto: %v\n", rows+2, err)
			os.Exit(1)
		}
		s.at = strings.TrimSpace(rec[col["changed_at"]])
		uid := strings.TrimSpace(rec[col["user_id"]])
		byUser[uid] = append(byUser[uid], s)
		rows++
	}
	ids := make([]string, 0, len(byUser))
	for id := range byUser {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	counts := map[string]int{}
	for _, id := range ids {
		snaps := byUser[id]
		sort.Slice(snaps, func(i, j int) bool { return snaps[i].at < snaps[j].at })
		last := snaps[len(snaps)-1]
		fmt.Printf("\n%s %q <%s>\n", id, last.name, last.email)
		enabledAfter, enabledBefore, seenAfter, firstAfterDisabled := false, false, false, false
		for _, s := range snaps {
			after := len(s.at) >= 10 && s.at[:10] > *prepared
			marker := "before"
			if after {
				marker = "after "
			}
			fmt.Printf("  %s prepare  %-32s status=%-11s directory=%s\n", marker, s.at, s.status, s.directory)
			if after {
				if !seenAfter {
					seenAfter = true
					firstAfterDisabled = s.status == "DISABLED"
				}
				if s.status == "ENABLED" {
					enabledAfter = true
				}
			} else if s.status == "ENABLED" {
				enabledBefore = true
			}
		}
		var verdict string
		switch {
		case enabledAfter:
			verdict = "DRIFT: still enabled after preparation, disabled later; the campaign was right to skip this user"
		case seenAfter && firstAfterDisabled && enabledBefore:
			verdict = "DRIFT: enabled before preparation, first post-preparation snapshot disabled; disable came after"
		case seenAfter && firstAfterDisabled:
			verdict = "AMBIGUOUS: every exported snapshot shows disabled; no in-window snapshot proves the status before preparation"
		default:
			verdict = "NO DATA: no snapshot after preparation in the export"
		}
		fmt.Println("  verdict:", verdict)
		counts[strings.SplitN(verdict, ":", 2)[0]]++
	}
	fmt.Printf("\n%d rows, %d users:", rows, len(ids))
	for _, k := range []string{"DRIFT", "AMBIGUOUS", "NO DATA"} {
		fmt.Printf(" %s=%d", k, counts[k])
	}
	fmt.Println()
}
