// INC-1154 helper: print every grant held by one or more principals in one or
// more c1z sync files, with the connector's grant sources and any
// grant-expansion annotations, so a grant can be classified direct or
// inherited as of that sync. Lives in the sdk module. Not for merge.
//
//	go run ./cmd/c1z-grant-sources -file a.c1z,b.c1z -principal jdoe,asmith -entitlement :member
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
)

type totals struct{ direct, inherited, shown, unmatched int }

func main() {
	files := flag.String("file", "", "comma-separated paths to .c1z files")
	principals := flag.String("principal", "", "comma-separated case-insensitive substrings of principal id, display name, login or email")
	entFilter := flag.String("entitlement", "", "optional case-insensitive substring of the entitlement id or display name")
	flag.Parse()
	if *files == "" || *principals == "" {
		fmt.Fprintln(os.Stderr, "usage: -file a.c1z[,b.c1z] -principal <substr>[,<substr>] [-entitlement <substr>]")
		os.Exit(2)
	}
	ctx := context.Background()
	var needles []string
	for _, p := range strings.Split(*principals, ",") {
		if p = strings.TrimSpace(p); p != "" {
			needles = append(needles, strings.ToLower(p))
		}
	}
	var sum totals
	for _, path := range strings.Split(*files, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		fmt.Printf("==== %s\n", path)
		t, err := runFile(ctx, path, needles, strings.ToLower(*entFilter))
		if err != nil {
			fmt.Fprintln(os.Stderr, path+":", err)
			os.Exit(1)
		}
		sum.direct += t.direct
		sum.inherited += t.inherited
		sum.shown += t.shown
		sum.unmatched += t.unmatched
	}
	fmt.Printf("\nTOTAL: %d grants shown, DIRECT=%d INHERITED=%d, principals not found=%d\n", sum.shown, sum.direct, sum.inherited, sum.unmatched)
}

func runFile(ctx context.Context, path string, needles []string, ef string) (totals, error) {
	var t totals
	store, cleanup, err := openStore(ctx, path)
	if err != nil {
		return t, err
	}
	defer cleanup()
	defer store.Close(ctx)

	key := func(id *v2.ResourceId) string { return id.GetResourceType() + "|" + id.GetResource() }
	matched := map[string]*v2.Resource{}
	matchedBy := map[string]string{}
	found := map[string]bool{}
	for tok := ""; ; {
		resp, err := store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			return t, fmt.Errorf("resource types: %w", err)
		}
		for _, rt := range resp.GetList() {
			for rtok := ""; ; {
				rresp, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{ResourceTypeId: rt.GetId(), PageSize: 1000, PageToken: rtok})
				if err != nil {
					return t, fmt.Errorf("resources: %w", err)
				}
				for _, r := range rresp.GetList() {
					for _, n := range needles {
						if resourceMatches(r, n) {
							matched[key(r.GetId())] = r
							matchedBy[key(r.GetId())] = n
							found[n] = true
							break
						}
					}
				}
				if rtok = rresp.GetNextPageToken(); rtok == "" {
					break
				}
			}
		}
		if tok = resp.GetNextPageToken(); tok == "" {
			break
		}
	}
	for _, n := range needles {
		if !found[n] {
			fmt.Printf("  (no principal matched %q in this file)\n", n)
			t.unmatched++
		}
	}
	if len(matched) == 0 {
		return t, nil
	}

	entNames := map[string]string{}
	for tok := ""; ; {
		resp, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			return t, fmt.Errorf("entitlements: %w", err)
		}
		for _, e := range resp.GetList() {
			entNames[e.GetId()] = e.GetDisplayName()
		}
		if tok = resp.GetNextPageToken(); tok == "" {
			break
		}
	}

	held := map[string][]*v2.Grant{}
	for tok := ""; ; {
		resp, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			return t, fmt.Errorf("grants: %w", err)
		}
		for _, g := range resp.GetList() {
			k := key(g.GetPrincipal().GetId())
			if _, ok := matched[k]; ok {
				held[k] = append(held[k], g)
			}
		}
		if tok = resp.GetNextPageToken(); tok == "" {
			break
		}
	}

	keys := make([]string, 0, len(matched))
	for k := range matched {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := matched[k]
		grants := held[k]
		fmt.Printf("\n  principal %s: %q %s (matched %q, %d grants in file)\n", k, r.GetDisplayName(), principalIdentity(r), matchedBy[k], len(grants))
		// expandedFrom[E] = entitlements this principal holds whose grants carry a
		// GrantExpandable annotation naming E; C1 records E grants as inherited.
		expandedFrom := map[string][]string{}
		for _, g := range grants {
			annos := annotations.Annotations(g.GetAnnotations())
			ge := &v2.GrantExpandable{}
			ok, err := annos.Pick(ge)
			if err != nil {
				return t, fmt.Errorf("annotations: %w", err)
			}
			if !ok {
				continue
			}
			for _, target := range ge.GetEntitlementIds() {
				expandedFrom[target] = append(expandedFrom[target], g.GetEntitlement().GetId())
			}
		}
		sort.Slice(grants, func(i, j int) bool { return grants[i].GetEntitlement().GetId() < grants[j].GetEntitlement().GetId() })
		for _, g := range grants {
			eid := g.GetEntitlement().GetId()
			name := entNames[eid]
			if name == "" {
				name = g.GetEntitlement().GetDisplayName()
			}
			if ef != "" && !strings.Contains(strings.ToLower(eid), ef) && !strings.Contains(strings.ToLower(name), ef) {
				continue
			}
			var srcs []string
			for s := range g.GetSources().GetSources() {
				srcs = append(srcs, s)
			}
			sort.Strings(srcs)
			verdict := "DIRECT"
			switch {
			case len(srcs) > 0:
				verdict = "INHERITED (connector sources)"
				t.inherited++
			case len(expandedFrom[eid]) > 0:
				verdict = "INHERITED (expanded from " + strings.Join(expandedFrom[eid], ", ") + ")"
				t.inherited++
			default:
				t.direct++
			}
			t.shown++
			fmt.Printf("    %-32s entitlement=%s %q sources=%v\n", verdict, eid, name, srcs)
		}
	}
	return t, nil
}

func resourceMatches(r *v2.Resource, needle string) bool {
	if strings.Contains(strings.ToLower(r.GetId().GetResource()), needle) || strings.Contains(strings.ToLower(r.GetDisplayName()), needle) {
		return true
	}
	return strings.Contains(strings.ToLower(principalIdentity(r)), needle)
}

func principalIdentity(r *v2.Resource) string {
	annos := annotations.Annotations(r.GetAnnotations())
	ut := &v2.UserTrait{}
	if ok, _ := annos.Pick(ut); !ok {
		return ""
	}
	parts := []string{"login=" + ut.GetLogin()}
	for _, e := range ut.GetEmails() {
		parts = append(parts, "email="+e.GetAddress())
	}
	return strings.Join(parts, " ")
}

// openStore opens a v3 (pebble) c1z directly. A v1 (sqlite) c1z is converted
// into a temporary pebble file first, since the store registry refuses v1.
func openStore(ctx context.Context, path string) (c1zstore.Store, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	format, err := dotc1z.ReadHeaderFormat(f)
	f.Close()
	if err != nil {
		return nil, nil, err
	}
	if format != dotc1z.C1ZFormatV1 {
		store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
		return store, func() {}, err
	}
	src, err := sqlite.Open(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	defer src.Close(ctx)
	syncID, err := src.ResolveSyncID(ctx, sqlite.ResolveNewest)
	if err != nil {
		return nil, nil, err
	}
	tmp, err := os.MkdirTemp("", "c1z-grant-sources-")
	if err != nil {
		return nil, nil, err
	}
	out := tmp + "/converted.c1z"
	fmt.Fprintf(os.Stderr, "v1 (sqlite) c1z: converting sync %s to a temporary pebble file\n", syncID)
	store, _, err := src.ToPebbleOpen(ctx, out, syncID)
	if err != nil {
		os.RemoveAll(tmp)
		return nil, nil, err
	}
	return store, func() { os.RemoveAll(tmp) }, nil
}
