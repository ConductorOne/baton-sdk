// INC-1154 helper: print every grant held by one principal in a c1z sync file,
// with the connector's grant sources and any grant-expansion annotations, so a
// grant can be classified direct or inherited as of that sync.
// Lives in the sdk module. Not for merge.
//
//	go run ./cmd/c1z-grant-sources -file sync.c1z -principal jdoe
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

func main() {
	path := flag.String("file", "", "path to .c1z")
	principal := flag.String("principal", "", "case-insensitive substring of the principal's id, display name, login or email")
	entFilter := flag.String("entitlement", "", "optional case-insensitive substring of the entitlement id or display name")
	flag.Parse()
	if *path == "" || *principal == "" {
		fmt.Fprintln(os.Stderr, "usage: -file sync.c1z -principal <substring> [-entitlement <substring>]")
		os.Exit(2)
	}
	ctx := context.Background()
	store, cleanup, err := openStore(ctx, *path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer cleanup()
	defer store.Close(ctx)

	needle := strings.ToLower(*principal)
	matched := map[string]*v2.Resource{} // key: type|id
	key := func(id *v2.ResourceId) string { return id.GetResourceType() + "|" + id.GetResource() }
	type typeSummary struct {
		name    string
		count   int
		samples []string
	}
	summary := map[string]*typeSummary{}
	var typeOrder []string

	for tok := ""; ; {
		resp, err := store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			fmt.Fprintln(os.Stderr, "resource types:", err)
			os.Exit(1)
		}
		for _, rt := range resp.GetList() {
			ts := &typeSummary{name: rt.GetDisplayName()}
			summary[rt.GetId()] = ts
			typeOrder = append(typeOrder, rt.GetId())
			for rtok := ""; ; {
				rresp, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{ResourceTypeId: rt.GetId(), PageSize: 1000, PageToken: rtok})
				if err != nil {
					fmt.Fprintln(os.Stderr, "resources:", err)
					os.Exit(1)
				}
				for _, r := range rresp.GetList() {
					ts.count++
					if len(ts.samples) < 3 {
						ts.samples = append(ts.samples, fmt.Sprintf("%q id=%s %s", r.GetDisplayName(), r.GetId().GetResource(), principalIdentity(r)))
					}
					if resourceMatches(r, needle) {
						matched[key(r.GetId())] = r
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
	if len(matched) == 0 {
		fmt.Println("no principal matched", *principal)
		fmt.Println("resource types in this file (count, then up to three samples):")
		for _, id := range typeOrder {
			ts := summary[id]
			fmt.Printf("  %s (%q): %d\n", id, ts.name, ts.count)
			for _, smp := range ts.samples {
				fmt.Printf("      %s\n", smp)
			}
		}
		return
	}
	for k, r := range matched {
		fmt.Printf("principal %s: %q %s\n", k, r.GetDisplayName(), principalIdentity(r))
	}

	entNames := map[string]string{}
	for tok := ""; ; {
		resp, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			fmt.Fprintln(os.Stderr, "entitlements:", err)
			os.Exit(1)
		}
		for _, e := range resp.GetList() {
			entNames[e.GetId()] = e.GetDisplayName()
		}
		if tok = resp.GetNextPageToken(); tok == "" {
			break
		}
	}

	var held []*v2.Grant
	for tok := ""; ; {
		resp, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{PageSize: 1000, PageToken: tok})
		if err != nil {
			fmt.Fprintln(os.Stderr, "grants:", err)
			os.Exit(1)
		}
		for _, g := range resp.GetList() {
			if _, ok := matched[key(g.GetPrincipal().GetId())]; ok {
				held = append(held, g)
			}
		}
		if tok = resp.GetNextPageToken(); tok == "" {
			break
		}
	}

	// expandedFrom[E] = entitlements held by the principal whose grants carry a
	// GrantExpandable annotation naming E. C1 records such E grants as sourced
	// from those entitlements (inherited).
	expandedFrom := map[string][]string{}
	for _, g := range held {
		annos := annotations.Annotations(g.GetAnnotations())
		ge := &v2.GrantExpandable{}
		ok, err := annos.Pick(ge)
		if err != nil {
			fmt.Fprintln(os.Stderr, "annotations:", err)
			os.Exit(1)
		}
		if !ok {
			continue
		}
		for _, target := range ge.GetEntitlementIds() {
			expandedFrom[target] = append(expandedFrom[target], g.GetEntitlement().GetId())
		}
	}

	sort.Slice(held, func(i, j int) bool { return held[i].GetEntitlement().GetId() < held[j].GetEntitlement().GetId() })
	ef := strings.ToLower(*entFilter)
	shown := 0
	for _, g := range held {
		eid := g.GetEntitlement().GetId()
		name := entNames[eid]
		if name == "" {
			name = g.GetEntitlement().GetDisplayName()
		}
		if ef != "" && !strings.Contains(strings.ToLower(eid), ef) && !strings.Contains(strings.ToLower(name), ef) {
			continue
		}
		shown++
		var srcs []string
		for s := range g.GetSources().GetSources() {
			srcs = append(srcs, s)
		}
		sort.Strings(srcs)
		verdict := "DIRECT"
		switch {
		case len(srcs) > 0:
			verdict = "INHERITED (connector sources)"
		case len(expandedFrom[eid]) > 0:
			verdict = "INHERITED (expanded from " + strings.Join(expandedFrom[eid], ", ") + ")"
		}
		fmt.Printf("%-32s grant=%s entitlement=%s %q sources=%v\n", verdict, g.GetId(), eid, name, srcs)
	}
	fmt.Printf("%d grants held by matched principal(s), %d shown\n", len(held), shown)
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
