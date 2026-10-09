package c1zsanitize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

const (
	sanitizeConfigFact = "c1zsanitize.config"
	sanitizePolicyV1   = 1

	sanitizeResourceTypesOp = "sanitize-resource-types"
	sanitizeResourcesOp     = "sanitize-resources"
	sanitizeEntitlementsOp  = "sanitize-entitlements"
	sanitizeGrantsOp        = "sanitize-grants"
	sanitizeTerminalOp      = "sanitize-terminal"
)

type ledgerSanitizeConfig struct {
	Version                 int    `json:"version"`
	SecretFingerprint       string `json:"secret_fingerprint"`
	SourceFingerprint       string `json:"source_fingerprint"`
	Anchor                  string `json:"anchor"`
	AllowUnknownAnnotations *bool  `json:"allow_unknown_annotations"`
}

func (s *sanitizer) runLedger(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	sourceSync *reader_v2.SyncRun,
) (string, error) {
	ledger, ok := dst.(c1zstore.PageLedgerStore)
	if !ok || dst.Metadata().Engine != string(c1zstore.EnginePebble) {
		return "", errors.New("c1zsanitize: destination must be a Pebble page-ledger store")
	}
	probe := ledger.BeginPage()
	_, trusted := probe.(c1zstore.TrustedImportPageWriter)
	probe.Discard()
	if !trusted {
		return "", errors.New("c1zsanitize: destination does not support trusted import pages")
	}
	runs, err := listDestinationSyncs(ctx, dst)
	if err != nil {
		return "", err
	}
	if len(runs) > 1 {
		return "", errors.New("c1zsanitize: Pebble destination contains multiple syncs")
	}

	syncType := connectorstore.SyncType(sourceSync.GetSyncType())
	if syncType == "" || syncType == connectorstore.SyncTypeAny {
		syncType = connectorstore.SyncTypeFull
	}
	expectedParentSyncID := ""
	if sourceParent := sourceSync.GetParentSyncId(); sourceParent != "" {
		expectedParentSyncID = s.id(sourceParent)
	}

	var destinationSyncID string
	finished := false
	if len(runs) == 0 {
		destinationSyncID, err = dst.StartNewSync(ctx, syncType, expectedParentSyncID)
		if err != nil {
			return "", err
		}
		config, err := s.ledgerConfig(sourceSync)
		if err != nil {
			return "", err
		}
		if err := ledger.BeginCollecting(ctx, []c1zstore.LedgerWork{sanitizeWork(sanitizeResourceTypesOp)}, map[string]string{sanitizeConfigFact: config}); err != nil {
			return "", err
		}
	} else {
		destinationSyncID = runs[0].ID
		if _, err := dst.ResumeSync(ctx, syncType, destinationSyncID); err != nil {
			return "", err
		}
		state, err := ledger.State(ctx)
		if err != nil {
			return "", err
		}
		if state.Phase == c1zstore.LedgerQueueAbsent && !state.Finished {
			empty, err := ledger.BoundSyncUnstarted(ctx)
			if err != nil {
				return "", err
			}
			if !empty {
				return "", errors.New("c1zsanitize: unfinished destination has records without sanitizer ledger state")
			}
			if runs[0].Type != syncType {
				return "", errors.New("c1zsanitize: empty destination has a different sync type")
			}
			if runs[0].ParentSyncID != expectedParentSyncID {
				return "", errors.New("c1zsanitize: empty destination has a different parent sync")
			}
			config, err := s.ledgerConfig(sourceSync)
			if err != nil {
				return "", err
			}
			if err := ledger.BeginCollecting(ctx, []c1zstore.LedgerWork{sanitizeWork(sanitizeResourceTypesOp)}, map[string]string{sanitizeConfigFact: config}); err != nil {
				return "", err
			}
		} else if err := s.validateLedgerConfig(ctx, ledger, sourceSync); err != nil {
			return "", err
		}
		finished = state.Finished
	}
	s.syncIDMap[sourceSync.GetId()] = destinationSyncID
	if finished {
		return destinationSyncID, nil
	}

	beforeTerminal := func() error {
		return s.preserveSupportsDiffMarkers(ctx, src, dst)
	}
	if err := s.processLedgerWork(ctx, src, ledger, sourceSync.GetId(), beforeTerminal); err != nil {
		return "", err
	}
	return destinationSyncID, nil
}

func listDestinationSyncs(ctx context.Context, dst connectorstore.Writer) ([]*c1zstore.SyncRun, error) {
	lister, ok := dst.(syncRunMetadataReader)
	if !ok {
		return nil, errors.New("c1zsanitize: destination cannot list sync metadata")
	}
	runs, next, err := lister.ListSyncRuns(ctx, "", 2)
	if err != nil {
		return nil, err
	}
	if next != "" {
		return nil, errors.New("c1zsanitize: destination contains more than one sync")
	}
	return runs, nil
}

func sanitizeWork(op string) c1zstore.LedgerWork {
	return c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: op}}}
}

func (s *sanitizer) ledgerConfig(sourceSync *reader_v2.SyncRun) (string, error) {
	allowUnknownAnnotations := !s.dropUnknownAnnotations
	config := ledgerSanitizeConfig{
		Version:                 sanitizePolicyV1,
		SecretFingerprint:       s.fingerprint,
		SourceFingerprint:       s.sourceFingerprint(sourceSync),
		Anchor:                  s.anchor.UTC().Format(time.RFC3339Nano),
		AllowUnknownAnnotations: &allowUnknownAnnotations,
	}
	value, err := json.Marshal(config)
	return string(value), err
}

func (s *sanitizer) validateLedgerConfig(
	ctx context.Context,
	ledger c1zstore.PageLedgerStore,
	sourceSync *reader_v2.SyncRun,
) error {
	facts, err := ledger.LedgerFacts(ctx)
	if err != nil {
		return err
	}
	raw, ok := facts[sanitizeConfigFact]
	if !ok {
		return errors.New("c1zsanitize: destination has no sanitizer configuration")
	}
	var config ledgerSanitizeConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return fmt.Errorf("c1zsanitize: invalid sanitizer configuration: %w", err)
	}
	if config.Version != sanitizePolicyV1 {
		return fmt.Errorf("c1zsanitize: unsupported sanitizer policy version %d", config.Version)
	}
	if config.SecretFingerprint != s.fingerprint {
		return errors.New("c1zsanitize: destination was created with a different secret")
	}
	if config.SourceFingerprint != s.sourceFingerprint(sourceSync) {
		return errors.New("c1zsanitize: destination was created from a different source sync")
	}
	if config.AllowUnknownAnnotations == nil {
		return errors.New("c1zsanitize: sanitizer configuration has no annotation policy")
	}
	if *config.AllowUnknownAnnotations != !s.dropUnknownAnnotations {
		return errors.New("c1zsanitize: destination was created with a different annotation policy")
	}
	anchor, err := time.Parse(time.RFC3339Nano, config.Anchor)
	if err != nil {
		return fmt.Errorf("c1zsanitize: invalid persisted anchor: %w", err)
	}
	if s.anchorExplicit && !s.anchor.Equal(anchor) {
		return errors.New("c1zsanitize: destination was created with a different timestamp anchor")
	}
	if !s.anchorExplicit && !s.anchor.Equal(anchor) {
		s.anchor = anchor
		s.shifter = newTimestampShifter(anchor, s.tMax)
	}
	return nil
}

func (s *sanitizer) sourceFingerprint(sourceSync *reader_v2.SyncRun) string {
	value := fmt.Sprintf(
		"%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		sourceSync.GetId(),
		sourceSync.GetSyncType(),
		sourceSync.GetParentSyncId(),
		sanitizeTimestamp(sourceSync.GetStartedAt()),
		sanitizeTimestamp(sourceSync.GetEndedAt()),
		sourceSync.GetSyncToken(),
		s.tMax.UTC().Format(time.RFC3339Nano),
	)
	return s.fingerprintFor("c1zsanitize-source-v1", value)
}

func sanitizeTimestamp(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

func (s *sanitizer) fingerprintFor(domain, value string) string {
	h := s.hmacPool.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(value))
	sum := h.Sum(nil)
	s.hmacPool.Put(h)
	return idEncoding.EncodeToString(sum)
}

func (s *sanitizer) loadKnownResourceTypes(ctx context.Context, src connectorstore.Reader, syncID string) error {
	s.knownResourceTypes = make(map[string]struct{})
	token := ""
	for {
		response, err := src.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize: listPageSize, PageToken: token, Annotations: syncIDAnnotations(syncID),
		}.Build())
		if err != nil {
			return err
		}
		for _, resourceType := range response.GetList() {
			if id := resourceType.GetId(); id != "" {
				s.knownResourceTypes[id] = struct{}{}
			}
		}
		token = response.GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

func (s *sanitizer) processLedgerWork(
	ctx context.Context,
	src connectorstore.Reader,
	ledger c1zstore.PageLedgerStore,
	syncID string,
	beforeTerminal func() error,
) error {
	attempt := ksuid.New().String()
	cache := newGrantSubCache(s.verifyGrantCache)
	resourceTypesLoaded := false
	for {
		pending, phase, err := ledger.PendingWork(ctx, 0, 2)
		if err != nil {
			return err
		}
		switch phase {
		case c1zstore.LedgerQueueSealing:
			facts, err := ledger.LedgerFacts(ctx)
			if err != nil {
				return err
			}
			counters, err := ledger.LedgerCounters(ctx)
			if err != nil {
				return err
			}
			return ledger.Seal(ctx, c1zstore.LedgerSyncStats(facts, counters))
		case c1zstore.LedgerQueueCollecting:
			if !resourceTypesLoaded {
				if err := s.loadKnownResourceTypes(ctx, src, syncID); err != nil {
					return err
				}
				resourceTypesLoaded = true
			}
		case c1zstore.LedgerQueueAbsent:
			return errors.New("c1zsanitize: sanitizer ledger disappeared")
		default:
			return fmt.Errorf("c1zsanitize: unsupported ledger phase %s", phase)
		}
		if len(pending) == 0 {
			if err := beforeTerminal(); err != nil {
				return err
			}
			writer := ledger.BeginPage()
			if err := writer.SetTerminal(); err != nil {
				writer.Discard()
				return err
			}
			id := c1zstore.LedgerActionIdentity{Op: sanitizeTerminalOp}
			err := writer.Commit(ctx, id, &c1zstore.LedgerRow{Identity: id, Attempt: attempt})
			writer.Discard()
			if err != nil {
				return err
			}
			continue
		}
		if len(pending) != 1 {
			return errors.New("c1zsanitize: sanitizer ledger contains concurrent work")
		}
		if err := s.processSanitizePage(ctx, src, ledger, pending[0], syncID, attempt, cache); err != nil {
			return err
		}
	}
}

func (s *sanitizer) processSanitizePage(
	ctx context.Context,
	src connectorstore.Reader,
	ledger c1zstore.PageLedgerStore,
	work c1zstore.LedgerWork,
	syncID string,
	attempt string,
	cache *grantSubCache,
) error {
	writer, ok := ledger.BeginPage().(c1zstore.TrustedImportPageWriter)
	if !ok {
		return errors.New("c1zsanitize: destination does not support trusted import pages")
	}
	defer writer.Discard()
	if err := writer.SetTrustedImport(); err != nil {
		return err
	}
	if err := writer.SetPendingWork(work); err != nil {
		return err
	}

	refs := newAssetRefSet()
	var next string
	switch work.Action.Identity.Op {
	case sanitizeResourceTypesOp:
		response, err := src.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize: listPageSize, PageToken: work.Action.Identity.PageToken, Annotations: syncIDAnnotations(syncID),
		}.Build())
		if err != nil {
			return err
		}
		rows := response.GetList()
		out := make([]*v2.ResourceType, len(rows))
		parallelTransform(len(rows), func(i int) { out[i] = s.transformResourceType(rows[i], refs) })
		if err := writer.PutResourceTypes(ctx, out...); err != nil {
			return err
		}
		next = response.GetNextPageToken()
	case sanitizeResourcesOp:
		response, err := src.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageSize: listPageSize, PageToken: work.Action.Identity.PageToken, Annotations: syncIDAnnotations(syncID),
		}.Build())
		if err != nil {
			return err
		}
		rows := response.GetList()
		out := make([]*v2.Resource, len(rows))
		parallelTransform(len(rows), func(i int) { out[i] = s.transformResource(rows[i], refs) })
		if err := writer.PutResources(ctx, out...); err != nil {
			return err
		}
		next = response.GetNextPageToken()
	case sanitizeEntitlementsOp:
		response, err := src.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize: listPageSize, PageToken: work.Action.Identity.PageToken, Annotations: syncIDAnnotations(syncID),
		}.Build())
		if err != nil {
			return err
		}
		rows := response.GetList()
		out := make([]*v2.Entitlement, len(rows))
		parallelTransform(len(rows), func(i int) { out[i] = s.transformEntitlement(rows[i], refs) })
		if err := writer.PutEntitlements(ctx, out...); err != nil {
			return err
		}
		next = response.GetNextPageToken()
	case sanitizeGrantsOp:
		list := src.ListGrants
		if expansion, ok := src.(connectorstore.ExpansionGrantLister); ok {
			list = expansion.ListGrantsWithExpansion
		}
		response, err := list(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize: listPageSize, PageToken: work.Action.Identity.PageToken, Annotations: syncIDAnnotations(syncID),
		}.Build())
		if err != nil {
			return err
		}
		rows := response.GetList()
		out := make([]*v2.Grant, len(rows))
		parallelTransform(len(rows), func(i int) { out[i] = s.transformGrant(rows[i], refs, cache) })
		if err := writer.PutGrants(ctx, out...); err != nil {
			return err
		}
		next = response.GetNextPageToken()
	default:
		return fmt.Errorf("c1zsanitize: unknown pending operation %q", work.Action.Identity.Op)
	}
	if err := s.stageAssets(ctx, src, writer, refs); err != nil {
		return err
	}

	row := &c1zstore.LedgerRow{Identity: work.Action.Identity, Attempt: attempt, NextPageToken: next}
	if next == "" {
		if child := nextSanitizeOp(work.Action.Identity.Op); child != "" {
			row.Children = []c1zstore.LedgerChild{{Identity: c1zstore.LedgerActionIdentity{Op: child}}}
		}
	}
	return writer.Commit(ctx, work.Action.Identity, row)
}

func nextSanitizeOp(op string) string {
	switch op {
	case sanitizeResourceTypesOp:
		return sanitizeResourcesOp
	case sanitizeResourcesOp:
		return sanitizeEntitlementsOp
	case sanitizeEntitlementsOp:
		return sanitizeGrantsOp
	default:
		return ""
	}
}

func (s *sanitizer) stageAssets(
	ctx context.Context,
	src connectorstore.Reader,
	writer c1zstore.PageWriter,
	refs *assetRefSet,
) error {
	for _, sourceID := range refs.drain() {
		contentType, reader, err := src.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
			Asset: v2.AssetRef_builder{Id: sourceID}.Build(),
		}.Build())
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return err
			}
			s.statsMu.Lock()
			s.missingAssets++
			s.statsMu.Unlock()
			continue
		}
		if err := closeIfCloser(reader); err != nil {
			return err
		}
		if err := writer.PutAsset(
			ctx,
			v2.AssetRef_builder{Id: s.id(sourceID)}.Build(),
			contentType,
			placeholderForContentType(contentType),
		); err != nil {
			return err
		}
	}
	return nil
}
