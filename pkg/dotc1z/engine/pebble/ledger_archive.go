package pebble

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type ledgerRecoveryState struct {
	Facts    map[string]string       `json:"facts"`
	Counters c1zstore.LedgerCounters `json:"counters"`
}

type ledgerArchive struct {
	Version          int             `json:"version"`
	SyncID           string          `json:"sync_id"`
	CollectionReport json.RawMessage `json:"collection_report,omitempty"`
	CollectionSyncID string          `json:"collection_sync_id,omitempty"`
	Report           json.RawMessage `json:"report"`
	ledgerRecoveryState
}

func ledgerArchiveKey() []byte {
	return codec.AppendTupleStrings([]byte{versionV3, typeEngineMeta}, "ledger-archive")
}

func (e *Engine) ArchiveLedgerReport(ctx context.Context) ([]byte, error) {
	var result []byte
	err := e.withWriteAllowSealed(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.IsSealed() {
			return errors.New("ledger archive requires a sealed sync")
		}
		record, err := e.LatestFinishedSyncRecord(ctx, func(v3.SyncType) bool { return true })
		if err != nil {
			return err
		}
		if record == nil {
			return errors.New("ledger archive requires a finished sync")
		}
		result, err = e.archiveLedgerReportLocked(ctx, record.GetSyncId())
		return err
	})
	return result, err
}

func (e *Engine) archiveLedgerReportLocked(ctx context.Context, syncID string) ([]byte, error) {
	lo, hi := rawdb.LedgerRowBounds()
	rows, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	present := rows.First()
	err = errors.Join(rows.Error(), rows.Close())
	if err != nil {
		return nil, err
	}
	if !present {
		prior, err := e.readLedgerArchive(ctx)
		if err != nil {
			return nil, err
		}
		if prior != nil && prior.SyncID == syncID {
			return renderLedgerArchive(prior)
		}
	}
	report, err := e.GenerateLedgerReport(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		ctxzap.Extract(ctx).Warn("failed to generate ledger report; saving unavailable status", zap.Error(err))
		report = []byte(`{"status":"unavailable","reason":"report_generation_failed"}`)
	}
	facts, err := e.Ledger().Facts(ctx)
	if err != nil {
		return nil, err
	}
	counters, err := e.Ledger().Counters(ctx)
	if err != nil {
		return nil, err
	}
	archive := ledgerArchive{Version: 1, SyncID: syncID, Report: report, ledgerRecoveryState: ledgerRecoveryState{Facts: facts, Counters: counters}}
	var options c1zstore.LedgerReportOptions
	if value := facts[c1zstore.LedgerFactReportOptions]; value != "" {
		if err := json.Unmarshal([]byte(value), &options); err != nil {
			options = c1zstore.LedgerReportOptions{}
		}
	}
	if options.Requested.OnlyExpandGrants {
		prior, err := e.readLedgerArchive(ctx)
		if err != nil {
			return nil, err
		}
		if prior != nil {
			archive.CollectionReport, archive.CollectionSyncID = prior.CollectionReport, prior.CollectionSyncID
			if len(archive.CollectionReport) == 0 && prior.Facts[c1zstore.LedgerFactReportOptions] != facts[c1zstore.LedgerFactReportOptions] {
				archive.CollectionReport, archive.CollectionSyncID = prior.Report, prior.SyncID
			}
		}
	}
	result, err := renderLedgerArchive(&archive)
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(archive)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if hook := e.test.ledgerArchiveHook; hook != nil {
		if err := hook("before-write"); err != nil {
			return nil, err
		}
	}
	if err := e.db.MetaSet(ledgerArchiveKey(), value, pebble.Sync); err != nil {
		return nil, err
	}
	if hook := e.test.ledgerArchiveHook; hook != nil {
		return result, hook("after-write")
	}
	return result, nil
}

func (e *Engine) readLedgerArchive(ctx context.Context) (*ledgerArchive, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, closer, err := e.db.Get(ledgerArchiveKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var archive ledgerArchive
	if err := json.Unmarshal(value, &archive); err != nil {
		return nil, err
	}
	if archive.Version != 1 || archive.SyncID == "" || !json.Valid(archive.Report) {
		return nil, errors.New("invalid ledger archive")
	}
	return &archive, nil
}

func (e *Engine) GetArchivedLedgerReport(ctx context.Context) ([]byte, error) {
	archive, err := e.readLedgerArchive(ctx)
	if err != nil || archive == nil {
		return nil, err
	}
	return renderLedgerArchive(archive)
}

func renderLedgerArchive(archive *ledgerArchive) ([]byte, error) {
	return json.Marshal(struct {
		Version          int             `json:"schema_version"`
		SyncID           string          `json:"sync_id"`
		Latest           json.RawMessage `json:"latest"`
		Collection       json.RawMessage `json:"preceding_collection,omitempty"`
		CollectionSyncID string          `json:"preceding_collection_sync_id,omitempty"`
	}{1, archive.SyncID, bytes.Clone(archive.Report), archive.CollectionReport, archive.CollectionSyncID})
}

func (e *Engine) GetArchivedLedgerOptions(ctx context.Context, attempt string) (*c1zstore.LedgerReportOptions, error) {
	archive, err := e.readLedgerArchive(ctx)
	if err != nil || archive == nil {
		return nil, err
	}
	for _, key := range []string{c1zstore.LedgerFactReportOptions, c1zstore.LedgerFactFirstReportOptions} {
		value, found := archive.Facts[key]
		if !found {
			continue
		}
		var options c1zstore.LedgerReportOptions
		if err := json.Unmarshal([]byte(value), &options); err != nil {
			return nil, err
		}
		if attempt == "" || options.Attempt == attempt {
			return &options, nil
		}
	}
	return nil, nil
}

func (e *Engine) RestoreLedgerArchive(ctx context.Context) error {
	return e.withWrite(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		finished, err := e.BoundSyncFinished(ctx)
		if err != nil {
			return err
		}
		lo, hi := rawdb.LedgerBounds()
		iterator, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		if err != nil {
			return err
		}
		present := iterator.First()
		pendingOnly := present && bytes.Equal(iterator.Key(), encodeLedgerFactKey(c1zstore.LedgerFactDiscardOnSeal))
		if pendingOnly {
			present = iterator.Next()
		}
		err = errors.Join(iterator.Error(), iterator.Close())
		if err != nil {
			return err
		}
		if present {
			return nil
		}
		if pendingOnly {
			finished = false
		}
		archive, err := e.readLedgerArchive(ctx)
		if err != nil || archive == nil {
			return err
		}
		if !finished {
			_, pendingDiscard := archive.Facts[c1zstore.LedgerFactDiscardOnSeal]
			if archive.SyncID != e.CurrentSyncID() || !pendingDiscard {
				return nil
			}
		}
		if archive.SyncID != e.CurrentSyncID() {
			record, err := e.GetSyncRunRecord(ctx, e.CurrentSyncID())
			if err != nil {
				return err
			}
			if !record.GetCompacted() {
				return errors.New("ledger archive belongs to another sync")
			}
		}
		counters, err := marshalRecord(ledgerCountersToProto(archive.Counters))
		if err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		for key, value := range archive.Facts {
			if err := batch.StageLedgerFactValue(encodeLedgerFactKey(key), value); err != nil {
				return err
			}
		}
		if err := batch.StageLedgerCounterBucket(encodeLedgerCounterKey("archived", c1zstore.TakeoverBucketWorker), counters); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.Ledger().markInFlightLocked(); err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	})
}
