package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

func WithLedgerDebug(enabled bool) SyncOpt {
	return func(s *syncer) { s.cfg.ledgerDebug = enabled }
}

func (s *syncer) configureLedgerReport(ctx context.Context) error {
	logger := ctxzap.Extract(ctx)
	s.cfg.ledgerDebug = s.cfg.ledgerDebug || logger.Core().Enabled(zap.DebugLevel)
	if s.cfg.retainLedgerTokens && !s.cfg.ledgerDebug {
		return errors.New("retaining ledger tokens requires ledger debug mode")
	}
	if s.cfg.ledgerDebug {
		logger.Warn("ledger debug mode retains page history and performs additional reference lookups")
	}
	if s.cfg.retainLedgerTokens {
		logger.Warn("ledger tokens will be retained and may contain credentials")
	}
	return nil
}

func (s *syncer) finishLedgerReport(ctx context.Context) {
	logger := ctxzap.Extract(ctx)
	report, err := s.caps.pageLedger.ArchiveLedgerReport(ctx)
	if err != nil {
		logger.Warn("failed to save ledger report; retaining page history", zap.Error(err))
		return
	}
	logger.Info("sync ledger stats", zap.Reflect("ledger_stats", json.RawMessage(report)))
	if s.cfg.ledgerDebug {
		var summary struct {
			Latest struct {
				References struct {
					MissingChildren      uint64 `json:"missing_child_references"`
					MissingContinuations uint64 `json:"missing_continuation_references"`
					IdentityMismatches   uint64 `json:"identity_mismatches"`
					Uncheckable          uint64 `json:"uncheckable_references"`
				} `json:"reference_checks"`
			} `json:"latest"`
		}
		if err := json.Unmarshal(report, &summary); err != nil {
			logger.Warn("failed to read saved ledger reference checks; retaining page history", zap.Error(err))
			return
		}
		checks := summary.Latest.References
		if checks.MissingChildren+checks.MissingContinuations+checks.IdentityMismatches+checks.Uncheckable != 0 {
			logger.Warn("ledger reference checks found unresolved references; retaining page history", zap.Any("reference_checks", checks))
		}
		return
	}
	if err := s.caps.pageLedger.DropLedger(ctx); err != nil {
		logger.Warn("failed to delete page history; saved ledger report remains available", zap.Error(err))
	}
}
