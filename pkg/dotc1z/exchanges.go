package dotc1z

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// ExchangeStore / Exchange re-exports so callers can use the dotc1z names,
// matching the convention used for GrantStore/SyncMeta/FileOps.
type (
	ExchangeStore = c1zstore.ExchangeStore
	Exchange      = c1zstore.Exchange
)

const exchangesTableVersion = "1"
const exchangesTableName = "exchanges"
const exchangesTableSchema = `
create table if not exists %s (
    id integer primary key,
    request_id text not null,
    sync_id text not null,
    transport_kind text not null,
    request_json blob,
    response_json blob,
    intent_json blob,
    discovered_at datetime not null
);
create unique index if not exists %s on %s (request_id, sync_id);`

var exchanges = (*exchangesTable)(nil)

type exchangesTable struct{}

func (r *exchangesTable) Name() string { return fmt.Sprintf("v%s_%s", r.Version(), exchangesTableName) }

func (r *exchangesTable) Version() string { return exchangesTableVersion }

func (r *exchangesTable) Schema() (string, []interface{}) {
	return exchangesTableSchema, []interface{}{
		r.Name(),
		fmt.Sprintf("idx_exchanges_request_sync_v%s", r.Version()),
		r.Name(),
	}
}

func (r *exchangesTable) Migrations(ctx context.Context, db *goqu.Database) (bool, error) {
	return false, nil
}

// ensureExchangesTable creates the exchanges table on demand. The exchanges
// store is a per-sync debug side-store, deliberately NOT registered in
// allTableDescriptors: it must not participate in the object sync-diff / clone
// machinery (which keys every table on external_id). Creating it lazily also
// gives the "old .c1z opens empty" property for free — a file written before
// provenance simply never has the table, and reads treat its absence as empty.
func (c *C1File) ensureExchangesTable(ctx context.Context) error {
	schema, args := exchanges.Schema()
	q := fmt.Sprintf(schema, args...)
	_, err := c.db.ExecContext(ctx, q)
	return err
}

// Exchanges returns the provenance exchange sub-store. Its presence is what
// makes *C1File satisfy c1zstore.ExchangesProvider.
func (c *C1File) Exchanges() ExchangeStore { return c1FileExchangeStore{c} }

// c1FileExchangeStore is a zero-allocation value wrapper adapting *C1File to
// ExchangeStore.
type c1FileExchangeStore struct{ c *C1File }

func (e c1FileExchangeStore) PutExchanges(ctx context.Context, items ...*Exchange) error {
	return e.c.putExchanges(ctx, items...)
}

func (e c1FileExchangeStore) GetExchange(ctx context.Context, requestID string) (*Exchange, error) {
	return e.c.getExchange(ctx, requestID)
}

func (e c1FileExchangeStore) ListExchangesForObject(ctx context.Context, requestIDs []string) ([]*Exchange, error) {
	return e.c.listExchangesForObject(ctx, requestIDs)
}

func (c *C1File) putExchanges(ctx context.Context, items ...*Exchange) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if len(items) == 0 {
		return nil
	}
	if err := c.validateSyncDb(ctx); err != nil {
		return err
	}
	if err := c.ensureExchangesTable(ctx); err != nil {
		return err
	}
	now := time.Now().Format("2006-01-02 15:04:05.999999999")
	for _, it := range items {
		if it == nil || it.RequestID == "" {
			continue
		}
		fields := goqu.Record{
			"request_id":     it.RequestID,
			"sync_id":        c.currentSyncID,
			"transport_kind": it.TransportKind,
			"request_json":   it.RequestJSON,
			"response_json":  it.ResponseJSON,
			"intent_json":    it.IntentJSON,
			"discovered_at":  now,
		}
		q := c.db.Insert(exchanges.Name()).Prepared(true).Rows(fields)
		q = q.OnConflict(goqu.DoUpdate("request_id, sync_id",
			goqu.Record{
				"response_json": goqu.I("EXCLUDED.response_json"),
				"request_json":  goqu.I("EXCLUDED.request_json"),
			}))
		query, args, err := q.ToSQL()
		if err != nil {
			return err
		}
		if _, err := c.db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	c.dbUpdated = true
	return nil
}

func (c *C1File) getExchange(ctx context.Context, requestID string) (*Exchange, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}
	q := c.db.From(exchanges.Name()).Prepared(true).
		Select("request_id", "sync_id", "transport_kind", "request_json", "response_json", "intent_json", "discovered_at").
		Where(goqu.C("request_id").Eq(requestID)).
		Order(goqu.C("id").Desc()).
		Limit(1)
	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}
	row := c.db.QueryRowContext(ctx, query, args...)
	out := &Exchange{}
	if err := row.Scan(&out.RequestID, &out.SyncID, &out.TransportKind, &out.RequestJSON, &out.ResponseJSON, &out.IntentJSON, &out.DiscoveredAt); err != nil {
		// Absent rows, or an absent table on a .c1z written before provenance,
		// both read as "no exchange".
		if err.Error() == "sql: no rows in result set" || strings.Contains(err.Error(), "no such table") {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

func (c *C1File) listExchangesForObject(ctx context.Context, requestIDs []string) ([]*Exchange, error) {
	out := make([]*Exchange, 0, len(requestIDs))
	for _, id := range requestIDs {
		ex, err := c.getExchange(ctx, id)
		if err != nil {
			return nil, err
		}
		if ex != nil {
			out = append(out, ex)
		}
	}
	return out, nil
}
