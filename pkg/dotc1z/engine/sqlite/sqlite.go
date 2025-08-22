package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"go.opentelemetry.io/otel"

	// NOTE: required to register the dialect for goqu.
	//
	// If you remove this import, goqu.Dialect("sqlite3") will
	// return a copy of the default dialect, which is not what we want,
	// and allocates a ton of memory.
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/glebarez/go-sqlite"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.backends.sqlite")

type pragma struct {
	name  string
	value string
}

var _ engine.StorageEngine = (*SQLite)(nil)

type SQLite struct {
	rawDb         *sql.DB
	db            *goqu.Database
	currentSyncID string
	viewSyncID    string
	dbFilePath    string
	dbUpdated     bool
	workingDir    string
	pragmas       []pragma

	// Slow query tracking
	slowQueryLogTimes     map[string]time.Time
	slowQueryLogTimesMu   sync.Mutex
	slowQueryThreshold    time.Duration
	slowQueryLogFrequency time.Duration
}

type SQLiteOption func(*SQLite)

func WithPragma(name string, value string) SQLiteOption {
	return func(o *SQLite) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func NewSQLite(ctx context.Context, workingDir string, opts ...SQLiteOption) (*SQLite, error) {
	ctx, span := tracer.Start(ctx, "NewSQLite")
	defer span.End()

	err := os.MkdirAll(workingDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("c1z: sqlite: could not create working directory: %w", err)
	}

	dbFilePath := filepath.Join(workingDir, "db")
	rawDB, err := sql.Open("sqlite", dbFilePath)
	if err != nil {
		return nil, err
	}

	db := goqu.New("sqlite3", rawDB)

	d := &SQLite{
		rawDb:                 rawDB,
		db:                    db,
		workingDir:            workingDir,
		dbFilePath:            dbFilePath,
		slowQueryLogTimes:     make(map[string]time.Time),
		slowQueryThreshold:    5 * time.Second,
		slowQueryLogFrequency: 1 * time.Minute,
	}

	for _, opt := range opts {
		opt(d)
	}

	err = d.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	err = d.init(ctx)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (c *SQLite) Dirty() bool {
	return c.dbUpdated
}

// Close ensures that the sqlite database is flushed to disk, and if any changes were made we update the original database
// with our changes.
func (c *SQLite) Close() error {
	var err error

	if c.rawDb != nil {
		err = c.rawDb.Close()
		if err != nil {
			return err
		}
	}
	c.rawDb = nil
	c.db = nil
	return nil
}

// init ensures that the database has all of the required schema.
func (c *SQLite) init(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "SQLite.init")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	for _, t := range allTableDescriptors {
		query, args := t.Schema()
		_, err = c.db.ExecContext(ctx, fmt.Sprintf(query, args...))
		if err != nil {
			return err
		}
		err = t.Migrations(ctx, c.db)
		if err != nil {
			return err
		}
	}

	for _, pragma := range c.pragmas {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf("PRAGMA %s = %s", pragma.name, pragma.value))
		if err != nil {
			return err
		}
	}

	return nil
}

// Stats introspects the database and returns the count of objects for the given sync run.
func (c *SQLite) Stats(ctx context.Context) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "SQLite.Stats")
	defer span.End()

	counts := make(map[string]int64)

	syncID, err := c.LatestSyncID(ctx)
	if err != nil {
		return nil, err
	}

	counts["resource_types"] = 0

	var rtStats []*v2.ResourceType
	pageToken := ""
	for {
		resp, err := c.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageToken: pageToken})
		if err != nil {
			return nil, err
		}

		rtStats = append(rtStats, resp.List...)

		if resp.NextPageToken == "" {
			break
		}

		pageToken = resp.NextPageToken
	}
	counts["resource_types"] = int64(len(rtStats))
	for _, rt := range rtStats {
		resourceCount, err := c.db.From(resources.Name()).
			Where(goqu.C("resource_type_id").Eq(rt.Id)).
			Where(goqu.C("sync_id").Eq(syncID)).
			CountContext(ctx)
		if err != nil {
			return nil, err
		}
		counts[rt.Id] = resourceCount
	}

	entitlementsCount, err := c.db.From(entitlements.Name()).
		Where(goqu.C("sync_id").Eq(syncID)).
		CountContext(ctx)
	if err != nil {
		return nil, err
	}
	counts["entitlements"] = entitlementsCount

	grantsCount, err := c.db.From(grants.Name()).
		Where(goqu.C("sync_id").Eq(syncID)).
		CountContext(ctx)
	if err != nil {
		return nil, err
	}

	counts["grants"] = grantsCount

	return counts, nil
}

// validateDb ensures that the database has been opened.
func (c *SQLite) validateDb(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("SQLite: datbase has not been opened")
	}

	return nil
}

// validateSyncDb ensures that there is a sync currently running, and that the database has been opened.
func (c *SQLite) validateSyncDb(ctx context.Context) error {
	if c.currentSyncID == "" {
		return fmt.Errorf("SQLite: sync is not active")
	}

	return c.validateDb(ctx)
}

func (c *SQLite) OutputFilepath() (string, error) {
	return "", fmt.Errorf("sqlite engine does not have an output path")
}
