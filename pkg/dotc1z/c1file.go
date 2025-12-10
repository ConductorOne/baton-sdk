package dotc1z

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// NOTE: required to register the dialect for goqu.
	//
	// If you remove this import, goqu.Dialect("sqlite3") will
	// return a copy of the default dialect, which is not what we want,
	// and allocates a ton of memory.
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"

	_ "github.com/glebarez/go-sqlite"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type pragma struct {
	name  string
	value string
}

type C1File struct {
	rawDb          *sql.DB
	db             *goqu.Database
	currentSyncID  string
	viewSyncID     string
	outputFilePath string
	dbFilePath     string
	dbUpdated      bool
	tempDir        string
	pragmas        []pragma

	// Slow query tracking
	slowQueryLogTimes     map[string]time.Time
	slowQueryLogTimesMu   sync.Mutex
	slowQueryThreshold    time.Duration
	slowQueryLogFrequency time.Duration
}

var _ connectorstore.Writer = (*C1File)(nil)

type C1FOption func(*C1File)

func WithC1FTmpDir(tempDir string) C1FOption {
	return func(o *C1File) {
		o.tempDir = tempDir
	}
}

func WithC1FPragma(name string, value string) C1FOption {
	return func(o *C1File) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

// Returns a C1File instance for the given db filepath.
func NewC1File(ctx context.Context, dbFilePath string, opts ...C1FOption) (*C1File, error) {
	ctx, span := tracer.Start(ctx, "NewC1File")
	defer span.End()

	rawDB, err := sql.Open("sqlite", dbFilePath)
	if err != nil {
		return nil, err
	}

	db := goqu.New("sqlite3", rawDB)

	c1File := &C1File{
		rawDb:                 rawDB,
		db:                    db,
		dbFilePath:            dbFilePath,
		pragmas:               []pragma{},
		slowQueryLogTimes:     make(map[string]time.Time),
		slowQueryThreshold:    5 * time.Second,
		slowQueryLogFrequency: 1 * time.Minute,
	}

	for _, opt := range opts {
		opt(c1File)
	}

	err = c1File.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	err = c1File.init(ctx)
	if err != nil {
		return nil, err
	}

	return c1File, nil
}

type c1zOptions struct {
	tmpDir         string
	pragmas        []pragma
	decoderOptions []DecoderOption
}
type C1ZOption func(*c1zOptions)

func WithTmpDir(tmpDir string) C1ZOption {
	return func(o *c1zOptions) {
		o.tmpDir = tmpDir
	}
}

func WithPragma(name string, value string) C1ZOption {
	return func(o *c1zOptions) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithDecoderOptions(opts ...DecoderOption) C1ZOption {
	return func(o *c1zOptions) {
		o.decoderOptions = opts
	}
}

// Returns a new C1File instance with its state stored at the provided filename.
func NewC1ZFile(ctx context.Context, outputFilePath string, opts ...C1ZOption) (*C1File, error) {
	ctx, span := tracer.Start(ctx, "NewC1ZFile")
	defer span.End()

	options := &c1zOptions{}
	for _, opt := range opts {
		opt(options)
	}

	dbFilePath, err := loadC1z(outputFilePath, options.tmpDir, options.decoderOptions...)
	if err != nil {
		return nil, err
	}

	var c1fopts []C1FOption
	for _, pragma := range options.pragmas {
		c1fopts = append(c1fopts, WithC1FPragma(pragma.name, pragma.value))
	}

	c1File, err := NewC1File(ctx, dbFilePath, c1fopts...)
	if err != nil {
		return nil, err
	}

	c1File.outputFilePath = outputFilePath

	return c1File, nil
}

func cleanupDbDir(dbFilePath string, err error) error {
	cleanupErr := os.RemoveAll(filepath.Dir(dbFilePath))
	if cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
	}
	return err
}

// Close ensures that the sqlite database is flushed to disk, and if any changes were made we update the original database
// with our changes.
func (c *C1File) Close() error {
	var err error

	if c.rawDb != nil {
		err = c.rawDb.Close()
		if err != nil {
			return cleanupDbDir(c.dbFilePath, err)
		}
	}
	c.rawDb = nil
	c.db = nil

	// We only want to save the file if we've made any changes
	if c.dbUpdated {
		err = saveC1z(c.dbFilePath, c.outputFilePath)
		if err != nil {
			return cleanupDbDir(c.dbFilePath, err)
		}
	}

	return cleanupDbDir(c.dbFilePath, err)
}

// init ensures that the database has all of the required schema.
func (c *C1File) init(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.init")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	err = c.InitTables(ctx)
	if err != nil {
		return err
	}

	for _, pragma := range c.pragmas {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf("PRAGMA %s = %s", pragma.name, pragma.value))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *C1File) InitTables(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.InitTables")
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

	return nil
}

// Stats introspects the database and returns the count of objects for the given sync run.
// If syncId is empty, it will use the latest sync run of the given type.
func (c *C1File) Stats(ctx context.Context, syncType connectorstore.SyncType, syncId string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.Stats")
	defer span.End()

	counts := make(map[string]int64)

	var err error
	if syncId == "" {
		syncId, err = c.LatestSyncID(ctx, syncType)
		if err != nil {
			return nil, err
		}
	}
	resp, err := c.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: syncId}.Build())
	if err != nil {
		return nil, err
	}
	if resp == nil || !resp.HasSync() {
		return nil, status.Errorf(codes.NotFound, "sync '%s' not found", syncId)
	}
	sync := resp.GetSync()
	if syncType != connectorstore.SyncTypeAny && syncType != connectorstore.SyncType(sync.GetSyncType()) {
		return nil, status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncId, syncType)
	}
	syncType = connectorstore.SyncType(sync.GetSyncType())

	counts["resource_types"] = 0

	var rtStats []*v2.ResourceType
	pageToken := ""
	for {
		resp, err := c.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return nil, err
		}

		rtStats = append(rtStats, resp.GetList()...)

		if resp.GetNextPageToken() == "" {
			break
		}

		pageToken = resp.GetNextPageToken()
	}
	counts["resource_types"] = int64(len(rtStats))
	for _, rt := range rtStats {
		resourceCount, err := c.db.From(resources.Name()).
			Where(goqu.C("resource_type_id").Eq(rt.GetId())).
			Where(goqu.C("sync_id").Eq(syncId)).
			CountContext(ctx)
		if err != nil {
			return nil, err
		}
		counts[rt.GetId()] = resourceCount
	}

	if syncType != connectorstore.SyncTypeResourcesOnly {
		entitlementsCount, err := c.db.From(entitlements.Name()).
			Where(goqu.C("sync_id").Eq(syncId)).
			CountContext(ctx)
		if err != nil {
			return nil, err
		}
		counts["entitlements"] = entitlementsCount

		grantsCount, err := c.db.From(grants.Name()).
			Where(goqu.C("sync_id").Eq(syncId)).
			CountContext(ctx)
		if err != nil {
			return nil, err
		}
		counts["grants"] = grantsCount
	}

	return counts, nil
}

// validateDb ensures that the database has been opened.
func (c *C1File) validateDb(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("c1file: datbase has not been opened")
	}

	return nil
}

// validateSyncDb ensures that there is a sync currently running, and that the database has been opened.
func (c *C1File) validateSyncDb(ctx context.Context) error {
	if c.currentSyncID == "" {
		return fmt.Errorf("c1file: sync is not active")
	}

	return c.validateDb(ctx)
}

func (c *C1File) OutputFilepath() (string, error) {
	if c.outputFilePath == "" {
		return "", fmt.Errorf("c1file: output file path is empty")
	}
	return c.outputFilePath, nil
}

func (c *C1File) AttachFile(other *C1File, dbName string) (*C1FileAttached, error) {
	_, err := c.db.Exec(`ATTACH DATABASE ? AS ?`, other.dbFilePath, dbName)
	if err != nil {
		return nil, err
	}

	return &C1FileAttached{
		safe: true,
		file: c,
	}, nil
}

func (c *C1FileAttached) DetachFile(dbName string) (*C1FileAttached, error) {
	_, err := c.file.db.Exec(`DETACH DATABASE ?`, dbName)
	if err != nil {
		return nil, err
	}

	return &C1FileAttached{
		safe: false,
		file: c.file,
	}, nil
}

// GrantStats introspects the database and returns the count of grants for the given sync run.
// If syncId is empty, it will use the latest sync run of the given type.
func (c *C1File) GrantStats(ctx context.Context, syncType connectorstore.SyncType, syncId string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.GrantStats")
	defer span.End()

	var err error
	if syncId == "" {
		syncId, err = c.LatestSyncID(ctx, syncType)
		if err != nil {
			return nil, err
		}
	} else {
		lastSync, err := c.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: syncId}.Build())
		if err != nil {
			return nil, err
		}
		if lastSync == nil {
			return nil, status.Errorf(codes.NotFound, "sync '%s' not found", syncId)
		}
		if syncType != connectorstore.SyncTypeAny && syncType != connectorstore.SyncType(lastSync.GetSync().GetSyncType()) {
			return nil, status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncId, syncType)
		}
	}

	var allResourceTypes []*v2.ResourceType
	pageToken := ""
	for {
		resp, err := c.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
		if err != nil {
			return nil, err
		}

		allResourceTypes = append(allResourceTypes, resp.GetList()...)

		if resp.GetNextPageToken() == "" {
			break
		}

		pageToken = resp.GetNextPageToken()
	}

	stats := make(map[string]int64)

	for _, resourceType := range allResourceTypes {
		grantsCount, err := c.db.From(grants.Name()).
			Where(goqu.C("sync_id").Eq(syncId)).
			Where(goqu.C("resource_type_id").Eq(resourceType.GetId())).
			CountContext(ctx)
		if err != nil {
			return nil, err
		}

		stats[resourceType.GetId()] = grantsCount
	}

	return stats, nil
}

type ExpandNeedsInsertPageToken struct {
	PrincipalTypeID string `json:"t,omitempty"`
	PrincipalID     string `json:"p,omitempty"`
}

func (c *C1File) ListPrincipalsNeedingExpandedGrant(
	ctx context.Context,
	sourceEntitlementID string,
	descendantEntitlementID string,
	resourceTypeIDs []string,
	shallow bool,
	pt *ExpandNeedsInsertPageToken,
) (principals []ExpandPrincipal, nextPageToken *ExpandNeedsInsertPageToken, err error) {
	if pt == nil {
		pt = &ExpandNeedsInsertPageToken{}
	}
	pageSize := 10000
	// Build resource type IDs JSON (or nil)
	var resourceTypeIDsJSON *string
	if len(resourceTypeIDs) > 0 {
		b, _ := json.Marshal(resourceTypeIDs)
		s := string(b)
		resourceTypeIDsJSON = &s
	}

	shallowInt := 0
	if shallow {
		shallowInt = 1
	}

	if pageSize <= 0 {
		pageSize = 10000
	}

	rows, err := c.db.QueryContext(ctx, `
        SELECT 
            src.principal_resource_type_id,
            src.principal_resource_id,
            src.data
        FROM v1_grants src
        WHERE src.entitlement_id = ?
          AND src.sync_id = ?
          AND (? IS NULL OR src.principal_resource_type_id IN (SELECT value FROM json_each(?)))
          AND (? = 0 OR json_type(src.sources, '$."' || ? || '"') IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1 FROM v1_grants dest
              WHERE dest.entitlement_id = ?
                AND dest.sync_id = ?
                AND dest.principal_resource_type_id = src.principal_resource_type_id
                AND dest.principal_resource_id = src.principal_resource_id
          )
          AND (? IS NULL OR (src.principal_resource_type_id, src.principal_resource_id) > (?, ?))
        ORDER BY src.principal_resource_type_id, src.principal_resource_id
        LIMIT ?
    `,
		sourceEntitlementID,
		c.currentSyncID,
		resourceTypeIDsJSON, resourceTypeIDsJSON,
		shallowInt, sourceEntitlementID,
		descendantEntitlementID,
		c.currentSyncID,
		nilIfEmpty(pt.PrincipalTypeID), pt.PrincipalTypeID, pt.PrincipalID,
		pageSize+1, // Fetch one extra to detect if there's more
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	principals = make([]ExpandPrincipal, 0, pageSize)
	var lastTypeID, lastID string

	for rows.Next() {
		var p ExpandPrincipal
		if err := rows.Scan(&p.TypeID, &p.ID, &p.SourceGrantData); err != nil {
			return nil, nil, err
		}
		principals = append(principals, p)
		lastTypeID, lastID = p.TypeID, p.ID
	}

	// If we got more than pageSize, there's another page
	if len(principals) > pageSize {
		principals = principals[:pageSize]
		lastTypeID = principals[pageSize-1].TypeID
		lastID = principals[pageSize-1].ID

		nextPT := ExpandNeedsInsertPageToken{
			PrincipalTypeID: lastTypeID,
			PrincipalID:     lastID,
		}
		nextPageToken = &nextPT
	}

	return principals, nextPageToken, nil
}

type ExpandPrincipal struct {
	TypeID          string
	ID              string
	SourceGrantData []byte // Contains the principal proto we need
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// InsertExpandedGrants inserts new grants for principals that have a source grant but no descendant grant.
// This is a SQL-only INSERT that creates grants with empty data blobs - the data is reconstructed
// during RectifyGrantSources.
func (c *C1File) InsertExpandedGrants(
	ctx context.Context,
	sourceEntitlementID string,
	descendantEntitlementID string,
	resourceTypeIDs []string,
	shallow bool,
) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.InsertExpandedGrants")
	defer span.End()

	// Build resource type IDs JSON (or nil)
	var resourceTypeIDsJSON *string
	if len(resourceTypeIDs) > 0 {
		b, _ := json.Marshal(resourceTypeIDs)
		s := string(b)
		resourceTypeIDsJSON = &s
	}

	shallowInt := 0
	if shallow {
		shallowInt = 1
	}

	// Get entitlement table and grants table names
	entTable := entitlements.Name()
	gTable := grants.Name()

	result, err := c.db.ExecContext(ctx, `
		INSERT INTO `+gTable+` (
			resource_type_id,
			resource_id,
			entitlement_id,
			principal_resource_type_id,
			principal_resource_id,
			external_id,
			data,
			sync_id,
			discovered_at,
			sources
		)
		SELECT
			e.resource_type_id,
			e.resource_id,
			?,
			src.principal_resource_type_id,
			src.principal_resource_id,
			? || ':' || src.principal_resource_type_id || ':' || src.principal_resource_id,
			X'',
			?,
			datetime('now'),
			json_object(?, json('{}'))
		FROM `+gTable+` src
		JOIN `+entTable+` e ON e.external_id = ? AND e.sync_id = ?
		WHERE src.entitlement_id = ?
		  AND src.sync_id = ?
		  -- Optional: filter by principal resource type
		  AND (? IS NULL 
			   OR src.principal_resource_type_id IN (SELECT value FROM json_each(?)))
		  -- Optional: shallow mode - source grant must have source_entitlement in its sources
		  AND (? = 0 
			   OR json_type(src.sources, '$."' || ? || '"') IS NOT NULL)
		  -- Principal has NO existing grant on descendant entitlement
		  AND NOT EXISTS (
			  SELECT 1 FROM `+gTable+` dest
			  WHERE dest.entitlement_id = ?
				AND dest.sync_id = ?
				AND dest.principal_resource_type_id = src.principal_resource_type_id
				AND dest.principal_resource_id = src.principal_resource_id
		  )
		ON CONFLICT (external_id, sync_id) DO NOTHING
	`,
		descendantEntitlementID,                  // entitlement_id
		descendantEntitlementID,                  // external_id prefix
		c.currentSyncID,                          // sync_id
		sourceEntitlementID,                      // sources json key
		descendantEntitlementID, c.currentSyncID, // JOIN entitlements
		sourceEntitlementID, c.currentSyncID, // WHERE src.entitlement_id
		resourceTypeIDsJSON, resourceTypeIDsJSON, // resource type filter
		shallowInt, sourceEntitlementID, // shallow filter
		descendantEntitlementID, c.currentSyncID, // NOT EXISTS subquery
	)
	if err != nil {
		return 0, fmt.Errorf("error inserting expanded grants: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return rowsAffected, nil
}

// UpdateExpandedGrantSources updates existing descendant grants to add the source entitlement to their sources.
// This handles the UPDATE case of grant expansion - when a principal already has a grant on the descendant
// entitlement, but needs the source entitlement added to its sources map.
func (c *C1File) UpdateExpandedGrantSources(
	ctx context.Context,
	sourceEntitlementID string,
	descendantEntitlementID string,
	resourceTypeIDs []string,
	shallow bool,
) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.UpdateExpandedGrantSources")
	defer span.End()

	// Build resource type IDs JSON (or nil)
	var resourceTypeIDsJSON *string
	if len(resourceTypeIDs) > 0 {
		b, _ := json.Marshal(resourceTypeIDs)
		s := string(b)
		resourceTypeIDsJSON = &s
	}

	shallowInt := 0
	if shallow {
		shallowInt = 1
	}

	result, err := c.db.ExecContext(ctx, `
		UPDATE `+grants.Name()+`
		SET sources = CASE
			-- If sources is null/empty, add both descendant_entitlement_id and source_entitlement_id
			WHEN json_type(sources) = 'null' 
				 OR sources IS NULL 
				 OR sources = '{}'
			THEN json_set(
				json_set('{}', '$."' || ? || '"', json('{}')),
				'$."' || ? || '"',
				json('{}')
			)
			-- Otherwise just add source_entitlement_id (if not already present)
			WHEN json_type(sources, '$."' || ? || '"') IS NULL
			THEN json_set(
				sources,
				'$."' || ? || '"',
				json('{}')
			)
			-- Already has this source, keep unchanged
			ELSE sources
		END
		WHERE entitlement_id = ?
		  AND sync_id = ?
		  -- Only for principals that have a source grant
		  AND (principal_resource_type_id, principal_resource_id) IN (
			  SELECT src.principal_resource_type_id, src.principal_resource_id
			  FROM `+grants.Name()+` src
			  WHERE src.entitlement_id = ?
				AND src.sync_id = ?
				-- Optional: filter by principal resource type
				AND (? IS NULL 
					 OR src.principal_resource_type_id IN (SELECT value FROM json_each(?)))
				-- Optional: shallow mode - source grant must have source_entitlement in its sources
				AND (? = 0 
					 OR json_type(src.sources, '$."' || ? || '"') IS NOT NULL)
		  )
	`,
		descendantEntitlementID, sourceEntitlementID, // CASE: empty sources
		sourceEntitlementID, sourceEntitlementID, // CASE: add source if not present
		descendantEntitlementID, c.currentSyncID, // WHERE clause
		sourceEntitlementID, c.currentSyncID, // subquery
		resourceTypeIDsJSON, resourceTypeIDsJSON, // resource type filter
		shallowInt, sourceEntitlementID, // shallow filter
	)
	if err != nil {
		return 0, fmt.Errorf("error updating expanded grant sources: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return rowsAffected, nil
}
