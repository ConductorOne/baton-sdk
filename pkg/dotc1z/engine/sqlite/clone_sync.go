package sqlite

import (
	"context"
	"fmt"
	"os"
	"strings"
)

func cloneTableQuery(tableName string) (string, error) {
	var sb strings.Builder
	var err error

	_, err = sb.WriteString("INSERT INTO clone.")
	if err != nil {
		return "", err
	}

	_, err = sb.WriteString(tableName)
	if err != nil {
		return "", err
	}

	_, err = sb.WriteString(" SELECT * FROM ")
	if err != nil {
		return "", err
	}

	_, err = sb.WriteString(tableName)
	if err != nil {
		return "", err
	}

	_, err = sb.WriteString(" WHERE sync_id=?")
	if err != nil {
		return "", err
	}

	return sb.String(), nil
}

// CloneSync uses sqlite hackery to directly copy the pertinent rows into a new database.
// 1. Create a new empty sqlite database in a directory in the working dir (clone-<sync_id>-<random>/db)
// 2. Open the c1z that we are cloning to get a db handle
// 3. Execute an ATTACH query to bring our empty sqlite db into the context of our db connection
// 4. Select directly from the cloned db and insert directly into the new database.
// 5. Close and save the new database and return the path.
// 6. It is the callers responsibility to clean up the cloned db/directory.
func (c *SQLite) CloneSync(ctx context.Context, syncID string) (string, error) {
	ctx, span := tracer.Start(ctx, "SQLite.CloneSync")
	defer span.End()

	tmpDir, err := os.MkdirTemp(c.workingDir, fmt.Sprintf("clone-%s-*", syncID))
	if err != nil {
		return "", err
	}

	out, err := NewSQLite(ctx, tmpDir)
	if err != nil {
		return "", err
	}
	defer out.Close()

	err = out.init(ctx)
	if err != nil {
		return "", err
	}

	if syncID == "" {
		syncID, err = c.LatestSyncID(ctx)
		if err != nil {
			return "", err
		}
	}

	sync, err := c.getSync(ctx, syncID)
	if err != nil {
		return "", err
	}

	if sync == nil {
		return "", fmt.Errorf("clone-sync: sync not found")
	}

	if sync.EndedAt == nil {
		return "", fmt.Errorf("clone-sync: sync is not ended")
	}

	qCtx, canc := context.WithCancel(ctx)
	defer canc()

	// Get a single connection to the current db so we can make multiple queries in the same session
	conn, err := c.rawDb.Conn(qCtx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	_, err = conn.ExecContext(qCtx, fmt.Sprintf(`ATTACH '%s' AS clone`, out.dbFilePath))
	if err != nil {
		return "", err
	}

	for _, t := range allTableDescriptors {
		q, err := cloneTableQuery(t.Name())
		if err != nil {
			return "", err
		}
		_, err = conn.ExecContext(qCtx, q, syncID)
		if err != nil {
			return "", err
		}
	}

	// Really be sure that our connection is closed and the db won't be mutated
	canc()
	_ = conn.Close()

	return out.dbFilePath, err
}
