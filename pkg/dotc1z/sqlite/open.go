package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	_ "modernc.org/sqlite"
)

const sqliteMagic = "SQLite format 3\x00"

type openConfig struct {
	tmpDir         string
	decoderOptions []dotc1z.DecoderOption
}

// OpenOption configures Open.
type OpenOption func(*openConfig)

// WithTmpDir sets the directory used for a decoded v1 c1z database.
func WithTmpDir(dir string) OpenOption {
	return func(c *openConfig) {
		c.tmpDir = dir
	}
}

// WithDecoderOptions passes options to the v1 c1z decoder.
func WithDecoderOptions(opts ...dotc1z.DecoderOption) OpenOption {
	return func(c *openConfig) {
		c.decoderOptions = append(c.decoderOptions, opts...)
	}
}

// File is an open read-only v1 SQLite source.
type File struct {
	db        *sql.DB
	dbPath    string
	tempDir   string
	columns   map[string]map[string]bool
	hasTables map[string]bool
}

// Open opens a v1 c1z envelope or bare SQLite database read-only.
func Open(ctx context.Context, path string, opts ...OpenOption) (_ *File, err error) {
	cfg := &openConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	source, err := os.Open(path) // #nosec G304 -- path is the caller-selected source file.
	if err != nil {
		return nil, err
	}
	sourceOpen := true
	defer func() {
		if sourceOpen {
			err = errors.Join(err, source.Close())
		}
	}()

	header := make([]byte, len(sqliteMagic))
	n, readErr := io.ReadFull(source, header)
	if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("sqlite: read source header: %w", readErr)
	}

	dbPath := path
	tempDir := ""
	if n != len(sqliteMagic) || !bytes.Equal(header, []byte(sqliteMagic)) {
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("sqlite: rewind source: %w", err)
		}
		format, err := dotc1z.ReadHeaderFormat(source)
		if err != nil {
			return nil, fmt.Errorf("sqlite: read c1z header: %w", err)
		}
		if format != dotc1z.C1ZFormatV1 {
			return nil, fmt.Errorf("sqlite: unsupported c1z format %s", format)
		}
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("sqlite: rewind c1z source: %w", err)
		}
		tempDir, err = os.MkdirTemp(cfg.tmpDir, "c1z-sqlite-")
		if err != nil {
			return nil, fmt.Errorf("sqlite: create decode directory: %w", err)
		}
		defer func() {
			if err != nil {
				err = errors.Join(err, os.RemoveAll(tempDir))
			}
		}()
		dbPath = filepath.Join(tempDir, "db")
		decoded, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("sqlite: create decoded database: %w", err)
		}
		decoder, err := dotc1z.NewC1ZFileDecoder(source, append(cfg.decoderOptions, dotc1z.WithContext(ctx))...)
		if err != nil {
			_ = decoded.Close()
			return nil, fmt.Errorf("sqlite: create c1z decoder: %w", err)
		}
		_, copyErr := io.Copy(decoded, decoder)
		closeErr := errors.Join(decoder.Close(), decoded.Sync(), decoded.Close())
		if err := errors.Join(copyErr, closeErr); err != nil {
			return nil, fmt.Errorf("sqlite: decode c1z: %w", err)
		}
	}
	if err := source.Close(); err != nil {
		return nil, fmt.Errorf("sqlite: close source: %w", err)
	}
	sourceOpen = false

	db, err := openSQLite(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, db.Close())
		}
	}()

	columns, tables, err := validateSQLiteSchema(ctx, db)
	if err != nil {
		return nil, err
	}
	return &File{
		db:        db,
		dbPath:    dbPath,
		tempDir:   tempDir,
		columns:   columns,
		hasTables: tables,
	}, nil
}

// Close releases the source database and removes any decoded temporary file.
func (f *File) Close(context.Context) error {
	if f == nil {
		return nil
	}
	var err error
	if f.db != nil {
		err = f.db.Close()
		f.db = nil
	}
	if f.tempDir != "" {
		removeErr := os.RemoveAll(f.tempDir)
		err = errors.Join(err, removeErr)
		if removeErr == nil {
			f.tempDir = ""
		}
	}
	return err
}

func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	dsn, err := sqliteDSN(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping database: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func sqliteDSN(path string) (string, error) {
	if path == "" {
		return "", errors.New("sqlite: empty database path")
	}
	if strings.HasPrefix(path, "file:") {
		u, err := url.Parse(path)
		if err != nil {
			return "", fmt.Errorf("sqlite: invalid file URI: %w", err)
		}
		q := u.Query()
		q.Set("mode", "ro")
		q.Add("_pragma", "trusted_schema(OFF)")
		q.Add("_pragma", "query_only(ON)")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("sqlite: resolve database path: %w", err)
	}
	slashPath := filepath.ToSlash(abs)
	if !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := &url.URL{Scheme: "file", Path: slashPath}
	q := u.Query()
	q.Set("mode", "ro")
	q.Add("_pragma", "trusted_schema(OFF)")
	q.Add("_pragma", "query_only(ON)")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
