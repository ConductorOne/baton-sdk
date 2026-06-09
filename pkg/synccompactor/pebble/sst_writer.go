package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"

	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// sstBuilder accumulates sorted (key, value) pairs and writes them to an
// SST file at path. Keys must be added in strictly increasing order
// per pebble/sstable.Writer.Set semantics. The on-disk format is pinned
// to the SDK's pebble format major version so the destination engine
// can ingest without a format upgrade.
type sstBuilder struct {
	w        *sstable.Writer
	file     vfs.File
	path     string
	keyCount int
}

// newSSTBuilder opens path for write and returns a builder. The
// caller must call (*sstBuilder).Close() to finalize; closing flushes
// the table footer and closes the underlying file. Caller may delete
// the file on error via os.Remove.
func newSSTBuilder(path string) (*sstBuilder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("sstBuilder: mkdir: %w", err)
	}
	file, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, fmt.Errorf("sstBuilder: create %q: %w", path, err)
	}
	writable := objstorageprovider.NewFileWritable(file)
	// TableFormat must match what the destination engine ingests. We
	// derive it from the engine's pinned FormatMajorVersion via
	// MaxTableFormat — this guarantees the SST is readable by any
	// engine running the same or newer SDK release.
	w := sstable.NewWriter(writable, sstable.WriterOptions{
		TableFormat: enginepkg.SDKPebbleFormat.MaxTableFormat(),
		// 32 KiB blocks (was 4 KiB) cut per-block index/framing CPU when building
		// compaction SSTs over millions of grants. BenchmarkCompactionFlow at 2M
		// grants: -8.7% wall-time for +13% on-disk. 32K is the speed/space knee —
		// 64K/128K buy a few % more speed for sharply more storage (ZFS lz4
		// compresses larger Snappy blocks less, so on-disk grows fast). These SSTs
		// are write-once and read sequentially during IngestAndExcise, so the
		// larger block costs nothing on the read side. See OPS-1875.
		BlockSize: 32 << 10,
	})
	return &sstBuilder{
		w:    w,
		file: file,
		path: path,
	}, nil
}

// Set writes a key/value pair. Keys must arrive in strictly increasing
// order.
func (b *sstBuilder) Set(key, value []byte) error {
	if err := b.w.Set(key, value); err != nil {
		return fmt.Errorf("sstBuilder.Set: %w", err)
	}
	b.keyCount++
	return nil
}

// Close finalizes and closes the SST file. After Close, the path is
// safe to pass to pebble.DB.IngestAndExcise.
func (b *sstBuilder) Close() error {
	if b.w == nil {
		return nil
	}
	if err := b.w.Close(); err != nil {
		return fmt.Errorf("sstBuilder.Close: %w", err)
	}
	b.w = nil
	// sstable.Writer.Close closes the writable, which in turn closes
	// the underlying vfs.File — calling file.Close again would be a
	// double-close.
	return nil
}

// KeyCount returns the number of keys written. Zero is legal; an
// empty SST is fine to ingest (it's a no-op on the key range).
func (b *sstBuilder) KeyCount() int { return b.keyCount }

// Path returns the on-disk path of the SST.
func (b *sstBuilder) Path() string { return b.path }

// buildSSTFromIter drains iter into a fresh SST at sstPath, returning
// the builder for the caller to inspect/delete. The iterator is
// expected to yield keys in strictly increasing pebble order; that
// holds for any pebble.Iterator scanning a single contiguous range
// without seek-skips.
//
// On any error, the partial SST is removed before returning.
func buildSSTFromIter(ctx context.Context, sstPath string, iter *pebble.Iterator) (*sstBuilder, error) {
	b, err := newSSTBuilder(sstPath)
	if err != nil {
		return nil, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = b.Close()
			_ = os.Remove(sstPath)
			return nil, err
		}
		// Copy the key/value out of the iterator — pebble reuses its
		// internal buffers on Next().
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		if err := b.Set(k, v); err != nil {
			_ = b.Close()
			_ = os.Remove(sstPath)
			return nil, err
		}
	}
	if err := iter.Error(); err != nil {
		_ = b.Close()
		_ = os.Remove(sstPath)
		return nil, fmt.Errorf("source iter: %w", err)
	}
	if err := b.Close(); err != nil {
		_ = os.Remove(sstPath)
		return nil, err
	}
	return b, nil
}
