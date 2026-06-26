package pebble

import (
	"context"
	"maps"
	"strings"

	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// This is the same function as in c1file, but it is duplicated here to avoid a circular dependency.
func applyBag(ctx context.Context, opt ...sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		err := o(ctx, bag)
		if err != nil {
			return nil, fmt.Errorf("error applying session option: %w", err)
		}
	}
	if bag.SyncID == "" {
		return nil, fmt.Errorf("sync id is required")
	}
	return bag, nil
}

func encodeSessionBySyncPrefix(syncID string) []byte {
	buf := make([]byte, 0, 32+len(syncID))
	buf = append(buf, versionV3, typeSession)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, syncID)
	return codec.AppendTupleSeparator(buf)
}

func encodeSessionKey(syncID, key string) []byte {
	buf := make([]byte, 0, 32+len(syncID)+len(key))
	buf = append(buf, versionV3, typeSession)
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleStrings(buf, syncID, key)
	return buf
}

func (e *Engine) SessionGet(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, false, fmt.Errorf("error applying session option: %w", err)
	}

	keyBytes := encodeSessionKey(bag.SyncID, bag.Prefix+key)
	val, closer, err := e.db.Get(keyBytes)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()
	r := &v3.SessionRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, false, fmt.Errorf("SessionGet: unmarshal: %w", err)
	}

	return r.GetValue(), true, nil
}

func (e *Engine) SessionGetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, nil, fmt.Errorf("error applying session option: %w", err)
	}

	if len(keys) == 0 {
		return nil, nil, nil
	}

	prefixedKeys := make([]string, len(keys))
	if bag.Prefix == "" {
		prefixedKeys = keys
	} else {
		for i, key := range keys {
			prefixedKeys[i] = bag.Prefix + key
		}
	}

	unprocessedKeys := make(map[string]struct{}, len(keys))

	// Initialize unprocessedKeys with all keys - we'll remove them as we process results
	// Start by calculating size of all unprocessed keys (they'll be in the return slice)
	type item struct {
		key   string
		value []byte
	}
	results := make([]item, 0, len(keys))
	messageSize := 0

	for _, prefixedKey := range prefixedKeys {
		keyBytes := encodeSessionKey(bag.SyncID, prefixedKey)
		val, closer, err := e.db.Get(keyBytes)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, nil, err
		}
		r := &v3.SessionRecord{}
		if err := unmarshalRecord(val, r); err != nil {
			closeErr := closer.Close()
			return nil, nil, errors.Join(closeErr, fmt.Errorf("SessionGetMany: unmarshal: %w", err))
		}
		closeErr := closer.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("SessionGetMany: close: %w", closeErr)
		}
		key := prefixedKey[len(bag.Prefix):]
		results = append(results, item{key: key, value: r.GetValue()})
		messageSize += len(key) + 10
	}

	ret := make(map[string][]byte)
	for _, r := range results {
		value := r.value
		key := r.key

		netItemSize := len(value) + 10 // 10 is extra padding for overhead.
		if messageSize+netItemSize <= sessions.MaxSessionStoreSizeLimit {
			messageSize += netItemSize
			ret[key] = value
		} else {
			unprocessedKeys[key] = struct{}{}
		}
	}

	unprocessedKeysSlice := make([]string, 0, len(unprocessedKeys))
	for key := range unprocessedKeys {
		unprocessedKeysSlice = append(unprocessedKeysSlice, key)
	}

	return ret, unprocessedKeysSlice, nil
}

func (e *Engine) SessionGetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return nil, "", fmt.Errorf("error applying session option: %w", err)
	}

	result := make(map[string][]byte)
	messageSizeRemaining := sessions.MaxSessionStoreSizeLimit
	for {
		items, nextPageToken, itemsSize, err := e.sessionGetAllChunk(ctx, pageToken, messageSizeRemaining, bag)
		if err != nil {
			return nil, "", fmt.Errorf("session-get-all: error getting all data from session: %w", err)
		}
		maps.Copy(result, items)

		if len(items) == 0 {
			break
		}

		if nextPageToken == "" {
			pageToken = ""
			break
		}

		if pageToken == nextPageToken {
			return nil, "", fmt.Errorf("page token is the same as the next page token: %s", pageToken)
		}
		pageToken = nextPageToken

		messageSizeRemaining -= itemsSize
		if messageSizeRemaining <= 0 {
			break
		}
	}

	return result, pageToken, nil
}

func (e *Engine) sessionGetAllChunk(ctx context.Context, pageToken string, sizeLimit int, bag *sessions.SessionStoreBag) (map[string][]byte, string, int, error) {
	syncPrefix := encodeSessionBySyncPrefix(bag.SyncID)
	var lower []byte
	switch {
	case pageToken != "":
		lower = encodeSessionKey(bag.SyncID, bag.Prefix+pageToken)
	case bag.Prefix != "":
		lower = encodeSessionKey(bag.SyncID, bag.Prefix)
	default:
		lower = syncPrefix
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upperBoundOf(syncPrefix),
	})
	if err != nil {
		return nil, "", 0, err
	}
	defer iter.Close()

	results := make(map[string][]byte)
	nextPageToken := ""
	messageSize := 0
	tooBig := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", 0, err
		}
		r := &v3.SessionRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return nil, "", 0, fmt.Errorf("session-get-all: unmarshal: %w", err)
		}
		if r.GetSyncId() != bag.SyncID {
			continue
		}
		storedKey := r.GetKey()
		if bag.Prefix != "" {
			if !strings.HasPrefix(storedKey, bag.Prefix) {
				break
			}
		}
		key := storedKey
		if bag.Prefix != "" {
			key = key[len(bag.Prefix):]
		}
		nextPageToken = key
		value := r.GetValue()
		itemSize := len(key) + len(value) + 20
		if messageSize+itemSize > sizeLimit {
			tooBig = true
			break
		}
		if len(results) >= 100 {
			break
		}
		results[key] = value
		messageSize += itemSize
	}
	if err := iter.Error(); err != nil {
		return nil, "", 0, fmt.Errorf("session-get-all: iter: %w", err)
	}

	if tooBig {
		return results, nextPageToken, messageSize, nil
	}
	if len(results) < 100 {
		return results, "", messageSize, nil
	}
	return results, nextPageToken, messageSize, nil
}

func (e *Engine) SessionSet(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}
	keyBytes := encodeSessionKey(bag.SyncID, bag.Prefix+key)
	val, err := marshalRecord(&v3.SessionRecord{
		SyncId: bag.SyncID,
		Key:    bag.Prefix + key,
		Value:  value,
	})
	if err != nil {
		return fmt.Errorf("error marshalling session record: %w", err)
	}

	return e.db.Set(keyBytes, val, writeOpts(e.opts.durability))
}

func (e *Engine) SessionSetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}

	batch := e.db.NewBatch()
	defer batch.Close()

	for key, value := range values {
		prefixedKey := bag.Prefix + key
		keyBytes := encodeSessionKey(bag.SyncID, prefixedKey)
		val, err := marshalRecord(&v3.SessionRecord{
			SyncId: bag.SyncID,
			Key:    prefixedKey,
			Value:  value,
		})
		if err != nil {
			return fmt.Errorf("error marshalling session record: %w", err)
		}
		if err := batch.Set(keyBytes, val, nil); err != nil {
			return fmt.Errorf("error setting session record: %w", err)
		}
	}

	err = batch.Commit(writeOpts(e.opts.durability))
	if err != nil {
		return fmt.Errorf("error committing session batch: %w", err)
	}
	return nil
}

func (e *Engine) SessionDelete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}
	keyBytes := encodeSessionKey(bag.SyncID, bag.Prefix+key)
	return e.db.Delete(keyBytes, writeOpts(e.opts.durability))
}

func (e *Engine) SessionClear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := applyBag(ctx, opt...)
	if err != nil {
		return fmt.Errorf("error applying session option: %w", err)
	}
	syncPrefix := encodeSessionBySyncPrefix(bag.SyncID)
	if bag.Prefix == "" {
		return e.db.DeleteRange(syncPrefix, upperBoundOf(syncPrefix), writeOpts(e.opts.durability))
	}

	lower := encodeSessionKey(bag.SyncID, bag.Prefix)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upperBoundOf(syncPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := e.db.NewBatch()
	defer batch.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		r := &v3.SessionRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("session-clear: unmarshal: %w", err)
		}
		if r.GetSyncId() != bag.SyncID {
			continue
		}
		if !strings.HasPrefix(r.GetKey(), bag.Prefix) {
			break
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("session-clear: iter: %w", err)
	}
	return batch.Commit(writeOpts(e.opts.durability))
}
