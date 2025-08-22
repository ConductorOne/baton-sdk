package pebble

import (
	"encoding/base64"
	"fmt"
)

// PaginationToken represents a key-based continuation token for stable pagination.
type PaginationToken struct {
	// LastKey is the last key from the previous page
	LastKey []byte
}

const (
	// DefaultPageSize is the default page size when none is specified
	DefaultPageSize uint32 = 100

	// MaxPageSize is the maximum allowed page size
	MaxPageSize uint32 = 1000
)

// Paginator handles key-based pagination for stable iteration across all List operations.
type Paginator struct {
	engine *PebbleEngine
}

// NewPaginator creates a new Paginator instance.
func NewPaginator(engine *PebbleEngine) *Paginator {
	return &Paginator{
		engine: engine,
	}
}

// EncodeToken encodes a pagination token from the given last key.
func (p *Paginator) EncodeToken(lastKey []byte) (string, error) {
	if len(lastKey) == 0 {
		return "", fmt.Errorf("last key cannot be empty")
	}

	return base64.URLEncoding.EncodeToString(lastKey), nil
}

// DecodeToken decodes a pagination token string back to its components.
func (p *Paginator) DecodeToken(tokenStr string) (*PaginationToken, error) {
	if tokenStr == "" {
		return nil, nil // Empty token is valid and means no pagination
	}

	lastKey, err := base64.URLEncoding.DecodeString(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 encoding: %w", err)
	}

	if len(lastKey) == 0 {
		return nil, fmt.Errorf("token missing last key")
	}

	return &PaginationToken{
		LastKey: lastKey,
	}, nil
}

// ValidatePageSize validates and normalizes a page size value.
func (p *Paginator) ValidatePageSize(pageSize uint32) uint32 {
	if pageSize == 0 {
		return DefaultPageSize
	}

	if pageSize > MaxPageSize {
		return MaxPageSize
	}

	return pageSize
}

// CreateStartKey creates the starting key for pagination based on a token or prefix.
func (p *Paginator) CreateStartKey(token *PaginationToken, prefixKey []byte) []byte {
	if token != nil {
		return p.createNextKey(token.LastKey)
	}
	return prefixKey
}

// CreateUpperBound creates the upper bound key for range iteration.
func (p *Paginator) CreateUpperBound(prefixKey []byte) []byte {
	upperBound := make([]byte, len(prefixKey))
	copy(upperBound, prefixKey)

	for i := len(upperBound) - 1; i >= 0; i-- {
		if upperBound[i] < 0xFF {
			upperBound[i]++
			return upperBound[:i+1]
		}
	}

	return append(upperBound, 0x00)
}

// CreateNextToken creates a pagination token for the next page.
func (p *Paginator) CreateNextToken(lastKey []byte) (string, error) {
	if len(lastKey) == 0 {
		return "", nil
	}
	return p.EncodeToken(lastKey)
}

// createNextKey creates a key that comes immediately after the given key.
func (p *Paginator) createNextKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}

	nextKey := make([]byte, len(key))
	copy(nextKey, key)

	for i := len(nextKey) - 1; i >= 0; i-- {
		if nextKey[i] < 0xFF {
			nextKey[i]++
			return nextKey
		}
		nextKey[i] = 0x00
	}

	// All bytes were 0xFF, append 0x00 to the original key
	return append(key, 0x00)
}
