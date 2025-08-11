package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// Codec defines how to serialize and deserialize values to/from bytes
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// TypedSessionCache provides type-safe methods for working with composite types
type TypedSessionCache[T any] struct {
	cache types.SessionStore
	codec Codec[T]
}

// NewTypedSessionCache creates a new type-safe session cache wrapper
func NewTypedSessionCache[T any](cache types.SessionStore, codec Codec[T]) *TypedSessionCache[T] {
	return &TypedSessionCache[T]{
		cache: cache,
		codec: codec,
	}
}

// Get retrieves a typed value from the cache
func (t *TypedSessionCache[T]) Get(ctx context.Context, key string, opt ...types.SessionOption) (T, bool, error) {
	var zero T
	data, found, err := t.cache.Get(ctx, key, opt...)
	if err != nil || !found {
		return zero, found, err
	}

	value, err := t.codec.Decode(data)
	if err != nil {
		return zero, false, fmt.Errorf("failed to decode value for key %q: %w", key, err)
	}

	return value, true, nil
}

// Set stores a typed value in the cache
func (t *TypedSessionCache[T]) Set(ctx context.Context, key string, value T, opt ...types.SessionOption) error {
	data, err := t.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode value for key %q: %w", key, err)
	}

	return t.cache.Set(ctx, key, data, opt...)
}

// GetMany retrieves multiple typed values from the cache
func (t *TypedSessionCache[T]) GetMany(ctx context.Context, keys []string, opt ...types.SessionOption) (map[string]T, error) {
	dataMap, err := t.cache.GetMany(ctx, keys, opt...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]T)
	for key, data := range dataMap {
		value, err := t.codec.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value for key %q: %w", key, err)
		}
		result[key] = value
	}

	return result, nil
}

// SetMany stores multiple typed values in the cache
func (t *TypedSessionCache[T]) SetMany(ctx context.Context, values map[string]T, opt ...types.SessionOption) error {
	dataMap := make(map[string][]byte)
	for key, value := range values {
		data, err := t.codec.Encode(value)
		if err != nil {
			return fmt.Errorf("failed to encode value for key %q: %w", key, err)
		}
		dataMap[key] = data
	}

	return t.cache.SetMany(ctx, dataMap, opt...)
}

// Built-in codecs for common composite types

// JSONCodec provides JSON serialization for any type
type JSONCodec[T any] struct{}

func (j JSONCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

func (j JSONCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

// StringCodec provides simple string serialization
type StringCodec struct{}

func (s StringCodec) Encode(value string) ([]byte, error) {
	return []byte(value), nil
}

func (s StringCodec) Decode(data []byte) (string, error) {
	return string(data), nil
}

// IntCodec provides integer serialization
type IntCodec struct{}

func (i IntCodec) Encode(value int) ([]byte, error) {
	return []byte(strconv.Itoa(value)), nil
}

func (i IntCodec) Decode(data []byte) (int, error) {
	return strconv.Atoi(string(data))
}

// BoolCodec provides boolean serialization
type BoolCodec struct{}

func (b BoolCodec) Encode(value bool) ([]byte, error) {
	return []byte(strconv.FormatBool(value)), nil
}

func (b BoolCodec) Decode(data []byte) (bool, error) {
	return strconv.ParseBool(string(data))
}

// Float64Codec provides float64 serialization
type Float64Codec struct{}

func (f Float64Codec) Encode(value float64) ([]byte, error) {
	return []byte(strconv.FormatFloat(value, 'g', -1, 64)), nil
}

func (f Float64Codec) Decode(data []byte) (float64, error) {
	return strconv.ParseFloat(string(data), 64)
}

// Convenience constructors for common types

// NewJSONSessionCache creates a type-safe cache using JSON serialization
func NewJSONSessionCache[T any](cache types.SessionStore) *TypedSessionCache[T] {
	return NewTypedSessionCache(cache, JSONCodec[T]{})
}

// NewStringSessionCache creates a type-safe cache for strings
func NewStringSessionCache(cache types.SessionStore) *TypedSessionCache[string] {
	return NewTypedSessionCache(cache, StringCodec{})
}

// NewIntSessionCache creates a type-safe cache for integers
func NewIntSessionCache(cache types.SessionStore) *TypedSessionCache[int] {
	return NewTypedSessionCache(cache, IntCodec{})
}

// NewBoolSessionCache creates a type-safe cache for booleans
func NewBoolSessionCache(cache types.SessionStore) *TypedSessionCache[bool] {
	return NewTypedSessionCache(cache, BoolCodec{})
}

// NewFloat64SessionCache creates a type-safe cache for float64s
func NewFloat64SessionCache(cache types.SessionStore) *TypedSessionCache[float64] {
	return NewTypedSessionCache(cache, Float64Codec{})
}
