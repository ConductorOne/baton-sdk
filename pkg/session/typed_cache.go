package session

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types"
)

type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

type TypedSessionCache[T any] struct {
	cache types.SessionStore
	codec Codec[T]
}

func NewTypedSessionCache[T any](cache types.SessionStore, codec Codec[T]) *TypedSessionCache[T] {
	return &TypedSessionCache[T]{
		cache: cache,
		codec: codec,
	}
}

func (t *TypedSessionCache[T]) Get(ctx context.Context, key string, opt ...types.SessionOption) (T, bool, error) {
	var zero T
	data, found, err := t.cache.Get(ctx, key, opt...)
	if err != nil {
		return zero, false, err
	}
	if !found {
		return zero, false, nil
	}

	value, err := t.codec.Decode(data)
	if err != nil {
		return zero, false, fmt.Errorf("failed to decode value: %w", err)
	}

	return value, true, nil
}

func (t *TypedSessionCache[T]) Set(ctx context.Context, key string, value T, opt ...types.SessionOption) error {
	data, err := t.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode value: %w", err)
	}

	return t.cache.Set(ctx, key, data, opt...)
}

func (t *TypedSessionCache[T]) GetMany(ctx context.Context, keys []string, opt ...types.SessionOption) (map[string]T, error) {
	dataMap, err := t.cache.GetMany(ctx, keys, opt...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]T)
	for key, data := range dataMap {
		value, err := t.codec.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value for key %s: %w", key, err)
		}
		result[key] = value
	}

	return result, nil
}

func (t *TypedSessionCache[T]) SetMany(ctx context.Context, values map[string]T, opt ...types.SessionOption) error {
	dataMap := make(map[string][]byte)
	for key, value := range values {
		data, err := t.codec.Encode(value)
		if err != nil {
			return fmt.Errorf("failed to encode value for key %s: %w", key, err)
		}
		dataMap[key] = data
	}

	return t.cache.SetMany(ctx, dataMap, opt...)
}

func (t *TypedSessionCache[T]) Delete(ctx context.Context, key string, opt ...types.SessionOption) error {
	return t.cache.Delete(ctx, key, opt...)
}

func (t *TypedSessionCache[T]) Clear(ctx context.Context, opt ...types.SessionOption) error {
	return t.cache.Clear(ctx, opt...)
}

func (t *TypedSessionCache[T]) GetAll(ctx context.Context, opt ...types.SessionOption) (map[string]T, error) {
	dataMap, err := t.cache.GetAll(ctx, opt...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]T)
	for key, data := range dataMap {
		value, err := t.codec.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value for key %s: %w", key, err)
		}
		result[key] = value
	}

	return result, nil
}

func (t *TypedSessionCache[T]) CloseStore(ctx context.Context) error {
	return t.cache.CloseStore(ctx)
}

type JSONCodec[T any] struct{}

func (j *JSONCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

func (j *JSONCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type StringCodec struct{}

func (s *StringCodec) Encode(value string) ([]byte, error) {
	return []byte(value), nil
}

func (s *StringCodec) Decode(data []byte) (string, error) {
	return string(data), nil
}

type IntCodec struct{}

func (i *IntCodec) Encode(value int) ([]byte, error) {
	return []byte(fmt.Sprintf("%d", value)), nil
}

func (i *IntCodec) Decode(data []byte) (int, error) {
	var value int
	_, err := fmt.Sscanf(string(data), "%d", &value)
	return value, err
}

type BoolCodec struct{}

func (b *BoolCodec) Encode(value bool) ([]byte, error) {
	if value {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (b *BoolCodec) Decode(data []byte) (bool, error) {
	return string(data) == "true", nil
}

func NewJSONSessionCache[T any](cache types.SessionStore) *TypedSessionCache[T] {
	return NewTypedSessionCache(cache, &JSONCodec[T]{})
}

func NewStringSessionCache(cache types.SessionStore) *TypedSessionCache[string] {
	return NewTypedSessionCache(cache, &StringCodec{})
}

func NewIntSessionCache(cache types.SessionStore) *TypedSessionCache[int] {
	return NewTypedSessionCache(cache, &IntCodec{})
}

func NewBoolSessionCache(cache types.SessionStore) *TypedSessionCache[bool] {
	return NewTypedSessionCache(cache, &BoolCodec{})
}
