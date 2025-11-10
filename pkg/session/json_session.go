package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

type Cache = otter.Cache[string, *WeightedValue]

func NewOtter(otterOptions *otter.Options[string, *WeightedValue]) (*Cache, error) {
	if otterOptions == nil {
		otterOptions = &otter.Options[string, *WeightedValue]{
			// 15MB Note(kans): not much rigor went into this number.  An arbirary sampling of lambda invocations suggests they use around 50MB out of 128MB.
			MaximumWeight:    1024 * 1024 * 15,
			ExpiryCalculator: otter.ExpiryWriting[string, *WeightedValue](10 * time.Minute),
			StatsRecorder:    stats.NewCounter(),
			Weigher: func(key string, value *WeightedValue) uint32 {
				return value.W
			},
		}
	}
	return otter.New(otterOptions)
}

type WeightedValue struct {
	V interface{}
	W uint32
}

type JSONCaching struct {
	cache *Cache
}

// See GRPC validation rules for eg GetManyRequest.
func GetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, cache *Cache, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	getFromSS := func(ctx context.Context, missingKeys []string) (map[string]*WeightedValue, error) {
		bytesMap, err := UnrollGetMany(ctx, ss, missingKeys, opt...)
		if err != nil {
			return nil, err
		}
		result := make(map[string]*WeightedValue)
		for key, bytes := range bytesMap {
			var item T
			err = json.Unmarshal(bytes, &item)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal item for key %s: %w", key, err)
			}
			fmt.Printf("🌮 loaded a many %v\n", item)
			result[key] = &WeightedValue{
				V: item,
				W: uint32(len(bytes)),
			}
		}
		if len(bytesMap) == 0 {
			fmt.Printf("🌮 found nothing %v\n", missingKeys)
		}
		return result, nil
	}
	fmt.Printf("🌮 getting a many %v\n", keys)
	var values map[string]*WeightedValue
	var err error

	if cache == nil {
		values, err = getFromSS(ctx, keys)
	} else {
		values, err = cache.BulkGet(ctx, keys, otter.BulkLoaderFunc[string, *WeightedValue](getFromSS))
	}

	if err != nil {
		return nil, err
	}
	result := make(map[string]T)
	for key, value := range values {
		var item T
		if value == nil {
			fmt.Printf("🌮 did not find %s in cache or store\n", key)
			continue
		}
		item, ok := value.V.(T)
		if !ok {
			return nil, fmt.Errorf("item (%T) is not of type %T (%s) %v", value.V, item, key, value == nil)
		}
		fmt.Printf("🌮 got a many %s\n", key)
		result[key] = item
	}
	return result, nil
}

func SetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, cache *Cache, items map[string]T, opt ...sessions.SessionStoreOption) error {
	bytesMap := make(map[string][]byte)
	fmt.Printf("🌮 setting a many\n")
	for key, item := range items {
		bytes, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal item for key %s: %w", key, err)
		}
		fmt.Printf("🌮 setting %s\n", key)
		cache.Set(key, &WeightedValue{
			V: item,
			W: uint32(len(bytes)),
		})
		bytesMap[key] = bytes
	}

	return UnrollSetMany(ctx, ss, bytesMap, opt...)
}

func GetJSON[T any](ctx context.Context, ss sessions.SessionStore, cache *Cache, key string, opt ...sessions.SessionStoreOption) (T, bool, error) {
	loader := func(ctx context.Context, key string) (*WeightedValue, error) {
		v := &WeightedValue{}
		bytes, found, err := ss.Get(ctx, key, opt...)
		if err != nil {
			return v, err
		}
		if !found {
			return v, otter.ErrNotFound
		}
		var item T
		err = json.Unmarshal(bytes, &item)
		if err != nil {
			return v, err
		}
		fmt.Printf("🌮 loaded %s\n", key)
		v.V = item
		v.W = uint32(len(bytes))
		return v, nil
	}

	var item *WeightedValue
	var err error
	if cache != nil {
		item, err = cache.Get(ctx, key, otter.LoaderFunc[string, *WeightedValue](loader))
	} else {
		item, err = loader(ctx, key)
	}
	var zero T
	if errors.Is(err, otter.ErrNotFound) || item == nil {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}

	v, ok := item.V.(T)
	if !ok {
		return zero, false, fmt.Errorf("item is not of type %T for key (%s)", v, key)
	}
	fmt.Printf("🌮 got a json: %s\n", key)
	return v, true, nil
}

func SetJSON[T any](ctx context.Context, ss sessions.SessionStore, cache *Cache, key string, item T, opt ...sessions.SessionStoreOption) error {
	bytes, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}
	fmt.Printf("🌮 seta a item %v\n", key)
	_, _ = cache.Set(key, &WeightedValue{
		V: item,
		W: uint32(len(bytes)),
	})
	return ss.Set(ctx, key, bytes, opt...)
}

func DeleteJSON(ctx context.Context, ss sessions.SessionStore, cache *Cache, key string, opt ...sessions.SessionStoreOption) error {
	_, _ = cache.Invalidate(key)
	return ss.Delete(ctx, key, opt...)
}

func ClearJSON(ctx context.Context, ss sessions.SessionStore, cache *Cache, opt ...sessions.SessionStoreOption) error {
	// TODO. Respect the prefix....
	cache.InvalidateAll()
	return ss.Clear(ctx, opt...)
}

func GetAllJSON[T any](ctx context.Context, ss sessions.SessionStore, cache *Cache, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	result := make(map[string]T)
	pageToken := ""

	for {
		rawMap, nextPageToken, err := ss.GetAll(ctx, pageToken, opt...)
		if err != nil {
			return nil, err
		}
		for key, bytes := range rawMap {
			var item T
			err = json.Unmarshal(bytes, &item)
			if err != nil {
				return nil, err
			}
			result[key] = item

			if cache != nil {
				_, _ = cache.Set(key, &WeightedValue{
					V: item,
					W: uint32(len(bytes)),
				})
			}
		}
		if nextPageToken == "" {
			break
		}
		if pageToken == nextPageToken {
			return nil, fmt.Errorf("page token is the same as the next page token")
		}
		pageToken = nextPageToken
	}

	return result, nil
}
