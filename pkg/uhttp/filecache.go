package uhttp

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type FileCache struct {
	data     map[string]any
	fileName string
	mu       sync.RWMutex
}

func NewFileCache(ctx context.Context, fileName string) (*FileCache, error) {
	l := ctxzap.Extract(ctx)
	fc := FileCache{
		data:     map[string]any{},
		fileName: fileName,
	}

	err := fc.load(ctx)
	if err != nil {
		l.Debug("error reading cache file", zap.Error(err))
		return nil, err
	}

	return &fc, nil
}

func (fc *FileCache) load(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	file, err := os.ReadFile(fc.fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			l.Debug("error reading cache file", zap.Error(err))
		}
		return err
	}

	err = json.Unmarshal(file, &fc.data)
	if err != nil {
		l.Debug("error unmarshaling cache data", zap.Error(err))
		return err
	}

	return nil
}

func (fc *FileCache) save(ctx context.Context) {
	l := ctxzap.Extract(ctx)
	file, err := json.Marshal(fc.data)
	if err != nil {
		l.Debug("error marshaling cache data", zap.Error(err))
		return
	}

	err = os.WriteFile(fc.fileName, file, 0600)
	if err != nil {
		l.Debug("error writing cache data", zap.Error(err))
	}
}

func (fc *FileCache) Get(key string) any {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.data[key]
}

func (fc *FileCache) Set(key string, value any) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	fc.data[key] = value
	fc.save(context.TODO())
}
