package cli

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

var _ sessions.SessionStore = (*lazyCachingSessionStore)(nil)

type OtterAdjuster func(otterOptions *otter.Options[string, []byte])

func NewLazyCachingSessionStore(constructor sessions.SessionStoreConstructor, otterAdjuster OtterAdjuster) *lazyCachingSessionStore {
	otterOptions := &otter.Options[string, []byte]{
		// 15MB Note(kans): not much rigor went into this number.  An arbirary sampling of lambda invocations suggests they use around 50MB out of 128MB.
		MaximumWeight:    1024 * 1024 * 15,
		ExpiryCalculator: otter.ExpiryWriting[string, []byte](10 * time.Minute),
		StatsRecorder:    stats.NewCounter(),
		Weigher: func(key string, value []byte) uint32 {
			totalLen := 32 + len(key) + len(value)
			if totalLen < 0 {
				return math.MaxUint32
			}
			if totalLen > math.MaxInt32 {
				return math.MaxUint32
			}
			return uint32(totalLen)
		},
	}
	if otterAdjuster != nil {
		otterAdjuster(otterOptions)
	}

	if otterOptions.MaximumWeight == 0 {
		otterOptions = nil
	}
	return &lazyCachingSessionStore{constructor: constructor, otterOptions: otterOptions}
}

// lazyCachingSessionStore implements types.SessionStore interface but only creates the actual session
// when a method is called for the first time.
type lazyCachingSessionStore struct {
	constructor  sessions.SessionStoreConstructor
	once         sync.Once
	session      sessions.SessionStore
	err          error
	otterOptions *otter.Options[string, []byte]
}

// ensureSession creates the actual session store if it hasn't been created yet.
func (l *lazyCachingSessionStore) ensureSession(ctx context.Context) error {
	l.once.Do(func() {
		var ss sessions.SessionStore
		ss, l.err = l.constructor(ctx)
		if l.err != nil {
			return
		}
		if l.otterOptions == nil {
			ctxzap.Extract(ctx).Info("Session store cache is disabled")
			l.session = ss
			return
		}
		l.session, l.err = session.NewMemorySessionCache(l.otterOptions, ss)
	})
	return l.err
}

// Get implements types.SessionStore.
func (l *lazyCachingSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, false, err
	}
	return l.session.Get(ctx, key, opt...)
}

// GetMany implements types.SessionStore.
func (l *lazyCachingSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, nil, err
	}
	return l.session.GetMany(ctx, keys, opt...)
}

// Set implements types.SessionStore.
func (l *lazyCachingSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Set(ctx, key, value, opt...)
}

// SetMany implements types.SessionStore.
func (l *lazyCachingSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.SetMany(ctx, values, opt...)
}

// Delete implements types.SessionStore.
func (l *lazyCachingSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Delete(ctx, key, opt...)
}

// Clear implements types.SessionStore.
func (l *lazyCachingSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	if err := l.ensureSession(ctx); err != nil {
		return err
	}
	return l.session.Clear(ctx, opt...)
}

// GetAll implements types.SessionStore.
func (l *lazyCachingSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	if err := l.ensureSession(ctx); err != nil {
		return nil, "", err
	}
	return l.session.GetAll(ctx, pageToken, opt...)
}
