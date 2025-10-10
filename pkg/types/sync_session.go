package types

// type SessionStore interface {
// 	Get(ctx context.Context, key string, opt ...SessionOption) ([]byte, bool, error)
// 	GetMany(ctx context.Context, keys []string, opt ...SessionOption) (map[string][]byte, error)
// 	Set(ctx context.Context, key string, value []byte, opt ...SessionOption) error
// 	SetMany(ctx context.Context, values map[string][]byte, opt ...SessionOption) error
// 	Delete(ctx context.Context, key string, opt ...SessionOption) error
// 	Clear(ctx context.Context, opt ...SessionOption) error
// 	GetAll(ctx context.Context, opt ...SessionOption) (map[string][]byte, error)
// 	CloseStore(ctx context.Context) error
// }

// type SessionOption func(ctx context.Context, bag *SessionBag) error

// type SessionConstructor func(ctx context.Context, opt ...SessionConstructorOption) (SessionStore, error)

// type SessionConstructorOption func(ctx context.Context) (context.Context, error)

// type SessionBag struct {
// 	SyncID string
// 	Prefix string
// }
