# Session Cache Implementation

This package provides session cache implementations for the Baton SDK. It includes both in-memory and gRPC-based implementations.

## Overview

The session cache is used to store temporary data during sync operations. It provides a key-value store interface with support for:

- Basic CRUD operations (Get, Set, Delete, Clear)
- Batch operations (GetMany, SetMany)
- Namespace isolation using sync IDs
- Prefix support for key organization
- Context-based configuration

## Implementations

### Memory Session Cache

The `MemorySessionCache` provides an in-memory implementation suitable for single-process applications.

```go
import "github.com/conductorone/baton-sdk/pkg/session"

// Create a memory session cache
cache, err := session.NewMemorySessionCache(ctx)
if err != nil {
    log.Fatal(err)
}

// Use the cache
err = cache.Set(ctx, "key", []byte("value"))
```

### gRPC Session Cache

The `GRPCSessionCache` provides a distributed implementation that communicates with a gRPC service using existing DPoP credentials.

#### Using Static Access Token

```go
import (
    "github.com/conductorone/baton-sdk/pkg/session"
    v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
    "github.com/go-jose/go-jose/v4"
)

// Create the gRPC client using existing DPoP credentials
// These would typically come from the connector config flow
accessToken := "your-existing-access-token"
dpopKey := &jose.JSONWebKey{
    Key:       yourExistingKey, // From connector config flow
    KeyID:     "key-id",
    Algorithm: string(jose.EdDSA),
    Use:       "sig",
}

client, err := session.NewGRPCSessionClient(ctx, accessToken, dpopKey)
if err != nil {
    log.Fatal(err)
}

// Create the session cache
cache, err := session.NewGRPCSessionCache(ctx, client)
if err != nil {
    log.Fatal(err)
}
```

#### Using Token Source (Recommended)

For production use, it's recommended to use a token source that can handle automatic token refresh:

```go
import (
    "github.com/conductorone/baton-sdk/pkg/session"
    "github.com/go-jose/go-jose/v4"
    "golang.org/x/oauth2"
)

// Create a token source (this would typically come from the connector config flow)
tokenSource := &staticTokenSource{accessToken: "your-access-token"}
dpopKey := &jose.JSONWebKey{
    Key:       yourExistingKey, // From connector config flow
    KeyID:     "key-id",
    Algorithm: string(jose.EdDSA),
    Use:       "sig",
}

// Create the session cache using the token source
// Note: This requires the token source and DPoP key from the connector config flow
```

For advanced usage with custom connection options:

```go
import (
    "github.com/conductorone/baton-sdk/pkg/session"
    v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
    "google.golang.org/grpc"
)

// Create a gRPC connection with custom options
conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
if err != nil {
    log.Fatal(err)
}

// Create the gRPC client
client := v1.NewBatonSessionServiceClient(conn)

// Create the session cache
cache, err := session.NewGRPCSessionCache(ctx, client)
if err != nil {
    log.Fatal(err)
}
```

## Usage

### Basic Operations

```go
// Set a value
err := cache.Set(ctx, "user:123", []byte("John Doe"))

// Get a value
value, found, err := cache.Get(ctx, "user:123")
if found {
    fmt.Printf("User: %s\n", string(value))
}

// Delete a value
err = cache.Delete(ctx, "user:123")

// Clear all values for the current sync ID
err = cache.Clear(ctx)
```

### Batch Operations

```go
// Set multiple values
values := map[string][]byte{
    "user:456": []byte("Jane Smith"),
    "user:789": []byte("Bob Johnson"),
}
err := cache.SetMany(ctx, values)

// Get multiple values
keys := []string{"user:456", "user:789"}
retrievedValues, err := cache.GetMany(ctx, keys)
for key, value := range retrievedValues {
    fmt.Printf("Key: %s, Value: %s\n", key, string(value))
}
```

### Using Prefixes

```go
// Set a value with a prefix
err := cache.Set(ctx, "config", []byte("production"), session.WithPrefix("app"))

// Get a value with a prefix
value, found, err := cache.Get(ctx, "config", session.WithPrefix("app"))
```

### Sync ID Management

```go
// Set sync ID in context
ctx, err := session.SetSyncIDInContext(ctx, "sync-123")
if err != nil {
    log.Fatal(err)
}

// The cache will automatically use the sync ID from context
err = cache.Set(ctx, "key", []byte("value"))
```

## Interface

The session cache implements the `types.SessionCache` interface:

```go
type SessionCache interface {
    Get(ctx context.Context, key string, opt ...SessionCacheOption) ([]byte, bool, error)
    GetMany(ctx context.Context, keys []string, opt ...SessionCacheOption) (map[string][]byte, error)
    Set(ctx context.Context, key string, value []byte, opt ...SessionCacheOption) error
    SetMany(ctx context.Context, values map[string][]byte, opt ...SessionCacheOption) error
    Delete(ctx context.Context, key string, opt ...SessionCacheOption) error
    Clear(ctx context.Context, opt ...SessionCacheOption) error
    GetAll(ctx context.Context, opt ...SessionCacheOption) (map[string][]byte, error)
    Close() error
}
```

## Options

### SessionCacheOption

Options that can be applied to individual operations:

- `WithSyncID(syncID string)` - Set the sync ID for the operation
- `WithPrefix(prefix string)` - Set a prefix for keys

### SessionCacheConstructorOption

Options that can be applied during cache construction:

- Custom configuration options for the cache implementation

## Error Handling

The session cache implementations return appropriate errors for various failure scenarios:

- Network errors (gRPC implementation)
- Invalid sync IDs
- Missing required context values
- Unsupported operations (e.g., GetAll in gRPC implementation)

## Testing

The package includes comprehensive tests for both implementations:

```bash
go test ./pkg/session -v
```

## Limitations

### gRPC Implementation

- The `GetAll` operation is not supported by the gRPC service and returns an error
- Requires a running gRPC server implementing the `BatonSessionService`
- Network latency may affect performance
- Requires proper error handling for network failures

### Memory Implementation

- Data is not persisted across process restarts
- Memory usage grows with the number of stored items
- Not suitable for distributed applications

## Examples

See `example_usage.go` for complete usage examples. 