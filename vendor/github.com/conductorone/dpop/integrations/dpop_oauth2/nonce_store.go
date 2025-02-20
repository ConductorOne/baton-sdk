package dpop_oauth2

import (
	"sync/atomic"
)

// NonceStore provides thread-safe storage and retrieval of the most recent DPoP nonce
type NonceStore struct {
	currentNonce atomic.Value
}

// NewNonceStore creates a new NonceStore
func NewNonceStore() *NonceStore {
	ns := &NonceStore{}
	ns.currentNonce.Store("")
	return ns
}

// GetNonce returns the current nonce value, or empty string if none is set
func (ns *NonceStore) GetNonce() string {
	val := ns.currentNonce.Load()
	if val == nil {
		return ""
	}
	return val.(string)
}

// SetNonce updates the current nonce value
func (ns *NonceStore) SetNonce(nonce string) {
	ns.currentNonce.Store(nonce)
}
