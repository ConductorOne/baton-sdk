package xjwt

import (
	"crypto/rand"
	"encoding/hex"

	jose "github.com/go-jose/go-jose/v3"
)

// RandomNonce provides a basic, random value, conforming to the jose.NonceSource interface.
type RandomNonce struct {
	Size int
}

var _ jose.NonceSource = (*RandomNonce)(nil)

// Nonce returns a random string or an error
func (rn *RandomNonce) Nonce() (string, error) {
	s := rn.Size
	if s == 0 {
		s = 8
	}
	buf := make([]byte, s)
	rand.Reader.Read(buf[:])
	return hex.EncodeToString(buf[:]), nil
}
