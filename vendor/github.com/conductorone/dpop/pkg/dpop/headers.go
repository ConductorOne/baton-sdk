package dpop

// HeaderName is the standard DPoP header name as defined in the DPoP specification.
// This header carries the DPoP proof JWT in requests and responses.
const HeaderName = "DPoP"

// NonceHeaderName is the standard DPoP nonce header name as defined in the DPoP specification.
// This header is used by servers to provide a nonce value that clients must include in subsequent DPoP proofs.
const NonceHeaderName = "DPoP-Nonce"
