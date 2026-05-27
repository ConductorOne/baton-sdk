package codec

import "errors"

// ErrCodecTypeMismatch is returned by a generated codec when the
// supplied proto.Message is not the type the codec was registered
// for. Engine write paths surface this as a DataLoss-class error.
var ErrCodecTypeMismatch = errors.New("codec: input message type does not match registered codec")

// ErrInvalidSyncID is returned when EncodeSyncID receives a string
// that is not a parseable KSUID.
var ErrInvalidSyncID = errors.New("codec: sync_id is not a valid KSUID")

// ErrInvalidTuple is returned by tuple decoders when the bytes are
// malformed (e.g. an escape sequence with no follower, or a truncated
// fixed-width integer).
var ErrInvalidTuple = errors.New("codec: invalid tuple encoding")
