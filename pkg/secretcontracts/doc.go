// Package secretcontracts validates and encodes typed secret payloads against
// the contract bundle that Multipass publishes. The bundle under bundle/ is
// vendored by scripts/update-secret-contracts.sh and pinned by UPSTREAM.json.
//
// Two checks differ on purpose. ValidateProducer says whether a connector may
// write the bytes. ValidateStored says whether the receiver opens them, so it
// accepts legacy generic and unknown stored values that a producer must not
// write.
package secretcontracts
