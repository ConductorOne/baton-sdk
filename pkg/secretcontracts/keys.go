package secretcontracts

import (
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"strings"

	"golang.org/x/crypto/cryptobyte"
	cbasn1 "golang.org/x/crypto/cryptobyte/asn1"
	"golang.org/x/crypto/ssh"
)

// sshDSAKeyType is the OpenSSH name of a DSA key, which the receiver refuses.
const sshDSAKeyType = "ssh-dss"

var (
	oidRSA     = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 1}
	oidEC      = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}
	oidEd25519 = asn1.ObjectIdentifier{1, 3, 101, 112}
	oidDSA     = asn1.ObjectIdentifier{1, 2, 840, 10040, 4, 1}
)

// pemBlocks returns every block in value. It fails when anything but
// whitespace precedes the first block or follows the last, or when a block
// does not frame; text between blocks is ignored.
func pemBlocks(value string) ([]*pem.Block, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "-----BEGIN ") || !strings.HasSuffix(trimmed, "-----") {
		return nil, false
	}
	var blocks []*pem.Block
	rest := []byte(trimmed)
	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		rest = remaining
	}
	if len(blocks) == 0 || len(blocks) != strings.Count(trimmed, "-----BEGIN ") ||
		len(blocks) != strings.Count(trimmed, "-----END ") {
		return nil, false
	}
	return blocks, true
}

func singleBlock(value string) (*pem.Block, bool) {
	blocks, ok := pemBlocks(value)
	if !ok || len(blocks) != 1 {
		return nil, false
	}
	return blocks[0], true
}

func singleCertificate(value string) bool {
	block, ok := singleBlock(value)
	return ok && block.Type == "CERTIFICATE" && certificateShape(block.Bytes)
}

func certificateChain(value string) bool {
	blocks, ok := pemBlocks(value)
	if !ok {
		return false
	}
	for _, block := range blocks {
		if block.Type != "CERTIFICATE" || !certificateShape(block.Bytes) {
			return false
		}
	}
	return true
}

type pkcs8Algorithm int

const (
	algorithmUnknown pkcs8Algorithm = iota
	algorithmSSH
	algorithmDSA
	algorithmOther
)

// pemPrivateKey accepts one PEM private key of any algorithm.
func pemPrivateKey(value string) bool {
	block, ok := singleBlock(value)
	if !ok {
		return false
	}
	if block.Type == "PRIVATE KEY" {
		return pkcs8Kind(block.Bytes) != algorithmUnknown
	}
	return privateKeyBlock(block)
}

// sshPrivateKey accepts what ssh -i reads: an OpenSSH key, or a PKCS#1, SEC1
// or PKCS#8 PEM key, encrypted or not. PuTTY, SSH2 and DSA keys are refused.
func sshPrivateKey(value string) bool {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "PuTTY-User-Key-File-") || strings.HasPrefix(value, "---- BEGIN SSH2 ") {
		return false
	}
	if strings.HasPrefix(value, "-----BEGIN OPENSSH PRIVATE KEY-----") {
		return openSSHKeyAccepted(value)
	}
	block, ok := singleBlock(value)
	if !ok {
		return false
	}
	switch block.Type {
	case "DSA PRIVATE KEY":
		return false
	case "PRIVATE KEY":
		return pkcs8Kind(block.Bytes) == algorithmSSH
	default:
		return privateKeyBlock(block)
	}
}

func openSSHKeyAccepted(value string) bool {
	key, err := ssh.ParseRawPrivateKey([]byte(value))
	if err != nil {
		var missing *ssh.PassphraseMissingError
		if errors.As(err, &missing) && missing.PublicKey != nil {
			return missing.PublicKey.Type() != sshDSAKeyType
		}
		return false
	}
	signer, err := ssh.NewSignerFromKey(key)
	return err == nil && signer.PublicKey().Type() != sshDSAKeyType
}

func privateKeyBlock(block *pem.Block) bool {
	switch block.Type {
	case "RSA PRIVATE KEY":
		return legacyEncrypted(block) || pkcs1Shape(block.Bytes)
	case "EC PRIVATE KEY":
		return legacyEncrypted(block) || sec1Shape(block.Bytes)
	case "ENCRYPTED PRIVATE KEY":
		return encryptedPKCS8Shape(block.Bytes)
	default:
		return false
	}
}

// legacyEncrypted is an OpenSSL-encrypted key: its body is ciphertext, so the
// headers are all there is to check.
func legacyEncrypted(block *pem.Block) bool {
	info, ok := block.Headers["DEK-Info"]
	if block.Headers["Proc-Type"] != "4,ENCRYPTED" || !ok || len(block.Bytes) == 0 {
		return false
	}
	cipher, iv, found := strings.Cut(info, ",")
	return found && cipher != "" && iv != ""
}

// sshPublicKey accepts one OpenSSH public key line without an options prefix.
func sshPublicKey(value string) bool {
	_, ok := parseSSHLine(value, false)
	return ok
}

// sshCertificate accepts one OpenSSH certificate line; its signature is not
// checked because nothing here knows which CA to trust.
func sshCertificate(value string) bool {
	key, ok := parseSSHLine(value, true)
	if !ok {
		return false
	}
	_, isCertificate := key.(*ssh.Certificate)
	return isCertificate
}

func parseSSHLine(value string, certificate bool) (ssh.PublicKey, bool) {
	value = strings.TrimSpace(value)
	if strings.ContainsAny(value, "\r\n") {
		return nil, false
	}
	fields := strings.Fields(value)
	if len(fields) < 2 {
		return nil, false
	}
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return nil, false
	}
	key, err := ssh.ParsePublicKey(blob)
	if err != nil || key.Type() != fields[0] {
		return nil, false
	}
	_, isCertificate := key.(*ssh.Certificate)
	return key, isCertificate == certificate
}

func pkcs8Kind(der []byte) pkcs8Algorithm {
	input := cryptobyte.String(der)
	var info, algorithm cryptobyte.String
	var oid asn1.ObjectIdentifier
	if !input.ReadASN1(&info, cbasn1.SEQUENCE) || !input.Empty() || !readVersion(&info, 0, 1) ||
		!info.ReadASN1(&algorithm, cbasn1.SEQUENCE) || !algorithm.ReadASN1ObjectIdentifier(&oid) ||
		!drain(&algorithm) || !info.ReadASN1(new(cryptobyte.String), cbasn1.OCTET_STRING) || !drain(&info) {
		return algorithmUnknown
	}
	switch {
	case oid.Equal(oidRSA), oid.Equal(oidEC), oid.Equal(oidEd25519):
		return algorithmSSH
	case oid.Equal(oidDSA):
		return algorithmDSA
	default:
		return algorithmOther
	}
}

func readVersion(s *cryptobyte.String, allowed ...int64) bool {
	var version int64
	if !s.ReadASN1Integer(&version) {
		return false
	}
	for _, candidate := range allowed {
		if version == candidate {
			return true
		}
	}
	return false
}

func drain(s *cryptobyte.String) bool {
	for !s.Empty() {
		if !s.ReadAnyASN1Element(new(cryptobyte.String), new(cbasn1.Tag)) {
			return false
		}
	}
	return true
}

func pkcs1Shape(der []byte) bool {
	input := cryptobyte.String(der)
	var key cryptobyte.String
	if !input.ReadASN1(&key, cbasn1.SEQUENCE) || !input.Empty() || !readVersion(&key, 0, 1) {
		return false
	}
	for i := 0; i < 8; i++ {
		if !key.ReadASN1(new(cryptobyte.String), cbasn1.INTEGER) {
			return false
		}
	}
	return drain(&key)
}

func sec1Shape(der []byte) bool {
	input := cryptobyte.String(der)
	var key cryptobyte.String
	return input.ReadASN1(&key, cbasn1.SEQUENCE) && input.Empty() && readVersion(&key, 1) &&
		key.ReadASN1(new(cryptobyte.String), cbasn1.OCTET_STRING) && drain(&key)
}

func encryptedPKCS8Shape(der []byte) bool {
	input := cryptobyte.String(der)
	var info, algorithm cryptobyte.String
	var oid asn1.ObjectIdentifier
	return input.ReadASN1(&info, cbasn1.SEQUENCE) && input.Empty() &&
		info.ReadASN1(&algorithm, cbasn1.SEQUENCE) && algorithm.ReadASN1ObjectIdentifier(&oid) && drain(&algorithm) &&
		info.ReadASN1(new(cryptobyte.String), cbasn1.OCTET_STRING)
}

// certificateShape checks the outline of an X.509 Certificate: a to-be-signed
// body (v1, v2 or v3), a signature algorithm and a signature.
func certificateShape(der []byte) bool {
	input := cryptobyte.String(der)
	var certificate, tbs cryptobyte.String
	if !input.ReadASN1(&certificate, cbasn1.SEQUENCE) || !input.Empty() ||
		!certificate.ReadASN1(&tbs, cbasn1.SEQUENCE) {
		return false
	}
	var version cryptobyte.String
	var present bool
	if !tbs.ReadOptionalASN1(&version, &present, cbasn1.Tag(0).Constructed().ContextSpecific()) {
		return false
	}
	if present {
		var number int64
		if !version.ReadASN1Integer(&number) || !version.Empty() || number < 0 || number > 2 {
			return false
		}
	}
	if !tbs.ReadASN1(new(cryptobyte.String), cbasn1.INTEGER) {
		return false
	}
	for i := 0; i < 5; i++ {
		if !tbs.ReadASN1(new(cryptobyte.String), cbasn1.SEQUENCE) {
			return false
		}
	}
	return drain(&tbs) &&
		certificate.ReadASN1(new(cryptobyte.String), cbasn1.SEQUENCE) &&
		certificate.ReadASN1(new(cryptobyte.String), cbasn1.BIT_STRING) && certificate.Empty()
}
