package age

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	filippoage "filippo.io/age"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const EncryptionProviderAge = "baton/age/v1"

// KeyIDForRecipient derives the EncryptedData.key_ids entry for an age recipient.
//
// It returns the lowercase hexadecimal SHA-256 digest of the UTF-8 canonical
// recipient string. This is the single source of truth for the key-ID
// derivation convention: producers set EncryptedData.key_ids to this value, and
// consumers that need to correlate ciphertext with recipient key material must
// call this function rather than reimplementing the derivation. Because both the
// SDK and its consumers import this package, the contract is enforced by shared
// code instead of by prose that can drift.
//
// The recipient must be the canonical recipient string (no surrounding
// whitespace, exactly one recipient); callers that accept untrusted input should
// validate it the same way Encrypt does before deriving a key ID.
func KeyIDForRecipient(recipient string) string {
	digest := sha256.Sum256([]byte(recipient))
	return hex.EncodeToString(digest[:])
}

type RecipientEncryptionProvider struct{}

func (p *RecipientEncryptionProvider) ValidateConfig(_ context.Context, conf *v2.EncryptionConfig) error {
	_, _, err := recipientFromConfig(conf)
	return err
}

func (p *RecipientEncryptionProvider) Encrypt(_ context.Context, conf *v2.EncryptionConfig, plaintext *v2.PlaintextData) (*v2.EncryptedData, error) {
	recipientText, recipient, err := recipientFromConfig(conf)
	if err != nil {
		return nil, err
	}

	var ciphertext bytes.Buffer
	w, err := filippoage.Encrypt(&ciphertext, recipient)
	if err != nil {
		return nil, fmt.Errorf("age: failed to initialize encryption: %w", err)
	}
	if _, err := io.Copy(w, bytes.NewReader(plaintext.GetBytes())); err != nil {
		return nil, fmt.Errorf("age: failed to encrypt data: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("age: failed to finalize encryption: %w", err)
	}

	return v2.EncryptedData_builder{
		Provider:       EncryptionProviderAge,
		Name:           plaintext.GetName(),
		Description:    plaintext.GetDescription(),
		Schema:         plaintext.GetSchema(),
		EncryptedBytes: ciphertext.Bytes(),
		KeyIds:         []string{KeyIDForRecipient(recipientText)},
	}.Build(), nil
}

func recipientFromConfig(conf *v2.EncryptionConfig) (string, filippoage.Recipient, error) {
	if conf == nil || conf.GetAgeRecipientConfig() == nil {
		return "", nil, status.Error(codes.InvalidArgument, "age: age recipient config is required")
	}
	recipientText := conf.GetAgeRecipientConfig().GetRecipient()
	if recipientText == "" {
		return "", nil, status.Error(codes.InvalidArgument, "age: recipient is required")
	}
	if strings.TrimSpace(recipientText) != recipientText || strings.ContainsAny(recipientText, "\r\n") {
		return "", nil, status.Error(codes.InvalidArgument, "age: recipient must be a single canonical recipient string")
	}

	recipients, err := filippoage.ParseRecipients(strings.NewReader(recipientText))
	if err != nil {
		return "", nil, status.Error(codes.InvalidArgument, "age: invalid recipient")
	}
	if len(recipients) != 1 {
		return "", nil, status.Error(codes.InvalidArgument, "age: exactly one recipient is required")
	}
	return recipientText, recipients[0], nil
}
