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

	keyID := sha256.Sum256([]byte(recipientText))
	return v2.EncryptedData_builder{
		Provider:       EncryptionProviderAge,
		Name:           plaintext.GetName(),
		Description:    plaintext.GetDescription(),
		Schema:         plaintext.GetSchema(),
		EncryptedBytes: ciphertext.Bytes(),
		KeyIds:         []string{hex.EncodeToString(keyID[:])},
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
