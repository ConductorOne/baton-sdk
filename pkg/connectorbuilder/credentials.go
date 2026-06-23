package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CredentialManager extends ResourceSyncer to add capabilities for managing credentials.
// Implementing this interface indicates the connector supports rotating credentials
// for resources of the associated type. This is commonly used for user accounts
// or service accounts that have rotatable credentials.
type CredentialManager interface {
	ResourceSyncer
	CredentialManagerLimited
}

type CredentialManagerLimited interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
	RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error)
}

type OldCredentialManager interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.CredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
}

func (b *builder) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.RotateCredential")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.RotateCredentialsType
	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	manager, ok := b.credentialManagers[rt]
	if !ok {
		l.Error("error: resource type does not have credential manager configured", zap.String("resource_type", rt))
		err = status.Error(codes.Unimplemented, "resource type does not have credential manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	plaintexts, annos, err := manager.Rotate(ctx, request.GetResourceId(), opts)
	if err != nil {
		l.Error("error: rotate credentials on resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: rotate credentials on resource failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		var encryptedData []*v2.EncryptedData
		encryptedData, err = pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.RotateCredentialResponse_builder{
		Annotations:   annos,
		ResourceId:    request.GetResourceId(),
		EncryptedData: encryptedDatas,
	}.Build(), nil
}

func (b *builder) addCredentialManager(_ context.Context, typeId string, in interface{}) error {
	if _, ok := in.(OldCredentialManager); ok {
		return fmt.Errorf("error: old credential manager interface implemented for %s", typeId)
	}

	if credentialManagers, ok := in.(CredentialManagerLimited); ok {
		if _, ok := b.credentialManagers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for credential manager %s", typeId)
		}
		b.credentialManagers[typeId] = credentialManagers
	}
	return nil
}

// CredentialIssuer extends ResourceSyncer to add the capability to mint a NEW
// credential for an existing identity (e.g. a service-account key/token),
// distinct from rotating the single credential on a resource. It is a separate,
// additive interface so existing CredentialManager implementations are
// unaffected — a connector opts in to issuance only by implementing it.
type CredentialIssuer interface {
	ResourceSyncer
	CredentialIssuerLimited
}

type CredentialIssuerLimited interface {
	// Issue mints a new credential for identityId and returns the newly created
	// secret resource alongside the plaintext material. The builder encrypts the
	// plaintext per the request's EncryptionConfigs before it leaves the
	// connector — for client-side keypair generation the returned PlaintextData
	// carries the private key, which the platform receives only in encrypted form.
	Issue(ctx context.Context,
		identityId *v2.ResourceId,
		credentialOptions *v2.LocalCredentialOptions) (*v2.Resource, []*v2.PlaintextData, annotations.Annotations, error)
	IssueCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error)
}

func (b *builder) IssueCredential(ctx context.Context, request *v2.IssueCredentialRequest) (*v2.IssueCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.IssueCredential")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.IssueCredentialType
	l := ctxzap.Extract(ctx)
	rt := request.GetIdentityId().GetResourceType()
	issuer, ok := b.credentialIssuers[rt]
	if !ok {
		l.Error("error: resource type does not have credential issuer configured", zap.String("resource_type", rt))
		err = status.Error(codes.Unimplemented, "resource type does not have credential issuer configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	secret, plaintexts, annos, err := issuer.Issue(ctx, request.GetIdentityId(), opts)
	if err != nil {
		l.Error("error: issue credential for identity failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: issue credential for identity failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		var encryptedData []*v2.EncryptedData
		encryptedData, err = pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return v2.IssueCredentialResponse_builder{
		Secret:        secret,
		EncryptedData: encryptedDatas,
		Annotations:   annos,
	}.Build(), nil
}

func (b *builder) addCredentialIssuer(_ context.Context, typeId string, in interface{}) error {
	if credentialIssuer, ok := in.(CredentialIssuerLimited); ok {
		if _, ok := b.credentialIssuers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for credential issuer %s", typeId)
		}
		b.credentialIssuers[typeId] = credentialIssuer
	}
	return nil
}
