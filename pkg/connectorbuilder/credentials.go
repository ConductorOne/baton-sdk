package connectorbuilder

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// CredentialIssuerV2 is the ResourceSyncerV2 variant implemented by modern
// connectors. It advertises the same issuance contract without forcing a V2
// syncer to also implement the legacy pagination interface.
type CredentialIssuerV2 interface {
	ResourceSyncerV2
	CredentialIssuerLimited
}

type CredentialIssuerLimited interface {
	// Issue mints a new credential for identityId and returns the newly created
	// secret resource alongside the plaintext material. The builder encrypts the
	// plaintext per the request's EncryptionConfigs before it leaves the
	// connector — for client-side keypair generation the returned PlaintextData
	// carries the private key, which the platform receives only in encrypted form.
	Issue(ctx context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error)
	IssueCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error)
}

// CredentialIssueInput is the connector-facing issuance contract. Keeping the
// request structured lets the SDK add portable constraints without repeatedly
// breaking every connector implementation.
type CredentialIssueInput struct {
	IdentityID        *v2.ResourceId
	CredentialOptions *v2.CredentialIssueOptions
	ExpiresAt         *timestamppb.Timestamp
	RequestID         string
}

type CredentialIssueOutput struct {
	Secret        *v2.Resource
	PlaintextData []*v2.PlaintextData
	Annotations   annotations.Annotations
	ResourceMode  v2.CredentialResourceMode
}

func (b *builder) IssueCredential(ctx context.Context, request *v2.IssueCredentialRequest) (*v2.IssueCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.IssueCredential")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	start := b.nowFunc()
	tt := tasks.IssueCredentialType
	l := ctxzap.Extract(ctx)
	if request == nil || request.GetIdentityId() == nil {
		err = status.Error(codes.InvalidArgument, "identity id is required")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	rt := request.GetIdentityId().GetResourceType()
	issuer, ok := b.credentialIssuers[rt]
	if !ok {
		l.Error("error: resource type does not have credential issuer configured", zap.String("resource_type", rt))
		err = status.Error(codes.Unimplemented, "resource type does not have credential issuer configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}
	if len(request.GetEncryptionConfigs()) == 0 {
		err = status.Error(codes.InvalidArgument, "at least one encryption config is required for credential issuance")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	// Validate recipients before asking the connector to mutate the provider.
	// A later transport or encryption failure can still be ambiguous, so service
	// task execution deliberately does not retry issuance automatically.
	err = crypto.ValidateEncryptionConfigs(request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}
	pkem, err := crypto.NewEncryptionManager(nil, request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: initializing encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: initializing encryption manager failed: %w", err)
	}
	details, _, err := issuer.IssueCapabilityDetails(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: get credential issuance capability details: %w", err)
	}
	input := &CredentialIssueInput{
		IdentityID:        request.GetIdentityId(),
		CredentialOptions: request.GetCredentialOptions(),
		ExpiresAt:         request.GetExpiresAt(),
		RequestID:         request.GetRequestId(),
	}
	descriptor, err := validateCredentialIssueInput(input, details, b.nowFunc())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid credential issuance request: %v", err)
	}

	output, err := issuer.Issue(ctx, input)
	if err != nil {
		l.Error("error: issue credential for identity failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: issue credential for identity failed: %w", err)
	}
	err = validateCredentialIssueOutput(request.GetIdentityId(), request.GetExpiresAt(), output, descriptor)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, status.Errorf(codes.Internal, "connector returned invalid credential issuance output: %v", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range output.PlaintextData {
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
		Secret:        output.Secret,
		EncryptedData: encryptedDatas,
		Annotations:   output.Annotations,
		ResourceMode:  output.ResourceMode,
		RequestId:     request.GetRequestId(),
	}.Build(), nil
}

func validateCredentialIssueOutput(identityID *v2.ResourceId, requestedExpiresAt *timestamppb.Timestamp, output *CredentialIssueOutput, descriptor *v2.CredentialIssueOptionDescriptor) error {
	if output == nil || output.Secret == nil || output.Secret.GetId() == nil {
		return fmt.Errorf("secret resource and id are required")
	}
	if strings.TrimSpace(output.Secret.GetId().GetResource()) == "" || strings.TrimSpace(output.Secret.GetId().GetResourceType()) == "" {
		return fmt.Errorf("secret resource id is incomplete")
	}
	trait := &v2.SecretTrait{}
	secretAnnotations := annotations.Annotations(output.Secret.GetAnnotations())
	found, err := secretAnnotations.Pick(trait)
	if err != nil {
		return fmt.Errorf("read secret trait: %w", err)
	}
	if !found {
		return fmt.Errorf("secret resource must carry SecretTrait")
	}
	if !proto.Equal(trait.GetIdentityId(), identityID) {
		return fmt.Errorf("secret trait identity_id does not match authenticating principal")
	}
	if requestedExpiresAt != nil {
		if trait.GetExpiresAt() == nil {
			return fmt.Errorf("secret trait expires_at is required when expiry was requested")
		}
		if err := trait.GetExpiresAt().CheckValid(); err != nil {
			return fmt.Errorf("secret trait expires_at is invalid: %w", err)
		}
		if trait.GetExpiresAt().AsTime().After(requestedExpiresAt.AsTime()) {
			return fmt.Errorf("secret trait expires_at exceeds requested expiry")
		}
	}
	if output.ResourceMode == v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED || output.ResourceMode != descriptor.GetResourceMode() {
		return fmt.Errorf("credential resource mode does not match advertised capability")
	}
	if output.Secret.GetId().GetResourceType() != descriptor.GetSecretResourceTypeId() {
		return fmt.Errorf("secret resource type does not match advertised capability")
	}
	if len(output.PlaintextData) == 0 {
		return fmt.Errorf("at least one plaintext value is required")
	}
	names := make(map[string]struct{}, len(output.PlaintextData))
	for i, value := range output.PlaintextData {
		if value == nil || strings.TrimSpace(value.GetName()) == "" || len(value.GetBytes()) == 0 {
			return fmt.Errorf("plaintext value %d must have a name and non-empty bytes", i)
		}
		if _, ok := names[value.GetName()]; ok {
			return fmt.Errorf("duplicate plaintext value name %q", value.GetName())
		}
		names[value.GetName()] = struct{}{}
	}
	return nil
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
