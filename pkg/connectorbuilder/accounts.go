package connectorbuilder

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CreateAccountResponse is a semi-opaque type returned from CreateAccount operations.
//
// This is used to communicate the result of account creation back to Baton.
type CreateAccountResponse interface {
	proto.Message
	GetIsCreateAccountResult() bool
}

// AccountManager extends ResourceSyncer to add capabilities for managing user accounts.
//
// Implementing this interface indicates the connector supports creating accounts
// in the external system. A resource type should implement this interface if it
// represents users or accounts that can be provisioned.
type AccountManager interface {
	ResourceSyncer
	AccountManagerLimited
}

type AccountManagerV2 interface {
	ResourceSyncerV2
	AccountManagerLimited
}

type AccountManagerLimited interface {
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.LocalCredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
	CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error)
}

type OldAccountManager interface {
	ResourceSyncer
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.CredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
}

func (b *builder) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.CreateAccount")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateAccountType
	l := ctxzap.Extract(ctx)

	if len(b.accountManagers) == 0 {
		l.Error("error: connector does not have account manager configured")
		err := status.Error(codes.Unimplemented, "connector does not have account manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	var accountManager AccountManagerLimited
	if request.GetResourceTypeId() == "" {
		if len(b.accountManagers) == 1 {
			// If there's only one account manager, use it.
			for _, am := range b.accountManagers {
				accountManager = am
				break
			}
		} else {
			// If there are multiple account managers, default to user resource type.
			var ok bool
			accountManager, ok = b.accountManagers["user"]
			if !ok {
				err := status.Error(codes.Unimplemented, "connector has multiple account managers configured, but no resource type specified, and no default account manager configured")
				b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
				return nil, err
			}
		}
	}

	// If resource type is specified, use the account manager for that resource type.
	if accountManager == nil {
		var ok bool
		accountManager, ok = b.accountManagers[request.GetResourceTypeId()]
		if !ok {
			l.Error("error: connector does not have account manager configured")
			err := status.Errorf(codes.Unimplemented, "connector does not have account manager configured for resource type: %s", request.GetResourceTypeId())
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
	}

	opts, err := crypto.ConvertCredentialOptions(ctx, b.clientSecret, request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: converting credential options failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: converting credential options failed: %w", err)
	}

	result, plaintexts, annos, err := accountManager.CreateAccount(ctx, request.GetAccountInfo(), opts)
	if err != nil {
		l.Error("error: create account failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: create account failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	rv := v2.CreateAccountResponse_builder{
		EncryptedData: encryptedDatas,
		Annotations:   annos,
	}.Build()

	switch r := result.(type) {
	case *v2.CreateAccountResponse_SuccessResult:
		rv.SetSuccess(proto.ValueOrDefault(r))
	case *v2.CreateAccountResponse_ActionRequiredResult:
		rv.SetActionRequired(proto.ValueOrDefault(r))
	case *v2.CreateAccountResponse_AlreadyExistsResult:
		rv.SetAlreadyExists(proto.ValueOrDefault(r))
	case *v2.CreateAccountResponse_InProgressResult:
		rv.SetInProgress(proto.ValueOrDefault(r))
	default:
		err := status.Error(codes.Unimplemented, fmt.Sprintf("unknown result type: %T", result))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start), err)
		return nil, err
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builder) addAccountManager(_ context.Context, typeId string, in any) error {
	if _, ok := in.(OldAccountManager); ok {
		return fmt.Errorf("error: old account manager interface implemented for %s", typeId)
	}

	if accountManager, ok := in.(AccountManagerLimited); ok {
		if _, ok := b.accountManagers[typeId]; ok {
			return fmt.Errorf("error: duplicate resource type found for account manager %s", typeId)
		}
		b.accountManagers[typeId] = accountManager
	}
	return nil
}
