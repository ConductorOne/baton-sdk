package resource

import (
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestUserTrait(t *testing.T) {
	ut, err := NewUserTrait()
	require.NoError(t, err)
	require.Nil(t, ut.GetProfile())
	require.Nil(t, ut.GetIcon())
	require.Nil(t, ut.GetEmails())
	require.NotNil(t, ut.GetStatus())
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.GetStatus().GetStatus())

	ut, err = NewUserTrait(WithUserIcon(v2.AssetRef_builder{Id: "iconID"}.Build()))
	require.NoError(t, err)
	require.Nil(t, ut.GetProfile())
	require.Nil(t, ut.GetEmails())
	require.NotNil(t, ut.GetStatus())
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.GetStatus().GetStatus())

	require.NotNil(t, ut.GetIcon())
	require.Equal(t, "iconID", ut.GetIcon().GetId())

	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.GetAccountType())

	userProfile := make(map[string]interface{})
	userProfile["test"] = "user-profile-field"

	ut, err = NewUserTrait(
		WithUserIcon(v2.AssetRef_builder{Id: "iconID"}.Build()),
		WithUserProfile(userProfile),
	)
	require.NoError(t, err)
	require.Nil(t, ut.GetEmails())
	require.NotNil(t, ut.GetStatus())
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.GetStatus().GetStatus())

	require.NotNil(t, ut.GetIcon())
	require.Equal(t, "iconID", ut.GetIcon().GetId())
	require.NotNil(t, ut.GetProfile())
	val, ok := GetProfileStringValue(ut.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	val, ok = GetProfileStringValue(ut.GetProfile(), "no-key")
	require.False(t, ok)
	require.Empty(t, val)
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.GetAccountType())

	ut, err = NewUserTrait(
		WithUserIcon(v2.AssetRef_builder{Id: "iconID"}.Build()),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_DISABLED),
	)
	require.NoError(t, err)
	require.Nil(t, ut.GetEmails())

	require.NotNil(t, ut.GetStatus())
	require.Equal(t, v2.UserTrait_Status_STATUS_DISABLED, ut.GetStatus().GetStatus())
	require.NotNil(t, ut.GetIcon())
	require.Equal(t, "iconID", ut.GetIcon().GetId())
	require.NotNil(t, ut.GetProfile())
	val, ok = GetProfileStringValue(ut.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.GetAccountType())

	now := time.Now().UTC()
	ut, err = NewUserTrait(
		WithUserIcon(v2.AssetRef_builder{Id: "iconID"}.Build()),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_UNSPECIFIED),
		WithEmail("alice@example.com", true),
		WithEmail("bob@example.com", false),
		WithCreatedAt(now),
		WithLastLogin(now),
		WithMFAStatus(v2.UserTrait_MFAStatus_builder{MfaEnabled: true}.Build()),
		WithSSOStatus(v2.UserTrait_SSOStatus_builder{SsoEnabled: true}.Build()),
	)
	require.NoError(t, err)

	require.NotNil(t, ut.GetStatus())
	require.Equal(t, v2.UserTrait_Status_STATUS_UNSPECIFIED, ut.GetStatus().GetStatus())
	require.NotNil(t, ut.GetIcon())
	require.Equal(t, "iconID", ut.GetIcon().GetId())
	require.NotNil(t, ut.GetProfile())
	val, ok = GetProfileStringValue(ut.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Len(t, ut.GetEmails(), 2)
	require.True(t, ut.GetEmails()[0].GetIsPrimary())
	require.Equal(t, ut.GetEmails()[0].GetAddress(), "alice@example.com")
	require.False(t, ut.GetEmails()[1].GetIsPrimary())
	require.Equal(t, ut.GetEmails()[1].GetAddress(), "bob@example.com")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.GetAccountType())
	require.Equal(t, now, ut.GetCreatedAt().AsTime())
	require.Equal(t, now, ut.GetLastLogin().AsTime())
	require.Equal(t, true, ut.GetMfaStatus().GetMfaEnabled())
	require.Equal(t, true, ut.GetSsoStatus().GetSsoEnabled())

	ut, err = NewUserTrait(
		WithUserIcon(v2.AssetRef_builder{Id: "iconID"}.Build()),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_UNSPECIFIED),
		WithEmail("alice@example.com", true),
		WithEmail("bob@example.com", false),
		WithAccountType(v2.UserTrait_ACCOUNT_TYPE_SERVICE),
	)
	require.NoError(t, err)

	require.NotNil(t, ut.GetStatus())
	require.Equal(t, v2.UserTrait_Status_STATUS_UNSPECIFIED, ut.GetStatus().GetStatus())
	require.NotNil(t, ut.GetIcon())
	require.Equal(t, "iconID", ut.GetIcon().GetId())
	require.NotNil(t, ut.GetProfile())
	val, ok = GetProfileStringValue(ut.GetProfile(), "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Len(t, ut.GetEmails(), 2)
	require.True(t, ut.GetEmails()[0].GetIsPrimary())
	require.Equal(t, ut.GetEmails()[0].GetAddress(), "alice@example.com")
	require.False(t, ut.GetEmails()[1].GetIsPrimary())
	require.Equal(t, ut.GetEmails()[1].GetAddress(), "bob@example.com")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_SERVICE, ut.GetAccountType())
}
