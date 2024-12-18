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
	require.Nil(t, ut.Profile)
	require.Nil(t, ut.Icon)
	require.Nil(t, ut.Emails)
	require.NotNil(t, ut.Status)
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.Status.Status)

	ut, err = NewUserTrait(WithUserIcon(&v2.AssetRef{Id: "iconID"}))
	require.NoError(t, err)
	require.Nil(t, ut.Profile)
	require.Nil(t, ut.Emails)
	require.NotNil(t, ut.Status)
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.Status.Status)

	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)

	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.AccountType)

	userProfile := make(map[string]interface{})
	userProfile["test"] = "user-profile-field"

	ut, err = NewUserTrait(
		WithUserIcon(&v2.AssetRef{Id: "iconID"}),
		WithUserProfile(userProfile),
	)
	require.NoError(t, err)
	require.Nil(t, ut.Emails)
	require.NotNil(t, ut.Status)
	// User traits default to enabled
	require.Equal(t, v2.UserTrait_Status_STATUS_ENABLED, ut.Status.Status)

	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)
	require.NotNil(t, ut.Profile)
	val, ok := GetProfileStringValue(ut.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	val, ok = GetProfileStringValue(ut.Profile, "no-key")
	require.False(t, ok)
	require.Empty(t, val)
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.AccountType)

	ut, err = NewUserTrait(
		WithUserIcon(&v2.AssetRef{Id: "iconID"}),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_DISABLED),
	)
	require.NoError(t, err)
	require.Nil(t, ut.Emails)

	require.NotNil(t, ut.Status)
	require.Equal(t, v2.UserTrait_Status_STATUS_DISABLED, ut.Status.Status)
	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)
	require.NotNil(t, ut.Profile)
	val, ok = GetProfileStringValue(ut.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.AccountType)

	now := time.Now().UTC()
	ut, err = NewUserTrait(
		WithUserIcon(&v2.AssetRef{Id: "iconID"}),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_UNSPECIFIED),
		WithEmail("alice@example.com", true),
		WithEmail("bob@example.com", false),
		WithCreatedAt(now),
		WithLastLogin(now),
		WithMFAStatus(&v2.UserTrait_MFAStatus{MfaEnabled: true}),
		WithSSOStatus(&v2.UserTrait_SSOStatus{SsoEnabled: true}),
	)
	require.NoError(t, err)

	require.NotNil(t, ut.Status)
	require.Equal(t, v2.UserTrait_Status_STATUS_UNSPECIFIED, ut.Status.Status)
	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)
	require.NotNil(t, ut.Profile)
	val, ok = GetProfileStringValue(ut.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Len(t, ut.Emails, 2)
	require.True(t, ut.Emails[0].IsPrimary)
	require.Equal(t, ut.Emails[0].Address, "alice@example.com")
	require.False(t, ut.Emails[1].IsPrimary)
	require.Equal(t, ut.Emails[1].Address, "bob@example.com")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, ut.AccountType)
	require.Equal(t, now, ut.CreatedAt.AsTime())
	require.Equal(t, now, ut.LastLogin.AsTime())
	require.Equal(t, true, ut.MfaStatus.MfaEnabled)
	require.Equal(t, true, ut.SsoStatus.SsoEnabled)

	ut, err = NewUserTrait(
		WithUserIcon(&v2.AssetRef{Id: "iconID"}),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_UNSPECIFIED),
		WithEmail("alice@example.com", true),
		WithEmail("bob@example.com", false),
		WithAccountType(v2.UserTrait_ACCOUNT_TYPE_SERVICE),
	)
	require.NoError(t, err)

	require.NotNil(t, ut.Status)
	require.Equal(t, v2.UserTrait_Status_STATUS_UNSPECIFIED, ut.Status.Status)
	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)
	require.NotNil(t, ut.Profile)
	val, ok = GetProfileStringValue(ut.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Len(t, ut.Emails, 2)
	require.True(t, ut.Emails[0].IsPrimary)
	require.Equal(t, ut.Emails[0].Address, "alice@example.com")
	require.False(t, ut.Emails[1].IsPrimary)
	require.Equal(t, ut.Emails[1].Address, "bob@example.com")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_SERVICE, ut.AccountType)

	ut, err = NewUserTrait(
		WithUserIcon(&v2.AssetRef{Id: "iconID"}),
		WithUserProfile(userProfile),
		WithStatus(v2.UserTrait_Status_STATUS_UNSPECIFIED),
		WithEmail("alice@example.com", true),
		WithEmail("bob@example.com", false),
		WithAccountType(v2.UserTrait_ACCOUNT_TYPE_AI_AGENT),
	)
	require.NoError(t, err)

	require.NotNil(t, ut.Status)
	require.Equal(t, v2.UserTrait_Status_STATUS_UNSPECIFIED, ut.Status.Status)
	require.NotNil(t, ut.Icon)
	require.Equal(t, "iconID", ut.Icon.Id)
	require.NotNil(t, ut.Profile)
	val, ok = GetProfileStringValue(ut.Profile, "test")
	require.True(t, ok)
	require.Equal(t, "user-profile-field", val)
	require.Len(t, ut.Emails, 2)
	require.True(t, ut.Emails[0].IsPrimary)
	require.Equal(t, ut.Emails[0].Address, "alice@example.com")
	require.False(t, ut.Emails[1].IsPrimary)
	require.Equal(t, ut.Emails[1].Address, "bob@example.com")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_AI_AGENT, ut.AccountType)
}
