package resource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// TestNewManagedDeviceTrait_FullPopulation exercises every option setter to
// confirm fields round-trip through the proto correctly.
func TestNewManagedDeviceTrait_FullPopulation(t *testing.T) {
	enrolledAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	os := &v2.DeviceOS{}
	os.SetType(v2.DeviceOS_OS_TYPE_MACOS)
	os.SetName("macOS")
	os.SetVersion("14.5")
	os.SetBuild_("23F79")

	trait, err := NewManagedDeviceTrait(
		WithManagedDeviceSerial("C02XL0THJGH5"),
		WithManagedDeviceUDID("00008110-000A4D8E0C8A801E"),
		WithManagedDeviceHardwareHash("T0RD1234"),
		WithManagedDeviceType(v2.ManagedDeviceTrait_DEVICE_TYPE_LAPTOP),
		WithManagedDeviceModel("MacBookPro18,3"),
		WithManagedDeviceVendor("Apple"),
		WithManagedDeviceOS(os),
		WithManagedDeviceCompliance(v2.ManagedDeviceTrait_COMPLIANCE_COMPLIANT),
		WithManagedDeviceEncrypted(true),
		WithManagedDeviceSupervised(false),
		WithManagedDevicePersonal(false),
		WithManagedDeviceManagementState(v2.ManagedDeviceTrait_MANAGEMENT_STATE_MANAGED),
		WithManagedDeviceEnrolledAt(enrolledAt),
	)
	require.NoError(t, err)

	require.Equal(t, "C02XL0THJGH5", trait.GetSerial())
	require.Equal(t, "00008110-000A4D8E0C8A801E", trait.GetUdid())
	require.Equal(t, "T0RD1234", trait.GetHardwareHash())
	require.Equal(t, v2.ManagedDeviceTrait_DEVICE_TYPE_LAPTOP, trait.GetDeviceType())
	require.Equal(t, "MacBookPro18,3", trait.GetModel())
	require.Equal(t, "Apple", trait.GetVendor())
	require.Equal(t, v2.DeviceOS_OS_TYPE_MACOS, trait.GetOs().GetType())
	require.Equal(t, "macOS", trait.GetOs().GetName())
	require.Equal(t, "14.5", trait.GetOs().GetVersion())
	require.Equal(t, "23F79", trait.GetOs().GetBuild_())
	require.Equal(t, v2.ManagedDeviceTrait_COMPLIANCE_COMPLIANT, trait.GetCompliance())
	require.Equal(t, v2.ManagedDeviceTrait_MANAGEMENT_STATE_MANAGED, trait.GetManagementState())
	require.True(t, enrolledAt.Equal(trait.GetEnrolledAt().AsTime()))
}

// TestManagedDeviceTrait_BoolValuesNullable verifies the disk-encryption,
// supervised and personal flags are true tri-state BoolValues: unset stays
// nil (distinguishable from false), while an explicit false is wrapped.
func TestManagedDeviceTrait_BoolValuesNullable(t *testing.T) {
	t.Run("unset flags remain nil", func(t *testing.T) {
		trait, err := NewManagedDeviceTrait(WithManagedDeviceSerial("SN"))
		require.NoError(t, err)
		require.Nil(t, trait.GetIsEncrypted())
		require.Nil(t, trait.GetIsSupervised())
		require.Nil(t, trait.GetIsPersonal())
	})

	t.Run("explicit false is wrapped, not dropped", func(t *testing.T) {
		trait, err := NewManagedDeviceTrait(
			WithManagedDeviceEncrypted(false),
			WithManagedDeviceSupervised(true),
			WithManagedDevicePersonal(false),
		)
		require.NoError(t, err)
		require.NotNil(t, trait.GetIsEncrypted())
		require.False(t, trait.GetIsEncrypted().GetValue())
		require.NotNil(t, trait.GetIsSupervised())
		require.True(t, trait.GetIsSupervised().GetValue())
		require.NotNil(t, trait.GetIsPersonal())
		require.False(t, trait.GetIsPersonal().GetValue())
	})
}

// TestNewManagedDeviceResource_RoundTrip builds a resource via
// NewManagedDeviceResource and re-extracts the ManagedDeviceTrait from the
// resource annotations bag, locking in the annotation-packing behavior that
// downstream device-inventory connectors will depend on.
func TestNewManagedDeviceResource_RoundTrip(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("device")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_MANAGED_DEVICE})

	enrolledAt := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	os := &v2.DeviceOS{}
	os.SetType(v2.DeviceOS_OS_TYPE_WINDOWS)
	os.SetName("Windows 11 Pro")

	r, err := NewManagedDeviceResource(
		"Bob's Laptop",
		rt,
		"device-uuid",
		[]ManagedDeviceTraitOption{
			WithManagedDeviceSerial("SN-12345"),
			WithManagedDeviceType(v2.ManagedDeviceTrait_DEVICE_TYPE_LAPTOP),
			WithManagedDeviceOS(os),
			WithManagedDeviceEncrypted(true),
			WithManagedDeviceEnrolledAt(enrolledAt),
		},
	)
	require.NoError(t, err)

	// Re-extract the trait out of the annotations bag.
	got := &v2.ManagedDeviceTrait{}
	annos := annotations.Annotations(r.GetAnnotations())
	ok, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, ok, "ManagedDeviceTrait annotation should be present")

	require.Equal(t, "SN-12345", got.GetSerial())
	require.Equal(t, v2.ManagedDeviceTrait_DEVICE_TYPE_LAPTOP, got.GetDeviceType())
	require.Equal(t, v2.DeviceOS_OS_TYPE_WINDOWS, got.GetOs().GetType())
	require.Equal(t, "Windows 11 Pro", got.GetOs().GetName())
	require.NotNil(t, got.GetIsEncrypted())
	require.True(t, got.GetIsEncrypted().GetValue())
	require.True(t, enrolledAt.Equal(got.GetEnrolledAt().AsTime()))
}

// TestWithManagedDeviceTrait_UpdatesExistingAnnotation confirms merge
// semantics: applying the option a second time updates fields rather than
// replacing the whole trait.
func TestWithManagedDeviceTrait_UpdatesExistingAnnotation(t *testing.T) {
	rt := &v2.ResourceType{}
	rt.SetId("device")
	rt.SetTraits([]v2.ResourceType_Trait{v2.ResourceType_TRAIT_MANAGED_DEVICE})

	r, err := NewResource(
		"Device",
		rt,
		"device-uuid",
		WithManagedDeviceTrait(
			WithManagedDeviceSerial("SN-12345"),
		),
	)
	require.NoError(t, err)

	require.NoError(t, WithManagedDeviceTrait(
		WithManagedDeviceVendor("Dell"),
	)(r))

	got := &v2.ManagedDeviceTrait{}
	annos := annotations.Annotations(r.GetAnnotations())
	ok, err := annos.Pick(got)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "SN-12345", got.GetSerial(), "prior field should survive the update")
	require.Equal(t, "Dell", got.GetVendor())
}

// TestManagedDeviceTrait_Validate exercises the proto-generated Validate
// method directly, pinning the defined_only enum rules on the trait's enum
// fields and DeviceOS.
func TestManagedDeviceTrait_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*v2.ManagedDeviceTrait)
		wantErr string
	}{
		{
			name:   "empty trait is valid",
			mutate: func(_ *v2.ManagedDeviceTrait) {},
		},
		{
			name: "all defined enums valid",
			mutate: func(tr *v2.ManagedDeviceTrait) {
				tr.SetDeviceType(v2.ManagedDeviceTrait_DEVICE_TYPE_LAPTOP)
				tr.SetCompliance(v2.ManagedDeviceTrait_COMPLIANCE_COMPLIANT)
				tr.SetManagementState(v2.ManagedDeviceTrait_MANAGEMENT_STATE_MANAGED)
			},
		},
		{
			name: "undefined device_type rejected",
			mutate: func(tr *v2.ManagedDeviceTrait) {
				tr.SetDeviceType(v2.ManagedDeviceTrait_DeviceType(1234))
			},
			wantErr: "DeviceType",
		},
		{
			name: "undefined compliance rejected",
			mutate: func(tr *v2.ManagedDeviceTrait) {
				tr.SetCompliance(v2.ManagedDeviceTrait_Compliance(1234))
			},
			wantErr: "Compliance",
		},
		{
			name: "undefined management_state rejected",
			mutate: func(tr *v2.ManagedDeviceTrait) {
				tr.SetManagementState(v2.ManagedDeviceTrait_ManagementState(1234))
			},
			wantErr: "ManagementState",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &v2.ManagedDeviceTrait{}
			tc.mutate(tr)
			err := tr.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestDeviceOS_Validate pins the defined_only rule on DeviceOS.type.
func TestDeviceOS_Validate(t *testing.T) {
	t.Run("defined os type valid", func(t *testing.T) {
		os := &v2.DeviceOS{}
		os.SetType(v2.DeviceOS_OS_TYPE_IOS)
		require.NoError(t, os.Validate())
	})

	t.Run("undefined os type rejected", func(t *testing.T) {
		os := &v2.DeviceOS{}
		os.SetType(v2.DeviceOS_OsType(1234))
		err := os.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Type")
	})
}
