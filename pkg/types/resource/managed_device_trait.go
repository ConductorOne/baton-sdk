package resource

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

type ManagedDeviceTraitOption func(t *v2.ManagedDeviceTrait) error

// WithManagedDeviceSerial sets the hardware serial, the primary correlation key.
func WithManagedDeviceSerial(serial string) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetSerial(serial)
		return nil
	}
}

// WithManagedDeviceUDID sets the Apple UDID.
func WithManagedDeviceUDID(udid string) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetUdid(udid)
		return nil
	}
}

// WithManagedDeviceHardwareHash sets the Windows Autopilot hardware hash.
func WithManagedDeviceHardwareHash(hardwareHash string) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetHardwareHash(hardwareHash)
		return nil
	}
}

// WithManagedDeviceType sets the device form factor (OCSF device.type_id taxonomy).
func WithManagedDeviceType(deviceType v2.ManagedDeviceTrait_DeviceType) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetDeviceType(deviceType)
		return nil
	}
}

// WithManagedDeviceModel sets the device model (OCSF device.model).
func WithManagedDeviceModel(model string) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetModel(model)
		return nil
	}
}

// WithManagedDeviceVendor sets the device vendor (OCSF device.vendor_name).
func WithManagedDeviceVendor(vendor string) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetVendor(vendor)
		return nil
	}
}

// WithManagedDeviceOS sets the operating system of the device (OCSF device.os).
func WithManagedDeviceOS(os *v2.DeviceOS) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetOs(os)
		return nil
	}
}

// WithManagedDeviceCompliance sets the device compliance state.
func WithManagedDeviceCompliance(compliance v2.ManagedDeviceTrait_Compliance) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetCompliance(compliance)
		return nil
	}
}

// WithManagedDeviceEncrypted sets the nullable disk-encryption state.
func WithManagedDeviceEncrypted(isEncrypted bool) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetIsEncrypted(wrapperspb.Bool(isEncrypted))
		return nil
	}
}

// WithManagedDeviceSupervised sets the nullable supervised state.
func WithManagedDeviceSupervised(isSupervised bool) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetIsSupervised(wrapperspb.Bool(isSupervised))
		return nil
	}
}

// WithManagedDevicePersonal sets the nullable personal-ownership (BYOD) state.
func WithManagedDevicePersonal(isPersonal bool) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetIsPersonal(wrapperspb.Bool(isPersonal))
		return nil
	}
}

// WithManagedDeviceManagementState sets the management lifecycle state.
func WithManagedDeviceManagementState(managementState v2.ManagedDeviceTrait_ManagementState) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetManagementState(managementState)
		return nil
	}
}

// WithManagedDeviceEnrolledAt sets the enrollment timestamp.
func WithManagedDeviceEnrolledAt(enrolledAt time.Time) ManagedDeviceTraitOption {
	return func(t *v2.ManagedDeviceTrait) error {
		t.SetEnrolledAt(timestamppb.New(enrolledAt))
		return nil
	}
}

// NewManagedDeviceTrait creates a new `ManagedDeviceTrait` with the given options.
func NewManagedDeviceTrait(opts ...ManagedDeviceTraitOption) (*v2.ManagedDeviceTrait, error) {
	managedDeviceTrait := &v2.ManagedDeviceTrait{}

	for _, opt := range opts {
		err := opt(managedDeviceTrait)
		if err != nil {
			return nil, err
		}
	}

	return managedDeviceTrait, nil
}
