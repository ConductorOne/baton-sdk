package cli

import "github.com/spf13/viper"

func (s *ManagedRuntimeSnapshot) AssetByRole(role string) *ManagedRuntimeAsset {
	if s == nil {
		return nil
	}
	for i := range s.Assets {
		if s.Assets[i].Role == role {
			return &s.Assets[i]
		}
	}
	return nil
}

func (s *ManagedRuntimeSnapshot) GetRevision() string {
	if s == nil {
		return ""
	}
	return s.Revision
}

func CloneViperSettings(source *viper.Viper) *viper.Viper {
	clone := viper.New()
	for key, value := range source.AllSettings() {
		clone.Set(key, value)
	}
	return clone
}
