package config

import (
	"errors"
	"path/filepath"
	"strings"
)

func CleanOrGetConfigPath(customPath string) (string, string, error) {
	if customPath != "" {
		cfgDir, cfgFile := filepath.Split(filepath.Clean(customPath))
		if cfgDir == "" {
			cfgDir = "."
		}

		ext := filepath.Ext(cfgFile)
		if ext == "" || (ext != ".yaml" && ext != ".yml") {
			return "", "", errors.New("expected config file to have .yaml or .yml extension")
		}

		return strings.TrimSuffix(cfgDir, string(filepath.Separator)), strings.TrimSuffix(cfgFile, ext), nil
	}

	return ".", ".baton", nil
}
