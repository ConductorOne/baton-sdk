package v2

import (
	"encoding/json"
	"fmt"
	"io/fs"
)

// Manifest represents the manifest structure for V2 format.
type Manifest struct {
	Version  string                 `json:"version"`
	Backends map[string]BackendInfo `json:"backends"`
	Metadata map[string]string      `json:"metadata,omitempty"`
}

// BackendInfo contains information about a backend in V2 format.
type BackendInfo struct {
	Name     string            `json:"name"`
	Path     string            `json:"path"`
	Engine   string            `json:"engine"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ReadManifest reads the manifest from the V2File's working directory using the safe DirFS.
func (c *V2File) ReadManifest() (*Manifest, error) {
	data, err := fs.ReadFile(c.workingDirFS.FS(), "manifest.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest.json: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest.json: %w", err)
	}

	return &manifest, nil
}

// WriteManifest writes a manifest to the specified working directory.
func (c *V2File) WriteManifest(manifest *Manifest) error {
	if manifest == nil {
		return fmt.Errorf("manifest cannot be nil")
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest.json: %w", err)
	}

	mf, err := c.workingDirFS.Create("manifest.json")
	if err != nil {
		return fmt.Errorf("failed to create manifest.json: %w", err)
	}

	_, err = mf.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write manifest.json: %w", err)
	}

	return nil
}

// CreateDefaultManifest creates a default manifest with SQLite as the default engine.
func CreateDefaultManifest(engineType string) *Manifest {
	if engineType == "" {
		engineType = "sqlite"
	}

	return &Manifest{
		Version: "2.0",
		Backends: map[string]BackendInfo{
			"default": {
				Name:     "default",
				Path:     "default-sqlite",
				Engine:   engineType,
				Metadata: make(map[string]string),
			},
		},
		Metadata: make(map[string]string),
	}
}
