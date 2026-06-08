package cli

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
)

// metadataConnector embeds types.ConnectorServer so it satisfies the interface without
// implementing every method. It deliberately does NOT implement GetCapabilities, so
// capabilitiesFromConnector must fall back to GetMetadata.
type metadataConnector struct {
	types.ConnectorServer

	resp *v2.ConnectorServiceGetMetadataResponse
	err  error
}

func (m *metadataConnector) GetMetadata(_ context.Context, _ *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return m.resp, m.err
}

// getterConnector implements the optional GetCapabilities getter. capabilitiesFromConnector
// should use it and never call GetMetadata (which would panic via the nil embedded interface).
type getterConnector struct {
	types.ConnectorServer

	caps *v2.ConnectorCapabilities
	err  error
}

func (g *getterConnector) GetCapabilities(_ context.Context) (*v2.ConnectorCapabilities, error) {
	return g.caps, g.err
}

// closableConnector records whether Close was called and can return an error from it.
type closableConnector struct {
	types.ConnectorServer

	closeErr    error
	closeCalled bool
}

func (c *closableConnector) Close(_ context.Context) error {
	c.closeCalled = true
	return c.closeErr
}

func metadataWithCapabilities(caps *v2.ConnectorCapabilities) *v2.ConnectorServiceGetMetadataResponse {
	return v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: v2.ConnectorMetadata_builder{
			DisplayName:  "test",
			Capabilities: caps,
		}.Build(),
	}.Build()
}

func TestCapabilitiesFromConnector_UsesGetterWhenAvailable(t *testing.T) {
	want := v2.ConnectorCapabilities_builder{
		ConnectorCapabilities: []v2.Capability{v2.Capability_CAPABILITY_SYNC},
	}.Build()

	c := &getterConnector{caps: want}

	got, err := capabilitiesFromConnector(context.Background(), c)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestCapabilitiesFromConnector_GetterError(t *testing.T) {
	wantErr := errors.New("boom")
	c := &getterConnector{err: wantErr}

	_, err := capabilitiesFromConnector(context.Background(), c)
	require.ErrorIs(t, err, wantErr)
}

func TestCapabilitiesFromConnector_FallsBackToMetadata(t *testing.T) {
	want := v2.ConnectorCapabilities_builder{
		ConnectorCapabilities: []v2.Capability{v2.Capability_CAPABILITY_PROVISION},
	}.Build()

	c := &metadataConnector{resp: metadataWithCapabilities(want)}

	got, err := capabilitiesFromConnector(context.Background(), c)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestCapabilitiesFromConnector_MetadataError(t *testing.T) {
	wantErr := errors.New("metadata failed")
	c := &metadataConnector{err: wantErr}

	_, err := capabilitiesFromConnector(context.Background(), c)
	require.ErrorIs(t, err, wantErr)
}

func TestCapabilitiesFromConnector_MetadataWithoutCapabilities(t *testing.T) {
	// Metadata present but no capabilities set -> explicit "not supported" error.
	c := &metadataConnector{resp: metadataWithCapabilities(nil)}

	_, err := capabilitiesFromConnector(context.Background(), c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support capabilities")
}

func TestCloseIfPossible_CallsClose(t *testing.T) {
	c := &closableConnector{}
	closeIfPossible(context.Background(), c)
	require.True(t, c.closeCalled)
}

func TestCloseIfPossible_SwallowsCloseError(t *testing.T) {
	c := &closableConnector{closeErr: errors.New("close failed")}
	// Must not panic and must still attempt the close.
	require.NotPanics(t, func() { closeIfPossible(context.Background(), c) })
	require.True(t, c.closeCalled)
}

func TestCloseIfPossible_NoCloseMethodIsNoOp(t *testing.T) {
	// metadataConnector does not implement Close; this should be a safe no-op.
	c := &metadataConnector{}
	require.NotPanics(t, func() { closeIfPossible(context.Background(), c) })
}
