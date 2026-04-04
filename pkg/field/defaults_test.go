package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCMaxMsgSizeField(t *testing.T) {
	t.Run("has correct field name", func(t *testing.T) {
		require.Equal(t, "grpc-max-msg-size", GRPCMaxMsgSizeField.GetName())
	})

	t.Run("default value is 4MB", func(t *testing.T) {
		require.Equal(t, 4*1024*1024, GRPCMaxMsgSizeField.DefaultValue)
	})

	t.Run("is in DefaultFields", func(t *testing.T) {
		require.True(t, IsFieldAmongDefaultList(GRPCMaxMsgSizeField))
	})

	t.Run("is persistent", func(t *testing.T) {
		require.True(t, GRPCMaxMsgSizeField.SyncerConfig.Persistent)
	})

	t.Run("rejects zero value", func(t *testing.T) {
		f := GRPCMaxMsgSizeField
		_, err := ValidateField(&f, 0)
		require.Error(t, err)
	})

	t.Run("rejects negative value", func(t *testing.T) {
		f := GRPCMaxMsgSizeField
		_, err := ValidateField(&f, -1)
		require.Error(t, err)
	})

	t.Run("accepts valid value", func(t *testing.T) {
		f := GRPCMaxMsgSizeField
		_, err := ValidateField(&f, 8*1024*1024)
		require.NoError(t, err)
	})
}
