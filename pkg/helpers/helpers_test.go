package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHelpers_SplitFullName(t *testing.T) {
	firstName, lastName := SplitFullName("Prince")
	require.Equal(t, "Prince", firstName)
	require.Equal(t, "", lastName)

	firstName, lastName = SplitFullName("John Smith")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Smith", lastName)

	firstName, lastName = SplitFullName("John Jacob Jingleheimer Schmidt")
	require.Equal(t, "John", firstName)
	require.Equal(t, "Jacob Jingleheimer Schmidt", lastName)
}
