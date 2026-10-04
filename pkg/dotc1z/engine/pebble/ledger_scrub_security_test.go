package pebble

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestSecurity_LedgerNilChildIdentityRejected: a v3 .c1z is hostile input,
// and its ledger rows are read back from the file. A LedgerRow child whose
// identity submessage is absent is a well-formed proto encoding, but
// scrubLedgerRow wrote through the nil identity (the generated setter
// dereferences its receiver), panicking EndSyncWithStats on the sealing
// path. Malformed records must be rejected with an error, never
// dereferenced.
func TestSecurity_LedgerNilChildIdentityRejected(t *testing.T) {
	row := &v3.LedgerRow{}
	row.Children = append(row.Children, &v3.LedgerChild{}) // no identity submessage

	// Pre-fix: this panics inside the generated setter (nil receiver).
	// Post-fix: the scrub returns an error for the malformed row.
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errPanicked{r}
			}
		}()
		_, err = scrubLedgerRow(row)
		return err
	}()
	require.Error(t, err, "a ledger child without an identity must be rejected, not dereferenced")
	var wantPanic errPanicked
	if errors.As(err, &wantPanic) {
		t.Fatalf("scrub panicked instead of returning an error: %v", err)
	}
}

// TestSecurity_LedgerWellFormedRowsStillScrub is the control: rows written
// by this SDK (identity present on every child) scrub exactly as before.
func TestSecurity_LedgerWellFormedRowsStillScrub(t *testing.T) {
	row := &v3.LedgerRow{}
	row.SetNextPageToken("secret-token")
	row.Children = append(row.Children, &v3.LedgerChild{})
	row.Children[0].SetIdentity(&v3.LedgerActionIdentity{PageToken: "child-token"})

	changed, err := scrubLedgerRow(row)
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, row.GetNextPageToken())
	require.NotEmpty(t, row.GetNextPageTokenHash())
	require.Empty(t, row.GetChildren()[0].GetIdentity().GetPageToken())
	require.NotEmpty(t, row.GetChildren()[0].GetIdentity().GetPageTokenHash())
}

// errPanicked marks a panic that escaped the scrub, so the test can fail
// with the panic instead of a bare nil-error pass.
type errPanicked struct{ v any }

func (e errPanicked) Error() string { return fmt.Sprintf("panicked: %v", e.v) }
