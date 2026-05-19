package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func randomOpts(length int64) *v2.LocalCredentialOptions_RandomPassword {
	return v2.LocalCredentialOptions_RandomPassword_builder{Length: length}.Build()
}

func TestGenerateRandomPasswordWithPolicy_NilAndZeroValuePolicy(t *testing.T) {
	t.Run("nil policy is equivalent to GenerateRandomPassword", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			p, err := GenerateRandomPasswordWithPolicy(randomOpts(16), nil)
			require.NoError(t, err)
			require.Len(t, p, 16)
		}
	})

	t.Run("zero-value policy is a no-op", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			p, err := GenerateRandomPasswordWithPolicy(randomOpts(16), &RandomPasswordPolicy{})
			require.NoError(t, err)
			require.Len(t, p, 16)
		}
	})

	t.Run("nil request errors", func(t *testing.T) {
		_, err := GenerateRandomPasswordWithPolicy(nil, &RandomPasswordPolicy{})
		require.Error(t, err)
	})

	t.Run("length below 8 still errors", func(t *testing.T) {
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(4), nil)
		require.ErrorIs(t, err, ErrInvalidPasswordLength)
	})
}

func TestGenerateRandomPasswordWithPolicy_ExcludedCharacters(t *testing.T) {
	t.Run("excluded runes never appear", func(t *testing.T) {
		excluded := "!@#$%^&*()0123456789"
		policy := &RandomPasswordPolicy{ExcludedCharacters: excluded}

		for i := 0; i < 200; i++ {
			p, err := GenerateRandomPasswordWithPolicy(randomOpts(32), policy)
			require.NoError(t, err)
			require.Len(t, p, 32)
			for _, r := range excluded {
				require.NotContainsf(t, p, string(r),
					"iteration %d produced %q containing disallowed %q", i, p, string(r))
			}
		}
	})

	t.Run("excluded characters apply to caller-supplied constraints", func(t *testing.T) {
		// Caller says "at least 8 chars from upper+lower+digit set."
		// Policy excludes all digits and most letters; constraint must filter.
		excluded := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvw"
		policy := &RandomPasswordPolicy{ExcludedCharacters: excluded}
		opts := v2.LocalCredentialOptions_RandomPassword_builder{
			Length: 16,
			Constraints: []*v2.PasswordConstraint{
				v2.PasswordConstraint_builder{
					CharSet:  UpperCaseLetters + LowerCaseLetters + Digits,
					MinCount: 8,
				}.Build(),
			},
		}.Build()

		p, err := GenerateRandomPasswordWithPolicy(opts, policy)
		require.NoError(t, err)
		require.Len(t, p, 16)
		for _, r := range excluded {
			require.NotContains(t, p, string(r))
		}
	})

	t.Run("excluding every printable char returns ErrPolicyConflict", func(t *testing.T) {
		var all strings.Builder
		for c := byte(33); c < 127; c++ {
			all.WriteByte(c)
		}
		policy := &RandomPasswordPolicy{ExcludedCharacters: all.String()}

		_, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict),
			"expected ErrPolicyConflict, got %v", err)
	})

	t.Run("excluding every char of a caller constraint returns ErrPolicyConflict", func(t *testing.T) {
		// Constraint requires digits, but policy excludes them all.
		policy := &RandomPasswordPolicy{ExcludedCharacters: Digits}
		opts := v2.LocalCredentialOptions_RandomPassword_builder{
			Length: 16,
			Constraints: []*v2.PasswordConstraint{
				v2.PasswordConstraint_builder{CharSet: Digits, MinCount: 4}.Build(),
			},
		}.Build()

		_, err := GenerateRandomPasswordWithPolicy(opts, policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
	})
}

func TestGenerateRandomPasswordWithPolicy_LengthEnforcement(t *testing.T) {
	t.Run("strict rejects length over max", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MaxLength: 16}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(32), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyViolation))
	})

	t.Run("strict rejects length under min", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MinLength: 20}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyViolation))
	})

	t.Run("clamp shortens to max", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MaxLength: 16, Enforcement: PolicyEnforcementClamp}
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(32), policy)
		require.NoError(t, err)
		require.Len(t, p, 16)
	})

	t.Run("clamp extends to min", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MinLength: 20, Enforcement: PolicyEnforcementClamp}
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(12), policy)
		require.NoError(t, err)
		require.Len(t, p, 20)
	})

	t.Run("warn_only proceeds with original length", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MaxLength: 16, Enforcement: PolicyEnforcementWarnOnly}
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(32), policy)
		require.NoError(t, err)
		require.Len(t, p, 32)
	})

	t.Run("length within bounds passes strict", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MinLength: 12, MaxLength: 32}
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(20), policy)
		require.NoError(t, err)
		require.Len(t, p, 20)
	})

	t.Run("zero bounds are unspecified (no check)", func(t *testing.T) {
		policy := &RandomPasswordPolicy{} // all zeroes
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(20), policy)
		require.NoError(t, err)
		require.Len(t, p, 20)
	})

	t.Run("MinLength greater than MaxLength is an unsatisfiable conflict (Strict)", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MinLength: 20, MaxLength: 16}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(18), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict),
			"expected ErrPolicyConflict, got %v", err)
		require.Contains(t, err.Error(), "MinLength 20 is greater than MaxLength 16")
	})

	t.Run("MinLength greater than MaxLength fails under Clamp (no silent violation)", func(t *testing.T) {
		// With Clamp, naïvely clamping max-then-min would silently return
		// MinLength even though it exceeds MaxLength. The upfront conflict
		// check ensures the call fails loudly.
		policy := &RandomPasswordPolicy{MinLength: 20, MaxLength: 16, Enforcement: PolicyEnforcementClamp}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(12), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
	})

	t.Run("MinLength greater than MaxLength fails under WarnOnly", func(t *testing.T) {
		// Same loud-fail behavior under WarnOnly — the bounds themselves
		// are nonsensical regardless of enforcement mode.
		policy := &RandomPasswordPolicy{MinLength: 20, MaxLength: 16, Enforcement: PolicyEnforcementWarnOnly}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(18), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
	})

	t.Run("MinLength equal to MaxLength is allowed", func(t *testing.T) {
		policy := &RandomPasswordPolicy{MinLength: 16, MaxLength: 16}
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
		require.NoError(t, err)
		require.Len(t, p, 16)
	})
}

func TestGenerateRandomPasswordWithPolicy_RequiredClasses(t *testing.T) {
	t.Run("required classes replace basic validity when no constraints", func(t *testing.T) {
		// Request a password that must include only upper + digit (no lower, no symbol).
		policy := &RandomPasswordPolicy{
			RequiredClasses: []PasswordCharClass{
				PasswordCharClassUpper,
				PasswordCharClassDigit,
			},
		}
		// With this configuration, basic-validity coverage is replaced; the
		// remainder is still filled from the full pool, but the loud guarantee
		// is that at least one upper + one digit are present.
		for i := 0; i < 50; i++ {
			p, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
			require.NoError(t, err)
			require.Len(t, p, 16)
			require.True(t, containsAny(p, UpperCaseLetters), "upper missing in %q", p)
			require.True(t, containsAny(p, Digits), "digit missing in %q", p)
		}
	})

	t.Run("required classes are additive over caller constraints", func(t *testing.T) {
		// Caller constraint already covers digits. Required adds upper.
		policy := &RandomPasswordPolicy{
			RequiredClasses: []PasswordCharClass{PasswordCharClassUpper},
		}
		opts := v2.LocalCredentialOptions_RandomPassword_builder{
			Length: 16,
			Constraints: []*v2.PasswordConstraint{
				v2.PasswordConstraint_builder{CharSet: Digits, MinCount: 4}.Build(),
			},
		}.Build()
		for i := 0; i < 50; i++ {
			p, err := GenerateRandomPasswordWithPolicy(opts, policy)
			require.NoError(t, err)
			require.True(t, containsAny(p, UpperCaseLetters), "upper missing in %q", p)
			require.GreaterOrEqual(t, countOccurences(p, Digits), 4)
		}
	})

	t.Run("required class fully excluded returns ErrPolicyConflict", func(t *testing.T) {
		policy := &RandomPasswordPolicy{
			ExcludedCharacters: Digits,
			RequiredClasses:    []PasswordCharClass{PasswordCharClassDigit},
		}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
		require.Contains(t, err.Error(), "after applying ExcludedCharacters")
	})

	t.Run("PasswordCharClassUnspecified in RequiredClasses returns a tailored ErrPolicyConflict (no constraints)", func(t *testing.T) {
		policy := &RandomPasswordPolicy{
			RequiredClasses: []PasswordCharClass{PasswordCharClassUnspecified},
		}
		_, err := GenerateRandomPasswordWithPolicy(randomOpts(16), policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
		require.Contains(t, err.Error(), "PasswordCharClassUnspecified")
	})

	t.Run("PasswordCharClassUnspecified in RequiredClasses returns a tailored ErrPolicyConflict (with constraints)", func(t *testing.T) {
		policy := &RandomPasswordPolicy{
			RequiredClasses: []PasswordCharClass{PasswordCharClassUpper, PasswordCharClassUnspecified},
		}
		opts := v2.LocalCredentialOptions_RandomPassword_builder{
			Length: 16,
			Constraints: []*v2.PasswordConstraint{
				v2.PasswordConstraint_builder{CharSet: Digits, MinCount: 4}.Build(),
			},
		}.Build()
		_, err := GenerateRandomPasswordWithPolicy(opts, policy)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPolicyConflict))
		require.Contains(t, err.Error(), "PasswordCharClassUnspecified")
	})
}

func TestGenerateRandomPasswordWithPolicy_Combined(t *testing.T) {
	// Realistic config combining all knobs: 24 chars, excluding ambiguous
	// characters, requiring upper + lower + digit (but not symbols).
	policy := &RandomPasswordPolicy{
		MinLength:          16,
		MaxLength:          32,
		ExcludedCharacters: "0Ol1Il|`'\"",
		RequiredClasses: []PasswordCharClass{
			PasswordCharClassUpper,
			PasswordCharClassLower,
			PasswordCharClassDigit,
		},
	}
	for i := 0; i < 100; i++ {
		p, err := GenerateRandomPasswordWithPolicy(randomOpts(24), policy)
		require.NoError(t, err)
		require.Len(t, p, 24)
		for _, r := range policy.ExcludedCharacters {
			require.NotContains(t, p, string(r))
		}
		require.True(t, containsAny(p, UpperCaseLetters), "upper missing in %q", p)
		require.True(t, containsAny(p, LowerCaseLetters), "lower missing in %q", p)
		require.True(t, containsAny(p, Digits), "digit missing in %q", p)
	}
}

func TestGenerateRandomPasswordWithPolicy_ExportedConstants(t *testing.T) {
	// Smoke test: the exported constants are the strings we expect.
	require.Equal(t, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", UpperCaseLetters)
	require.Equal(t, "abcdefghijklmnopqrstuvwxyz", LowerCaseLetters)
	require.Equal(t, "0123456789", Digits)
	require.Equal(t, "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", Symbols)
}
