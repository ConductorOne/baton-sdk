package crypto //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"errors"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// RandomPasswordPolicy is a connector-local policy describing how a random
// password should be generated. It is intentionally NOT a proto type — it
// lives only in connector code and the SDK. The C1 backend has no involvement
// in supplying or merging this policy at the wire level.
//
// Zero-valued policies are no-ops; callers can construct an empty
// RandomPasswordPolicy and pass it without surprise. A nil *RandomPasswordPolicy
// is treated identically to a zero-valued one.
type RandomPasswordPolicy struct {
	// Inclusive length bounds. Zero means "unspecified."
	//
	// When MinLength is non-zero and the request's Length is below it, behavior
	// follows Enforcement: Strict rejects with ErrPolicyViolation, Clamp clamps
	// up to MinLength, WarnOnly proceeds with the request's Length.
	//
	// When MaxLength is non-zero and the request's Length exceeds it, the same
	// Enforcement rules apply (Strict rejects, Clamp clamps down).
	MinLength int64
	MaxLength int64

	// ExcludedCharacters lists runes that must not appear anywhere in the
	// generated password. The exclusion is applied uniformly to:
	//   - The basic-validity character classes (upper / lower / digits / symbols).
	//   - Each PasswordConstraint.CharSet in the request.
	//   - The remainder pool used to fill the password to its requested length.
	//
	// If filtering empties a constraint's char set or the overall remainder
	// pool, GenerateRandomPasswordWithPolicy returns ErrPolicyConflict.
	ExcludedCharacters string

	// RequiredClasses lists character classes that must each contribute at
	// least one character to the generated password.
	//
	// When the request has no PasswordConstraints and RequiredClasses is set,
	// RequiredClasses replaces the default basic-validity coverage (which
	// otherwise picks one character from each of upper / lower / digits /
	// symbols). When the request has PasswordConstraints, RequiredClasses is
	// applied additively: if a required class is not already covered by the
	// constraints, one character from that class is added.
	//
	// A class that is fully excluded by ExcludedCharacters cannot be satisfied;
	// this returns ErrPolicyConflict.
	RequiredClasses []PasswordCharClass

	// Enforcement controls how violations of MinLength / MaxLength are
	// handled. Excluded-character / required-class conflicts always return
	// ErrPolicyConflict regardless of Enforcement, since they cannot be
	// silently resolved.
	Enforcement PolicyEnforcement
}

// PasswordCharClass enumerates the character classes recognized by
// RandomPasswordPolicy.RequiredClasses.
type PasswordCharClass int

const (
	PasswordCharClassUnspecified PasswordCharClass = iota
	PasswordCharClassUpper
	PasswordCharClassLower
	PasswordCharClassDigit
	PasswordCharClassSymbol
)

// PolicyEnforcement controls how length-bound violations are handled by
// GenerateRandomPasswordWithPolicy. Character-set conflicts (ExcludedCharacters
// or RequiredClasses) always fail loudly regardless of mode.
type PolicyEnforcement int

const (
	// PolicyEnforcementStrict (the zero value, and the default) rejects any
	// request that violates the policy's length bounds.
	PolicyEnforcementStrict PolicyEnforcement = iota

	// PolicyEnforcementClamp silently clamps the request's Length into
	// [MinLength, MaxLength].
	PolicyEnforcementClamp

	// PolicyEnforcementWarnOnly proceeds with the request's Length unchanged
	// even when it violates the bounds. Intended for migration windows only.
	PolicyEnforcementWarnOnly
)

// ErrPolicyViolation is returned when a request violates a Strict-mode policy.
var ErrPolicyViolation = errors.New("password options violate policy")

// ErrPolicyConflict is returned when the policy itself is internally
// unsatisfiable, e.g., ExcludedCharacters removes every rune of a required
// character class.
var ErrPolicyConflict = errors.New("password policy is unsatisfiable")

// GenerateRandomPasswordWithPolicy generates a random password using the same
// algorithm as GenerateRandomPassword, additionally enforcing the supplied
// policy. When policy is nil or zero-valued, output is equivalent to
// GenerateRandomPassword(rp).
//
// The algorithm:
//  1. Apply length policy (Strict / Clamp / WarnOnly) to rp.Length.
//  2. Filter every relevant character set by policy.ExcludedCharacters.
//  3. Apply rp.Constraints (filtered) or basic-validity coverage (filtered).
//  4. Ensure each policy.RequiredClasses is represented.
//  5. Fill remaining length from the filtered remainder pool.
//  6. Shuffle.
func GenerateRandomPasswordWithPolicy(
	rp *v2.LocalCredentialOptions_RandomPassword,
	policy *RandomPasswordPolicy,
) (string, error) {
	if rp == nil {
		return "", errors.New("random password options are required")
	}

	length, err := applyLengthPolicy(rp.GetLength(), policy)
	if err != nil {
		return "", err
	}
	if length < 8 {
		return "", ErrInvalidPasswordLength
	}

	excluded := buildExcludedSet(policy)

	upper := filterRunes(UpperCaseLetters, excluded)
	lower := filterRunes(LowerCaseLetters, excluded)
	dig := filterRunes(Digits, excluded)
	sym := filterRunes(Symbols, excluded)
	pool := upper + lower + dig + sym
	if pool == "" {
		return "", fmt.Errorf("%w: ExcludedCharacters removes every printable character", ErrPolicyConflict)
	}

	var password strings.Builder

	constraints := rp.GetConstraints()
	switch {
	case len(constraints) > 0:
		for _, c := range constraints {
			set := filterRunes(c.GetCharSet(), excluded)
			if set == "" {
				return "", fmt.Errorf("%w: a PasswordConstraint's CharSet is empty after applying ExcludedCharacters", ErrPolicyConflict)
			}
			for i := uint32(0); i < c.GetMinCount(); i++ {
				if err := addCharacterToPassword(&password, set); err != nil {
					return "", err
				}
			}
		}
	case policy != nil && len(policy.RequiredClasses) > 0:
		// RequiredClasses replaces basic-validity coverage when explicitly set.
		for _, class := range policy.RequiredClasses {
			if class == PasswordCharClassUnspecified {
				return "", fmt.Errorf("%w: RequiredClasses contains PasswordCharClassUnspecified; use one of Upper/Lower/Digit/Symbol or omit the entry", ErrPolicyConflict)
			}
			set := classRunes(class, excluded)
			if set == "" {
				return "", fmt.Errorf("%w: RequiredClass %v has no available characters after applying ExcludedCharacters", ErrPolicyConflict, class)
			}
			if err := addCharacterToPassword(&password, set); err != nil {
				return "", err
			}
		}
	default:
		// Basic-validity: one character from each non-empty class.
		for _, set := range []string{upper, lower, dig, sym} {
			if set == "" {
				continue
			}
			if err := addCharacterToPassword(&password, set); err != nil {
				return "", err
			}
		}
	}

	// When constraints are present, RequiredClasses are additive — ensure each
	// required class is represented if it isn't already.
	if len(constraints) > 0 && policy != nil && len(policy.RequiredClasses) > 0 {
		current := password.String()
		for _, class := range policy.RequiredClasses {
			if class == PasswordCharClassUnspecified {
				return "", fmt.Errorf("%w: RequiredClasses contains PasswordCharClassUnspecified; use one of Upper/Lower/Digit/Symbol or omit the entry", ErrPolicyConflict)
			}
			set := classRunes(class, excluded)
			if set == "" {
				return "", fmt.Errorf("%w: RequiredClass %v has no available characters after applying ExcludedCharacters", ErrPolicyConflict, class)
			}
			if containsAny(current, set) {
				continue
			}
			if err := addCharacterToPassword(&password, set); err != nil {
				return "", err
			}
		}
	}

	remaining := length - int64(len(password.String()))
	if remaining < 0 {
		return "", fmt.Errorf("password length %d is less than the sum of constraint minimums (%d)", length, int64(password.Len()))
	}
	for i := int64(0); i < remaining; i++ {
		if err := addCharacterToPassword(&password, pool); err != nil {
			return "", err
		}
	}

	return getShuffledPassword(password)
}

func applyLengthPolicy(length int64, policy *RandomPasswordPolicy) (int64, error) {
	if policy == nil {
		return length, nil
	}
	if policy.MaxLength > 0 && length > policy.MaxLength {
		switch policy.Enforcement {
		case PolicyEnforcementClamp, PolicyEnforcementWarnOnly:
			if policy.Enforcement == PolicyEnforcementClamp {
				length = policy.MaxLength
			}
		default:
			return 0, fmt.Errorf("%w: length %d exceeds MaxLength %d", ErrPolicyViolation, length, policy.MaxLength)
		}
	}
	if policy.MinLength > 0 && length < policy.MinLength {
		switch policy.Enforcement {
		case PolicyEnforcementClamp, PolicyEnforcementWarnOnly:
			if policy.Enforcement == PolicyEnforcementClamp {
				length = policy.MinLength
			}
		default:
			return 0, fmt.Errorf("%w: length %d is below MinLength %d", ErrPolicyViolation, length, policy.MinLength)
		}
	}
	return length, nil
}

func buildExcludedSet(policy *RandomPasswordPolicy) map[rune]struct{} {
	if policy == nil || policy.ExcludedCharacters == "" {
		return nil
	}
	set := make(map[rune]struct{}, len(policy.ExcludedCharacters))
	for _, r := range policy.ExcludedCharacters {
		set[r] = struct{}{}
	}
	return set
}

func filterRunes(s string, excluded map[rune]struct{}) string {
	if len(excluded) == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if _, banned := excluded[r]; banned {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func classRunes(class PasswordCharClass, excluded map[rune]struct{}) string {
	switch class {
	case PasswordCharClassUpper:
		return filterRunes(UpperCaseLetters, excluded)
	case PasswordCharClassLower:
		return filterRunes(LowerCaseLetters, excluded)
	case PasswordCharClassDigit:
		return filterRunes(Digits, excluded)
	case PasswordCharClassSymbol:
		return filterRunes(Symbols, excluded)
	case PasswordCharClassUnspecified:
		return ""
	}
	return ""
}

func containsAny(s, chars string) bool {
	if s == "" || chars == "" {
		return false
	}
	for _, r := range chars {
		if strings.ContainsRune(s, r) {
			return true
		}
	}
	return false
}
