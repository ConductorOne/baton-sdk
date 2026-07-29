package connectorbuilder

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
)

var credentialIssueRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func credentialIssueOptionKind(options *v2.CredentialIssueOptions) v2.CapabilityDetailCredentialOption {
	if options == nil {
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
	switch options.WhichOptions() {
	case v2.CredentialIssueOptions_ApiKey_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	case v2.CredentialIssueOptions_Keypair_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR
	case v2.CredentialIssueOptions_Token_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN
	case v2.CredentialIssueOptions_ClientSecret_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET
	default:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
}

func validateCredentialIssueInput(input *CredentialIssueInput, details *v2.CredentialDetailsCredentialIssue, now time.Time) (*v2.CredentialIssueOptionDescriptor, error) {
	if input == nil || input.IdentityID == nil {
		return nil, fmt.Errorf("identity id is required")
	}
	if len(input.RequestID) == 0 || len(input.RequestID) > 128 || !credentialIssueRequestIDPattern.MatchString(input.RequestID) {
		return nil, fmt.Errorf("request id must be 1..128 characters containing only letters, digits, underscore, or hyphen")
	}
	kind := credentialIssueOptionKind(input.CredentialOptions)
	if kind == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
		return nil, fmt.Errorf("unsupported credential option")
	}
	var descriptor *v2.CredentialIssueOptionDescriptor
	for _, candidate := range details.GetOptions() {
		if candidate.GetOption() == kind {
			descriptor = candidate
			break
		}
	}
	if descriptor == nil {
		return nil, fmt.Errorf("credential option %s is not advertised by connector", kind)
	}
	if descriptor.GetResourceMode() == v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED {
		return nil, fmt.Errorf("credential resource mode must be advertised")
	}
	if keypair := input.CredentialOptions.GetKeypair(); keypair != nil {
		if err := validateKeyGenerationProfile(keypair.GetProfile()); err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(descriptor.GetKeyProfiles(), func(profile *v2.KeyGenerationProfile) bool {
			return proto.Equal(profile, keypair.GetProfile())
		}) {
			return nil, fmt.Errorf("requested key generation profile is not advertised by connector")
		}
	}
	if apiKey := input.CredentialOptions.GetApiKey(); apiKey != nil {
		if err := validateRequestedValues("scope", apiKey.GetScopes(), descriptor.GetScopes(), descriptor.GetCustomScopesAllowed()); err != nil {
			return nil, err
		}
	}
	if token := input.CredentialOptions.GetToken(); token != nil {
		if err := validateRequestedValues("scope", token.GetScopes(), descriptor.GetScopes(), descriptor.GetCustomScopesAllowed()); err != nil {
			return nil, err
		}
		if err := validateRequestedValues("audience", token.GetAudiences(), descriptor.GetAudiences(), descriptor.GetCustomAudiencesAllowed()); err != nil {
			return nil, err
		}
	}
	if input.ExpiresAt != nil {
		if err := input.ExpiresAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("expires_at must be valid: %w", err)
		}
		remaining := input.ExpiresAt.AsTime().Sub(now)
		if remaining <= 0 {
			return nil, fmt.Errorf("expires_at must be in the future")
		}
		capability := descriptor.GetExpiry()
		if capability == nil {
			return nil, fmt.Errorf("connector does not support caller-selected expiry")
		}
		if capability.GetMin() != nil && remaining < capability.GetMin().AsDuration() {
			return nil, fmt.Errorf("requested expiry is below connector minimum")
		}
		if capability.GetMax() != nil && remaining > capability.GetMax().AsDuration() {
			return nil, fmt.Errorf("requested expiry exceeds connector maximum")
		}
	}
	return descriptor, nil
}

func validateRequestedValues(kind string, requested []string, advertised []string, customAllowed bool) error {
	seen := make(map[string]struct{}, len(requested))
	for _, value := range requested {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", kind)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("duplicate %s %q", kind, value)
		}
		seen[value] = struct{}{}
		if !customAllowed && !slices.Contains(advertised, value) {
			return fmt.Errorf("%s %q is not advertised by connector", kind, value)
		}
	}
	return nil
}

func validateKeyGenerationProfile(profile *v2.KeyGenerationProfile) error {
	if profile == nil {
		return fmt.Errorf("key generation profile is required")
	}
	switch profile.GetKty() {
	case "RSA":
		if !profile.HasRsaModulusBits() || profile.HasCrv() {
			return fmt.Errorf("RSA profile requires rsa_modulus_bits and no curve")
		}
		bits := profile.GetRsaModulusBits()
		if bits < 2048 || bits > 16384 || bits%256 != 0 {
			return fmt.Errorf("RSA modulus bits must be 2048..16384 in 256-bit increments")
		}
	case "EC":
		if !profile.HasCrv() || profile.HasRsaModulusBits() || !slices.Contains([]string{"P-256", "P-384", "P-521"}, profile.GetCrv()) {
			return fmt.Errorf("EC profile requires a recognized P-256, P-384, or P-521 curve")
		}
	case "OKP":
		if !profile.HasCrv() || profile.HasRsaModulusBits() || !slices.Contains([]string{"Ed25519", "Ed448", "X25519", "X448"}, profile.GetCrv()) {
			return fmt.Errorf("OKP profile requires a recognized curve")
		}
	default:
		return fmt.Errorf("unsupported JWK key type %q", profile.GetKty())
	}
	return nil
}
