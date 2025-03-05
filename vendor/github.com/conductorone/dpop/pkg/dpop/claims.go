// Package dpop provides DPoP proof validation and creation
package dpop

import (
	"encoding/json"
	"fmt"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Claims represents the claims in a DPoP proof
type Claims struct {
	*jwt.Claims
	HTTPMethod string                 `json:"htm"`
	HTTPUri    string                 `json:"htu"`
	Nonce      string                 `json:"nonce,omitempty"`
	TokenHash  string                 `json:"ath,omitempty"`
	Additional map[string]interface{} `json:"-"`

	// publicKey is the public key used to verify the proof
	// This field is populated during validation and is not part of the JSON
	publicKey *jose.JSONWebKey `json:"-"`
}

// PublicKey returns the public key used to verify the proof
func (c *Claims) PublicKey() *jose.JSONWebKey {
	return c.publicKey
}

// knownJWTClaimsFields is used to detect if new fields are added to jwt.Claims
// that we need to handle in MarshalJSON/UnmarshalJSON
var knownJWTClaimsFields = []string{
	"iss", // Issuer
	"sub", // Subject
	"aud", // Audience
	"exp", // Expiry
	"nbf", // NotBefore
	"iat", // IssuedAt
	"jti", // ID
}

// MarshalJSON implements json.Marshaler for Claims
func (c *Claims) MarshalJSON() ([]byte, error) {
	// Create a map to hold all claims
	allClaims := make(map[string]interface{})

	// Add the jwt.Claims fields directly
	if c.Claims != nil {
		if c.Claims.Issuer != "" {
			allClaims["iss"] = c.Claims.Issuer
		}
		if c.Claims.Subject != "" {
			allClaims["sub"] = c.Claims.Subject
		}
		if len(c.Claims.Audience) > 0 {
			aud, err := c.Claims.Audience.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal audience: %w", err)
			}
			allClaims["aud"] = json.RawMessage(aud)
		}
		if c.Claims.Expiry != nil {
			exp, err := c.Claims.Expiry.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal expiry: %w", err)
			}
			allClaims["exp"] = json.RawMessage(exp)
		}
		if c.Claims.NotBefore != nil {
			nbf, err := c.Claims.NotBefore.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal not before: %w", err)
			}
			allClaims["nbf"] = json.RawMessage(nbf)
		}
		if c.Claims.IssuedAt != nil {
			iat, err := c.Claims.IssuedAt.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal issued at: %w", err)
			}
			allClaims["iat"] = json.RawMessage(iat)
		}
		if c.Claims.ID != "" {
			allClaims["jti"] = c.Claims.ID
		}
	}

	// Add the explicit fields
	allClaims["htm"] = c.HTTPMethod
	allClaims["htu"] = c.HTTPUri
	if c.TokenHash != "" {
		allClaims["ath"] = c.TokenHash
	}
	if c.Nonce != "" {
		allClaims["nonce"] = c.Nonce
	}

	// Add any additional claims
	for k, v := range c.Additional {
		// Don't override standard claims
		if _, exists := allClaims[k]; !exists {
			allClaims[k] = v
		}
	}

	return json.Marshal(allClaims)
}

// UnmarshalJSON implements json.Unmarshaler for Claims
func (c *Claims) UnmarshalJSON(data []byte) error {
	// Create a map to hold all claims
	var allClaims map[string]interface{}
	if err := json.Unmarshal(data, &allClaims); err != nil {
		return err
	}

	// Create a temporary struct to hold the standard fields
	type StandardClaims struct {
		HTTPMethod string `json:"htm"`
		HTTPUri    string `json:"htu"`
		TokenHash  string `json:"ath,omitempty"`
		Nonce      string `json:"nonce,omitempty"`
	}

	// Unmarshal the standard fields
	var std StandardClaims
	if err := json.Unmarshal(data, &std); err != nil {
		return err
	}

	// Set the standard fields
	c.HTTPMethod = std.HTTPMethod
	c.HTTPUri = std.HTTPUri
	c.TokenHash = std.TokenHash
	c.Nonce = std.Nonce

	// Create a new jwt.Claims and unmarshal into it
	c.Claims = new(jwt.Claims)
	if err := json.Unmarshal(data, c.Claims); err != nil {
		return err
	}

	// Initialize the Additional map
	c.Additional = make(map[string]interface{})

	// Add any non-standard claims to Additional
	standardFields := map[string]bool{
		"htm": true, "htu": true, "ath": true, "nonce": true,
		"iss": true, "sub": true, "aud": true, "exp": true,
		"nbf": true, "iat": true, "jti": true,
	}

	for k, v := range allClaims {
		if !standardFields[k] {
			c.Additional[k] = v
		}
	}

	return nil
}
