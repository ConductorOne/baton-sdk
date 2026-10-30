package chaosconnector

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// AnnotationCategory separates annotations by the kind of SDK obligation they
// can trigger.
type AnnotationCategory string

const (
	AnnotationControl       AnnotationCategory = "control"
	AnnotationCollection    AnnotationCategory = "collection-policy"
	AnnotationIngestion     AnnotationCategory = "ingestion"
	AnnotationProvisioning  AnnotationCategory = "provisioning-result"
	AnnotationTelemetry     AnnotationCategory = "telemetry"
	AnnotationDomain        AnnotationCategory = "domain-data"
	AnnotationCompatibility AnnotationCategory = "compatibility"
)

// AnnotationObligation is the expected high-level treatment. Individual tests
// refine this to an executable contract.
type AnnotationObligation string

const (
	ObligationConsume  AnnotationObligation = "consume"
	ObligationPreserve AnnotationObligation = "preserve"
	ObligationIgnore   AnnotationObligation = "ignore"
	ObligationReport   AnnotationObligation = "report"
	ObligationReject   AnnotationObligation = "reject"
)

// AnnotationPolicy classifies one known Any payload.
type AnnotationPolicy struct {
	FullName   protoreflect.FullName
	Category   AnnotationCategory
	Obligation AnnotationObligation
	Singleton  bool
	Scopes     []string
}

// KnownAnnotationPolicies returns the initial explicit semantic registry.
// Descriptor coverage of annotation-bearing fields is independent from this
// list: unknown Any types remain a deliberate ignore policy.
func KnownAnnotationPolicies() map[protoreflect.FullName]AnnotationPolicy {
	policies := []AnnotationPolicy{
		policy("c1.connector.v2.EnqueuePageTokens", AnnotationControl, ObligationConsume, true, "response"),
		policy("c1.connector.v2.SourceCacheCapability", AnnotationControl, ObligationConsume, true, "validate-response"),
		policy("c1.connector.v2.SourceCacheRecord", AnnotationControl, ObligationConsume, false, "record"),
		policy("c1.connector.v2.SourceCacheReplay", AnnotationControl, ObligationConsume, true, "response"),
		policy("c1.connector.v2.SourceCacheLookupOffer", AnnotationControl, ObligationConsume, true, "request"),
		policy("c1.connector.v2.SourceCacheLookupAsk", AnnotationControl, ObligationConsume, true, "response"),
		policy("c1.connector.v2.SourceCacheLookupAnswers", AnnotationControl, ObligationConsume, true, "request"),
		policy("c1.connector.v2.TypeScopedEntitlements", AnnotationCollection, ObligationConsume, true, "resource-type", "request"),
		policy("c1.connector.v2.TypeScopedGrants", AnnotationCollection, ObligationConsume, true, "resource-type", "request"),
		policy("c1.connector.v2.SkipEntitlementsAndGrants", AnnotationCollection, ObligationConsume, true, "resource-type"),
		policy("c1.connector.v2.SkipGrants", AnnotationCollection, ObligationConsume, true, "resource-type", "resource"),
		policy("c1.connector.v2.GrantExpandable", AnnotationIngestion, ObligationConsume, true, "grant"),
		policy("c1.connector.v2.InsertResourceGrants", AnnotationIngestion, ObligationConsume, true, "grant"),
		policy("c1.connector.v2.ExternalResourceMatch", AnnotationIngestion, ObligationConsume, true, "grant"),
		policy("c1.connector.v2.ExternalResourceMatchAll", AnnotationIngestion, ObligationConsume, true, "grant"),
		policy("c1.connector.v2.ExternalResourceMatchID", AnnotationIngestion, ObligationConsume, true, "grant"),
		policy("c1.connector.v2.BatonID", AnnotationIngestion, ObligationReject, true, "connector-resource"),
		policy("c1.connector.v2.Aliases", AnnotationIngestion, ObligationConsume, true, "resource"),
		policy("c1.connector.v2.EntitlementExclusionGroup", AnnotationIngestion, ObligationConsume, true, "entitlement"),
		policy("c1.connector.v2.GrantAlreadyExists", AnnotationProvisioning, ObligationConsume, true, "grant-response"),
		policy("c1.connector.v2.GrantAlreadyRevoked", AnnotationProvisioning, ObligationConsume, true, "revoke-response"),
		policy("c1.connector.v2.GrantReplaced", AnnotationProvisioning, ObligationConsume, true, "grant-response"),
		policy("c1.connector.v2.ResourceDoesNotExist", AnnotationProvisioning, ObligationConsume, true, "delete-response"),
		policy("c1.connector.v2.RateLimitWaitReport", AnnotationTelemetry, ObligationReport, true, "response"),
		policy("c1.connector.v2.SessionStoreUsage", AnnotationTelemetry, ObligationReport, true, "response"),
		policy("c1.connector.v2.ETag", AnnotationCompatibility, ObligationIgnore, true, "record"),
		policy("c1.connector.v2.ETagMetadata", AnnotationCompatibility, ObligationIgnore, true, "response"),
		policy("c1.connector.v2.ETagMatch", AnnotationCompatibility, ObligationIgnore, true, "request"),
		policy("c1.connector.v2.UserTrait", AnnotationDomain, ObligationPreserve, true, "resource"),
		policy("c1.connector.v2.GroupTrait", AnnotationDomain, ObligationPreserve, true, "resource"),
		policy("c1.connector.v2.RoleTrait", AnnotationDomain, ObligationPreserve, true, "resource"),
		policy("c1.connector.v2.AppTrait", AnnotationDomain, ObligationPreserve, true, "resource"),
		policy("c1.connector.v2.SecretTrait", AnnotationDomain, ObligationPreserve, true, "resource"),
	}
	out := make(map[protoreflect.FullName]AnnotationPolicy, len(policies))
	for _, item := range policies {
		out[item.FullName] = item
	}
	return out
}

func policy(
	name protoreflect.FullName,
	category AnnotationCategory,
	obligation AnnotationObligation,
	singleton bool,
	scopes ...string,
) AnnotationPolicy {
	return AnnotationPolicy{
		FullName:   name,
		Category:   category,
		Obligation: obligation,
		Singleton:  singleton,
		Scopes:     append([]string(nil), scopes...),
	}
}

// AnnotationField identifies one protobuf Any bag in the connector protocol.
type AnnotationField struct {
	Message protoreflect.FullName
	Field   protoreflect.FullName
}

// ConnectorAnnotationFields discovers every Any-bearing connector-v2 field.
// This is descriptor-driven so a new field changes the meta-test inventory.
func ConnectorAnnotationFields() []AnnotationField {
	var out []AnnotationField
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if file.Package() != "c1.connector.v2" ||
			!strings.Contains(file.Path(), "c1/connector/v2/") {
			return true
		}
		collectAnnotationFields(file.Messages(), &out)
		return true
	})
	slices.SortFunc(out, func(a, b AnnotationField) int {
		if byMessage := strings.Compare(string(a.Message), string(b.Message)); byMessage != 0 {
			return byMessage
		}
		return strings.Compare(string(a.Field), string(b.Field))
	})
	return out
}

func collectAnnotationFields(messages protoreflect.MessageDescriptors, out *[]AnnotationField) {
	for i := 0; i < messages.Len(); i++ {
		message := messages.Get(i)
		fields := message.Fields()
		for j := 0; j < fields.Len(); j++ {
			field := fields.Get(j)
			if field.IsList() && field.Message() != nil &&
				field.Message().FullName() == "google.protobuf.Any" {
				*out = append(*out, AnnotationField{
					Message: message.FullName(),
					Field:   field.FullName(),
				})
			}
		}
		collectAnnotationFields(message.Messages(), out)
	}
}
