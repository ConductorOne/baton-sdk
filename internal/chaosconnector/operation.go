// Package chaosconnector provides the SDK's deterministic adversarial connector.
package chaosconnector

import (
	"strconv"
	"strings"
)

// Domain identifies the environment seam that produced an operation.
type Domain string

const (
	DomainConnector Domain = "connector"
	DomainSDKFS     Domain = "sdk-fs"
	DomainProcess   Domain = "process"
)

// Phase identifies when an effect is applied relative to the delegated call.
type Phase string

const (
	PhaseBeforeCall     Phase = "before-call"
	PhaseAfterDelegate  Phase = "after-delegate"
	PhaseBeforeResponse Phase = "before-response"
)

// Operation is the stable logical identity used to match fault rules.
//
// Attempt is one-based. Concurrent schedules must match these fields rather
// than relying on physical call order.
type Operation struct {
	Domain       Domain `json:"domain"`
	Service      string `json:"service,omitempty"`
	Method       string `json:"method,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`
	Subject      string `json:"subject,omitempty"`
	PageToken    string `json:"page_token,omitempty"`
	Attempt      int    `json:"attempt,omitempty"`
	Phase        Phase  `json:"phase,omitempty"`
}

// LogicalKey returns the attempt-independent identity of an operation.
func (o Operation) LogicalKey() string {
	parts := []string{
		string(o.Domain),
		o.Service,
		o.Method,
		o.ResourceType,
		o.Subject,
		o.PageToken,
	}
	return strings.Join(parts, "\x00")
}

// Key returns the complete stable identity of an operation.
func (o Operation) Key() string {
	return o.LogicalKey() + "\x00" + strconv.Itoa(o.Attempt) + "\x00" + string(o.Phase)
}

// Matcher selects operations. Empty string fields and Attempt zero are
// wildcards. Domain and Phase are also wildcards when empty.
type Matcher struct {
	Domain       Domain `json:"domain,omitempty"`
	Service      string `json:"service,omitempty"`
	Method       string `json:"method,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`
	Subject      string `json:"subject,omitempty"`
	PageToken    string `json:"page_token,omitempty"`
	Attempt      int    `json:"attempt,omitempty"`
	Phase        Phase  `json:"phase,omitempty"`
}

// Matches reports whether the matcher selects op.
func (m Matcher) Matches(op Operation) bool {
	return (m.Domain == "" || m.Domain == op.Domain) &&
		(m.Service == "" || m.Service == op.Service) &&
		(m.Method == "" || m.Method == op.Method) &&
		(m.ResourceType == "" || m.ResourceType == op.ResourceType) &&
		(m.Subject == "" || m.Subject == op.Subject) &&
		(m.PageToken == "" || m.PageToken == op.PageToken) &&
		(m.Attempt == 0 || m.Attempt == op.Attempt) &&
		(m.Phase == "" || m.Phase == op.Phase)
}
