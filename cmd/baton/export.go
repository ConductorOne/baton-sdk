package main

import (
	"strings"
	"unicode"

	"github.com/spf13/cobra"
)

// sanitizeCell neutralizes spreadsheet formula injection in exported CSV/XLSX
// files. Spreadsheet applications (Excel, Google Sheets, LibreOffice, etc.)
// interpret a cell whose value begins with =, +, -, or @ as a formula. Because
// export values originate from upstream identity systems and are attacker
// controllable (e.g. a user's display name), any such value is prefixed with a
// single quote so the spreadsheet renders it as literal text. Embedded control
// characters — including tabs, carriage returns, and newlines that could be
// used to smuggle content or lead a formula — are stripped first.
func sanitizeCell(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)

	if s != "" {
		switch s[0] {
		case '=', '+', '-', '@':
			return "'" + s
		}
	}

	return s
}

type header int

func (h header) String() string {
	switch h {
	case headerType:
		return "Type"
	case headerLastName:
		return "Last Name"
	case headerFirstName:
		return "First Name"
	case headerUserID:
		return "User ID"
	case headerUserStatus:
		return "User Status"
	case headerEmailAddress:
		return "Email Address"
	case headerEntitlementDisplay:
		return "Entitlement Display Name"
	case headerEntitlement:
		return "Entitlement"
	case headerResourceType:
		return "Resource Type"
	case headerResourceName:
		return "Resource Name"
	case headerEntitlementDescription:
		return "Entitlement Description"
	case headerEntitlementSlug:
		return "Entitlement Slug"

	default:
		return "unknown"
	}
}

const (
	//nolint:deadcode,varcheck // Used for a starting point of iteration
	headerUnknown header = iota
	headerType
	headerLastName
	headerFirstName
	headerUserID
	headerUserStatus
	headerEmailAddress
	headerEntitlementDisplay
	headerEntitlement
	headerResourceType
	headerResourceName
	headerEntitlementDescription
	headerEntitlementSlug
	headerTerminator
)

func headers() []string {
	var ret []string
	for i := header(1); i < headerTerminator; i++ {
		ret = append(ret, i.String())
	}

	return ret
}

func export() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export data from the C1Z for upload",
	}

	cmd.AddCommand(exportCSV())
	cmd.AddCommand(exportXLSX())
	cmd.AddCommand(exportC1Z())

	return cmd
}
