package pebble

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ledgerReportScopePreview[T any] struct {
	Values []T    `json:"values"`
	Total  uint64 `json:"total"`
}

func (p *ledgerReportScopePreview[T]) UnmarshalJSON(data []byte) error {
	*p = ledgerReportScopePreview[T]{}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('[') {
		return errors.New("report option scope must be an array")
	}
	for decoder.More() {
		var value T
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		p.Total++
		if len(p.Values) < 16 {
			p.Values = append(p.Values, value)
		}
	}
	_, err = decoder.Token()
	return err
}

type ledgerReportRequestedSummary struct {
	c1zstore.LedgerRequestedOptions
	ResourceTypes          ledgerReportScopePreview[string]                      `json:"resource_types"`
	Targets                ledgerReportScopePreview[c1zstore.LedgerReportTarget] `json:"targets"`
	ExternalResourceTraits ledgerReportScopePreview[string]                      `json:"external_resource_traits"`
}

type ledgerReportOptionSummary struct {
	c1zstore.LedgerReportOptions
	Requested ledgerReportRequestedSummary `json:"requested"`
}
