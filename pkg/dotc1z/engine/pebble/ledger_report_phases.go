package pebble

import (
	"errors"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

func ledgerReportPhase(name string) bool {
	switch name {
	case "list-resource-types", "list-resources", "targeted-resource-sync", "list-static-entitlements",
		"list-entitlements", "list-grants", "list-external-resources", "fetch-assets":
		return true
	default:
		return false
	}
}

func ledgerReportPhaseDurations(data []byte) (map[string]uint64, error) {
	var phases map[string]uint64
	err := ledgerReportFields(data, func(number protowire.Number, kind protowire.Type, value []byte) error {
		if number != 4 {
			return nil
		}
		if kind != protowire.BytesType {
			return errors.New("invalid phase duration map")
		}
		entry, _ := protowire.ConsumeBytes(value)
		var name string
		var duration uint64
		err := ledgerReportFields(entry, func(field protowire.Number, typ protowire.Type, encoded []byte) error {
			switch int(field) {
			case 1:
				if typ != protowire.BytesType {
					return errors.New("invalid phase duration name")
				}
				key, _ := protowire.ConsumeBytes(encoded)
				name = string(key)
			case 2:
				if typ != protowire.VarintType {
					return errors.New("invalid phase duration value")
				}
				duration, _ = protowire.ConsumeVarint(encoded)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if ledgerReportPhase(name) {
			if duration > math.MaxInt64 {
				return errors.New("negative phase duration")
			}
			if phases == nil {
				phases = make(map[string]uint64)
			}
			phases[name] = duration
		}
		return nil
	})
	return phases, err
}

func (r *ledgerReportSummary) addPhaseDurations(data []byte) error {
	phases, err := ledgerReportPhaseDurations(data)
	if err != nil {
		return err
	}
	if len(phases) == 0 {
		return nil
	}
	if r.PhaseElapsedMs == nil {
		r.PhaseElapsedMs = make(map[string]uint64)
	}
	for name, duration := range phases {
		if duration > math.MaxInt64-r.PhaseElapsedMs[name] {
			return errors.New("phase duration sum overflow")
		}
		r.PhaseElapsedMs[name] += duration
	}
	return nil
}
