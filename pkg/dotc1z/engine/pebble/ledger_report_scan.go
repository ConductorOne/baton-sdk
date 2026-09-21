package pebble

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

type ledgerReportIterator interface {
	First() bool
	Valid() bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
}

type ledgerReportProjected struct {
	collection                                     c1zstore.LedgerCollectionStats
	collectionPages                                uint64
	attempts                                       ledgerReportAttempts
	writes                                         ledgerReportWrites
	scope                                          c1zstore.LedgerActionIdentity
	written, pageMS, connectorMS, waitMS, children uint64
	terminal, paginationKnown                      bool
}

func ledgerReportFields(data []byte, visit func(protowire.Number, protowire.Type, []byte) error) error {
	for len(data) > 0 {
		number, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		n = protowire.ConsumeFieldValue(number, typ, data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		if err := visit(number, typ, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func ledgerReportProject(data []byte) (ledgerReportProjected, error) {
	var row ledgerReportProjected
	var counts [4]uint64
	var hasIdentity, hasNext, scrubbed, hashPresent, hashEmpty bool
	emptyHash := ledgerTokenHash("")
	err := ledgerReportFields(data, func(number protowire.Number, typ protowire.Type, value []byte) error {
		switch int32(number) {
		case 23:
			if typ != protowire.BytesType {
				return errors.New("invalid report collection stats")
			}
			value, _ = protowire.ConsumeBytes(value)
			row.collectionPages = 1
			return ledgerReportFields(value, func(field protowire.Number, kind protowire.Type, v []byte) error {
				if field < 1 || field > 14 {
					return nil
				}
				if kind != protowire.VarintType {
					return errors.New("invalid report collection counter")
				}
				n, _ := protowire.ConsumeVarint(v)
				switch int32(field) {
				case 1:
					row.collection.ListResponses = n
				case 2:
					row.collection.EmptyListResponses = n
				case 3:
					row.collection.EmptyListResponsesWithContinuation = n
				case 4:
					row.collection.ResourceTypesReceived = n
				case 5:
					row.collection.ResourcesReceived = n
				case 6:
					row.collection.EntitlementsReceived = n
				case 7:
					row.collection.GrantsReceived = n
				case 8:
					row.collection.ResourceTypesExcludedBySelection = n
				case 9:
					row.collection.EntitlementsExcludedByType = n
				case 10:
					row.collection.GrantsExcludedByType = n
				case 11:
					row.collection.DerivedResourcesExcludedByType = n
				case 12:
					row.collection.ResourceTypesExcludedInvalid = n
				case 13:
					row.collection.ResourcesExcludedInvalid = n
				case 14:
					row.collection.EntitlementsExcludedInvalid = n
				}
				return nil
			})
		case 1, 2, 3, 4:
			if typ != protowire.BytesType {
				return errors.New("invalid report row bytes field")
			}
			value, _ = protowire.ConsumeBytes(value)
			switch int32(number) {
			case 1:
				hasIdentity = true
				return ledgerReportFields(value, func(field protowire.Number, kind protowire.Type, v []byte) error {
					switch int32(field) {
					case 1, 2, 3, 4, 5:
						if kind != protowire.BytesType {
							return errors.New("invalid report scope string")
						}
						v, _ = protowire.ConsumeBytes(v)
						switch int32(field) {
						case 1:
							row.scope.Op = string(v)
						case 2:
							row.scope.ResourceTypeID = string(v)
						case 3:
							row.scope.ResourceID = string(v)
						case 4:
							row.scope.ParentResourceTypeID = string(v)
						case 5:
							row.scope.ParentResourceID = string(v)
						}
					case 8:
						if kind != protowire.VarintType {
							return errors.New("invalid report scope bool")
						}
						n, _ := protowire.ConsumeVarint(v)
						row.scope.TypeScoped = n != 0
					}
					return nil
				})
			case 2:
				hasNext = len(value) > 0
			case 3:
				hashPresent = len(value) > 0
				hashEmpty = bytes.Equal(value, emptyHash)
				if hashPresent && len(value) != ledgerTokenHashLen {
					return errors.New("invalid report continuation hash")
				}
			case 4:
				row.children++
			}
		case 7, 8, 9, 10, 13, 14, 15, 16, 18, 19, 20, 21, 22:
			if typ != protowire.VarintType {
				return errors.New("invalid report row numeric field")
			}
			n, _ := protowire.ConsumeVarint(value)
			switch int32(number) {
			case 7, 8, 9, 10:
				counts[number-7] = n
			case 13:
				scrubbed = n != 0
			case 14:
				row.pageMS = n
			case 15:
				row.connectorMS = n
			case 16:
				row.waitMS = n
			case 18:
				if n != 0 {
					row.attempts.Pages = 1
				} else {
					row.attempts.Pages = 0
				}
			case 19:
				row.attempts.Calls = n
			case 20:
				row.attempts.Errors = n
			case 21:
				row.attempts.RetryWaitMs = n
			case 22:
				row.attempts.RateLimitWaitMs = n
			}
		}
		return nil
	})
	if err != nil {
		return row, err
	}
	if !hasIdentity {
		return row, errors.New("report row has no identity")
	}
	for _, count := range counts {
		row.written += count
	}
	row.writes = ledgerReportWrites{ResourceTypes: counts[0], Resources: counts[1], Entitlements: counts[2], Grants: counts[3]}
	row.paginationKnown = !scrubbed || hashPresent
	row.terminal = !hasNext
	if scrubbed {
		row.terminal = hashPresent && hashEmpty
	}
	return row, nil
}

type ledgerReportGroup struct {
	stats   ledgerReportCollection
	latency ledgerReportHistogram
}

func (g *ledgerReportGroup) add(row ledgerReportProjected) {
	c := &g.stats
	c.Pages++
	c.Written += row.written
	c.Writes.add(row.writes)
	c.Attempts.add(row.attempts)
	addReportCollection(&c.Collection, row.collection)
	c.CollectionPages += row.collectionPages
	if row.written == 0 {
		c.ZeroWritePages++
	}
	c.PageMs += row.pageMS
	c.ConnectorMs += row.connectorMS
	c.ReportedWaitMs += row.waitMS
	c.MaxConnectorMs = max(c.MaxConnectorMs, row.connectorMS)
	c.Children += row.children
	switch {
	case !row.paginationKnown:
		c.PaginationUnknownPages++
	case row.terminal:
		c.TerminalPages++
	default:
		c.Continuations++
	}
	g.latency.add(row.connectorMS)
}

func (g *ledgerReportGroup) finish() ledgerReportCollection {
	c := g.stats
	c.ConnectorPageMedian = g.latency.quantile(50)
	c.ConnectorPageP95 = g.latency.quantile(95)
	c.WrittenPerPage = float64(c.Written) / float64(c.Pages)
	if c.Written > 0 {
		pages := float64(c.Pages) * 1000 / float64(c.Written)
		ms := float64(c.ConnectorMs) * 1000 / float64(c.Written)
		c.PagesPerThousandWrites = &pages
		c.ConnectorMsPerThousandWrites = &ms
	}
	return c
}

func ledgerReportTop(top []ledgerReportCollection, c ledgerReportCollection) []ledgerReportCollection {
	pos := len(top)
	for pos > 0 && ledgerReportRankBefore(c, top[pos-1]) {
		pos--
	}
	if pos >= 10 {
		return top
	}
	if len(top) < 10 {
		top = append(top, c)
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = c
	return top
}

func ledgerReportScan(ctx context.Context, iter ledgerReportIterator,
	emitCollection func(ledgerReportCollection), emitType func(ledgerReportCollection) error,
) (ledgerReportSummary, error) {
	var result ledgerReportSummary
	var collection, group ledgerReportGroup
	flushCollection := func() {
		if collection.stats.Pages == 0 {
			return
		}
		c := collection.finish()
		c.Collections = 1
		result.Collections++
		group.stats.Collections++
		if emitCollection != nil {
			emitCollection(c)
		}
		result.Top = ledgerReportTop(result.Top, c)
	}
	flushType := func() error {
		if group.stats.Pages == 0 {
			return nil
		}
		c := group.finish()
		result.OperationTypes++
		result.TopOperationTypes = ledgerReportTop(result.TopOperationTypes, c)
		if emitType != nil {
			return emitType(c)
		}
		return nil
	}
	counterPrefix := rawdb.LedgerCounterPrefix()
	rowPrefix := rawdb.LedgerKeyPrefix()
	optionsKey := encodeLedgerFactKey(c1zstore.LedgerFactReportOptions)
	optionPrefix := encodeLedgerFactKey(c1zstore.LedgerFactReportOptionsPrefix)
	skipGrants := encodeLedgerFactKey("should_skip_grants")
	skipBoth := encodeLedgerFactKey("should_skip_entitlements_and_grants")
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.LedgerKeysScanned++
		key := iter.Key()
		switch {
		case bytes.HasPrefix(key, counterPrefix):
			if err := result.addPhaseDurations(iter.Value()); err != nil {
				return result, err
			}
		case bytes.Equal(key, optionsKey):
			var options ledgerReportOptionSummary
			if err := json.Unmarshal([]byte(rawdb.DecodeLedgerFactValue(iter.Value())), &options); err != nil {
				return result, fmt.Errorf("decode ledger report options: %w", err)
			}
			result.Options = &options
		case bytes.HasPrefix(key, optionPrefix):
			result.OptionSnapshots++
		case bytes.Equal(key, skipGrants):
			disabled := true
			result.GrantsDisabled = &disabled
		case bytes.Equal(key, skipBoth):
			disabled := true
			result.GrantsDisabled = &disabled
			result.EntitlementsDisabled = &disabled
		case bytes.HasPrefix(key, rowPrefix):
			row, err := ledgerReportProject(iter.Value())
			if err != nil {
				return result, fmt.Errorf("project ledger stats row: %w", err)
			}
			if collection.stats.Pages > 0 && collection.stats.Scope != row.scope {
				flushCollection()
				collection = ledgerReportGroup{}
			}
			if group.stats.Pages > 0 && (group.stats.Scope.Op != row.scope.Op || group.stats.Scope.ResourceTypeID != row.scope.ResourceTypeID) {
				if err := flushType(); err != nil {
					return result, err
				}
				group = ledgerReportGroup{}
			}
			collection.stats.Scope = row.scope
			group.stats.Scope = c1zstore.LedgerActionIdentity{Op: row.scope.Op, ResourceTypeID: row.scope.ResourceTypeID}
			collection.add(row)
			group.add(row)
			result.Pages++
			result.Written += row.written
			result.Writes.add(row.writes)
			result.Attempts.add(row.attempts)
			addReportCollection(&result.Collection, row.collection)
			result.CollectionPages += row.collectionPages
			result.ConnectorMs += row.connectorMS
			result.ReportedWaitMs += row.waitMS
			result.Children += row.children
			if !row.paginationKnown {
				result.PaginationUnknownPages++
			} else if !row.terminal {
				result.Continuations++
			}
		}
	}
	if err := iter.Error(); err != nil {
		return result, err
	}
	flushCollection()
	if err := flushType(); err != nil {
		return result, err
	}
	return result, nil
}
