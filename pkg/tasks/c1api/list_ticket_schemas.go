package c1api

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

const maxTicketSchemas = 100

const (
	defaultTicketSchemaPageSize = maxTicketSchemas
	maxTicketSchemaPageSize     = 1000
	// maxTicketSchemaBatchBytes keeps a paged response well under the default 4 MiB gRPC message limit.
	maxTicketSchemaBatchBytes = 2 << 20
	// maxTicketSchemaConnectorCalls bounds the work in one task when the connector returns empty pages.
	maxTicketSchemaConnectorCalls = 100
	// maxTicketSchemaPageTokenBytes matches the max_bytes rule on TicketsServiceListTicketSchemasResponse.next_page_token
	// and ListTicketSchemasTask.page_token, so an emitted cursor always passes both.
	maxTicketSchemaPageTokenBytes = 4096
	ticketSchemaCursorVersion     = 1
)

type listTicketSchemasTaskHelpers interface {
	ConnectorClient() types.ConnectorClient
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
}

type listTicketSchemasTaskHandler struct {
	task    *v1.Task
	helpers listTicketSchemasTaskHelpers
}

func (c *listTicketSchemasTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "listTicketSchemasTaskHandler.HandleTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)

	t := c.task.GetListTicketSchemas()
	if t == nil {
		l.Error("list ticket schemas was nil", zap.Any("list_ticket_schemas_task", t))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(errors.New("malformed list ticket schemas task"), ErrTaskNonRetryable))
	}

	if t.GetPaginate() {
		err = c.handlePaged(ctx, t)
		return err
	}

	cc := c.helpers.ConnectorClient()
	var ticketSchemas []*v2.TicketSchema
	pageToken := ""
	for {
		schemas, err := cc.ListTicketSchemas(ctx, v2.TicketsServiceListTicketSchemasRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return err
		}

		ticketSchemas = append(ticketSchemas, schemas.GetList()...)

		// Only return first 100 elements
		if len(ticketSchemas) >= maxTicketSchemas {
			ignoreCount := len(ticketSchemas) - maxTicketSchemas
			ticketSchemas = ticketSchemas[:maxTicketSchemas]
			hasAdditionalPages := schemas.GetNextPageToken() != ""

			l.Info("list ticket schemas was greater than or equal to max of 100",
				zap.Int("ignoredCount", ignoreCount),
				zap.Bool("hasAdditionalPages", hasAdditionalPages))
			break
		}

		if schemas.GetNextPageToken() == "" {
			break
		}

		pageToken = schemas.GetNextPageToken()
	}

	if len(ticketSchemas) == 0 {
		err = fmt.Errorf("connector returned no ticket schemas")
	}

	if err != nil {
		l.Error("failed listing ticket schemas", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	resp := v2.TicketsServiceListTicketSchemasResponse_builder{
		List:          ticketSchemas,
		NextPageToken: "",
	}.Build()

	return c.helpers.FinishTask(ctx, resp, resp.GetAnnotations(), nil)
}

// ticketSchemaCursor is the decoded ListTicketSchemasTask.page_token in paged mode.
// Offset counts schemas already returned from the connector page fetched with ConnectorPageToken.
// The handler never sends a page size to the connector, so connector page boundaries do not
// depend on the caller's page_size and a cursor stays valid when page_size changes between calls.
// Resume assumes the connector returns the same page for the same page token.
type ticketSchemaCursor struct {
	ConnectorPageToken string
	Offset             int
}

// encodeTicketSchemaCursor returns base64url(version byte, uvarint offset, connector token bytes).
// Callers hold this format across SDK versions; change ticketSchemaCursorVersion when it changes.
func encodeTicketSchemaCursor(c ticketSchemaCursor) (string, error) {
	b := make([]byte, 0, 1+binary.MaxVarintLen64+len(c.ConnectorPageToken))
	b = append(b, ticketSchemaCursorVersion)
	b = binary.AppendUvarint(b, uint64(c.Offset)) //nolint:gosec // Offset is never negative.
	b = append(b, c.ConnectorPageToken...)
	token := base64.RawURLEncoding.EncodeToString(b)
	if len(token) > maxTicketSchemaPageTokenBytes {
		return "", errors.Join(
			fmt.Errorf("ticket schema page token is %d bytes, over the %d byte limit", len(token), maxTicketSchemaPageTokenBytes),
			ErrTaskNonRetryable,
		)
	}
	return token, nil
}

func decodeTicketSchemaCursor(token string) (ticketSchemaCursor, error) {
	var c ticketSchemaCursor
	if token == "" {
		return c, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return c, fmt.Errorf("invalid ticket schema page token: %w", err)
	}
	if len(b) == 0 || b[0] != ticketSchemaCursorVersion {
		return c, errors.New("invalid ticket schema page token: unknown version")
	}
	offset, n := binary.Uvarint(b[1:])
	if n <= 0 || offset > math.MaxInt32 {
		return c, errors.New("invalid ticket schema page token: bad offset")
	}
	pageToken := b[1+n:]
	if !utf8.Valid(pageToken) {
		return c, errors.New("invalid ticket schema page token: connector token is not UTF-8")
	}
	c.ConnectorPageToken = string(pageToken)
	c.Offset = int(offset)
	return c, nil
}

func ticketSchemaPageSize(requested uint32) int {
	switch {
	case requested == 0:
		return defaultTicketSchemaPageSize
	case requested > maxTicketSchemaPageSize:
		return maxTicketSchemaPageSize
	default:
		return int(requested)
	}
}

// handlePaged returns one batch of at most page_size schemas and maxTicketSchemaBatchBytes,
// always including at least one schema when one is available. A batch may end inside a
// connector page; the returned token then resumes at the next schema of that page, so a
// connector that returns everything in one page is re-fetched for each batch.
// Every response sets paged.
func (c *listTicketSchemasTaskHandler) handlePaged(ctx context.Context, t *v1.Task_ListTicketSchemasTask) error {
	l := ctxzap.Extract(ctx)

	cursor, err := decodeTicketSchemaCursor(t.GetPageToken())
	if err != nil {
		l.Error("failed decoding ticket schema page token", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
	}
	firstPage := t.GetPageToken() == ""
	pageSize := ticketSchemaPageSize(t.GetPageSize())

	cc := c.helpers.ConnectorClient()
	var batch []*v2.TicketSchema
	batchBytes := 0
	nextToken := ""
	for calls := 0; ; calls++ {
		if calls >= maxTicketSchemaConnectorCalls {
			nextToken, err = encodeTicketSchemaCursor(cursor)
			if err != nil {
				l.Error("failed encoding ticket schema page token", zap.Error(err))
				return c.helpers.FinishTask(ctx, nil, nil, err)
			}
			break
		}

		schemas, err := cc.ListTicketSchemas(ctx, v2.TicketsServiceListTicketSchemasRequest_builder{
			PageToken: cursor.ConnectorPageToken,
		}.Build())
		if err != nil {
			return err
		}

		list := schemas.GetList()
		if cursor.Offset > len(list) {
			// The connector page shrank since the cursor was issued; continue with the next page.
			l.Warn("ticket schema page token offset exceeds connector page length",
				zap.Int("offset", cursor.Offset), zap.Int("page_length", len(list)))
			cursor.Offset = len(list)
		}

		full := false
		for i := cursor.Offset; i < len(list); i++ {
			size := protowire.SizeTag(1) + protowire.SizeBytes(proto.Size(list[i]))
			if len(batch) > 0 && (len(batch) >= pageSize || batchBytes+size > maxTicketSchemaBatchBytes) {
				cursor.Offset = i
				full = true
				break
			}
			batch = append(batch, list[i])
			batchBytes += size
		}
		if full {
			nextToken, err = encodeTicketSchemaCursor(cursor)
			if err != nil {
				l.Error("failed encoding ticket schema page token", zap.Error(err))
				return c.helpers.FinishTask(ctx, nil, nil, err)
			}
			break
		}

		next := schemas.GetNextPageToken()
		if next == "" {
			break
		}
		if next == cursor.ConnectorPageToken {
			err = fmt.Errorf("connector returned the same ticket schema page token it was given")
			l.Error("failed listing ticket schemas", zap.Error(err))
			return c.helpers.FinishTask(ctx, nil, nil, errors.Join(err, ErrTaskNonRetryable))
		}
		cursor = ticketSchemaCursor{ConnectorPageToken: next}
		if len(batch) >= pageSize {
			nextToken, err = encodeTicketSchemaCursor(cursor)
			if err != nil {
				l.Error("failed encoding ticket schema page token", zap.Error(err))
				return c.helpers.FinishTask(ctx, nil, nil, err)
			}
			break
		}
	}

	if firstPage && nextToken == "" && len(batch) == 0 {
		err = fmt.Errorf("connector returned no ticket schemas")
		l.Error("failed listing ticket schemas", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	resp := v2.TicketsServiceListTicketSchemasResponse_builder{
		List:          batch,
		NextPageToken: nextToken,
		Paged:         true,
	}.Build()

	return c.helpers.FinishTask(ctx, resp, nil, nil)
}

func newListSchemasTaskHandler(task *v1.Task, helpers listTicketSchemasTaskHelpers) *listTicketSchemasTaskHandler {
	return &listTicketSchemasTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
