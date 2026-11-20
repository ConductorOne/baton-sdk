package c1api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type ticketSchemaPage struct {
	list []*v2.TicketSchema
	next string
	err  error
}

// ticketSchemaClient serves scripted pages keyed by the request page token. It ignores the
// request page size, like a connector that picks its own page size.
type ticketSchemaClient struct {
	types.ConnectorClient
	pages    map[string]ticketSchemaPage
	requests []*v2.TicketsServiceListTicketSchemasRequest
}

func (c *ticketSchemaClient) ListTicketSchemas(
	_ context.Context,
	req *v2.TicketsServiceListTicketSchemasRequest,
	_ ...grpc.CallOption,
) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	c.requests = append(c.requests, req)
	page, ok := c.pages[req.GetPageToken()]
	if !ok {
		return nil, fmt.Errorf("unexpected page token %q", req.GetPageToken())
	}
	if page.err != nil {
		return nil, page.err
	}
	return v2.TicketsServiceListTicketSchemasResponse_builder{List: page.list, NextPageToken: page.next}.Build(), nil
}

type ticketSchemaHelpers struct {
	client   types.ConnectorClient
	finished bool
	response proto.Message
	annos    annotations.Annotations
	err      error
}

func (h *ticketSchemaHelpers) ConnectorClient() types.ConnectorClient { return h.client }
func (h *ticketSchemaHelpers) FinishTask(_ context.Context, response proto.Message, annos annotations.Annotations, err error) error {
	h.finished = true
	h.response = response
	h.annos = annos
	h.err = err
	return err
}

func makeTicketSchemas(n int, descBytes int) []*v2.TicketSchema {
	out := make([]*v2.TicketSchema, 0, n)
	for i := range n {
		out = append(out, v2.TicketSchema_builder{
			Id:          fmt.Sprintf("schema-%04d", i),
			DisplayName: strings.Repeat("x", descBytes),
		}.Build())
	}
	return out
}

// pagedClient splits schemas into connector pages of connectorPageSize, with tokens "p<start>".
func pagedClient(schemas []*v2.TicketSchema, connectorPageSize int) *ticketSchemaClient {
	pages := map[string]ticketSchemaPage{}
	if len(schemas) == 0 {
		pages[""] = ticketSchemaPage{}
		return &ticketSchemaClient{pages: pages}
	}
	for start := 0; start < len(schemas); start += connectorPageSize {
		end := min(start+connectorPageSize, len(schemas))
		token := ""
		if start > 0 {
			token = "p" + strconv.Itoa(start)
		}
		next := ""
		if end < len(schemas) {
			next = "p" + strconv.Itoa(end)
		}
		pages[token] = ticketSchemaPage{list: schemas[start:end], next: next}
	}
	return &ticketSchemaClient{pages: pages}
}

func ticketSchemasTask(t *v1.Task_ListTicketSchemasTask) *v1.Task {
	return v1.Task_builder{Id: "task-1", ListTicketSchemas: t}.Build()
}

func runTicketSchemasTask(client *ticketSchemaClient, t *v1.Task_ListTicketSchemasTask) (*ticketSchemaHelpers, error) {
	h := &ticketSchemaHelpers{client: client}
	err := newListSchemasTaskHandler(ticketSchemasTask(t), h).HandleTask(context.Background())
	return h, err
}

// requirePagedMarker asserts the ListTicketSchemasPaged marker is on the response and on the
// annotations passed to FinishTask, and that the response passes its own validation.
func requirePagedMarker(t *testing.T, h *ticketSchemaHelpers) *v2.TicketsServiceListTicketSchemasResponse {
	t.Helper()
	resp, ok := h.response.(*v2.TicketsServiceListTicketSchemasResponse)
	require.True(t, ok)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	require.True(t, respAnnos.Contains(&v2.ListTicketSchemasPaged{}))
	require.True(t, h.annos.Contains(&v2.ListTicketSchemasPaged{}))
	require.NoError(t, resp.Validate())
	require.LessOrEqual(t, len(resp.GetNextPageToken()), maxTicketSchemaPageTokenBytes)
	next := v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageToken: resp.GetNextPageToken()}.Build()
	require.NoError(t, next.Validate())
	return resp
}

func schemaIDs(list []*v2.TicketSchema) []string {
	ids := make([]string, 0, len(list))
	for _, s := range list {
		ids = append(ids, s.GetId())
	}
	return ids
}

// collectPaged runs paged tasks until next_page_token is empty. pageSizes is cycled per call.
func collectPaged(t *testing.T, client *ticketSchemaClient, pageSizes ...uint32) ([]string, []int) {
	t.Helper()
	var ids []string
	var batchLens []int
	token := ""
	for call := 0; ; call++ {
		require.Less(t, call, 10000, "paging did not terminate")
		size := pageSizes[call%len(pageSizes)]
		h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{
			Paginate:  true,
			PageToken: token,
			PageSize:  size,
		}.Build())
		require.NoError(t, err)
		require.NoError(t, h.err)
		resp := requirePagedMarker(t, h)
		require.LessOrEqual(t, len(resp.GetList()), ticketSchemaPageSize(size))
		ids = append(ids, schemaIDs(resp.GetList())...)
		batchLens = append(batchLens, len(resp.GetList()))
		token = resp.GetNextPageToken()
		if token == "" {
			return ids, batchLens
		}
	}
}

func TestListTicketSchemasLegacy(t *testing.T) {
	for _, n := range []int{0, 50, 100, 250} {
		for _, connectorPageSize := range []int{30, 100, 1000} {
			t.Run(fmt.Sprintf("n=%d/connector_page=%d", n, connectorPageSize), func(t *testing.T) {
				schemas := makeTicketSchemas(n, 0)
				client := pagedClient(schemas, connectorPageSize)
				// page_token and page_size are ignored without paginate.
				h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{PageToken: "ignored", PageSize: 7}.Build())
				for _, req := range client.requests {
					require.Zero(t, req.GetPageSize())
				}
				if n == 0 {
					require.EqualError(t, err, "connector returned no ticket schemas")
					require.Nil(t, h.response)
					return
				}
				require.NoError(t, err)
				want := v2.TicketsServiceListTicketSchemasResponse_builder{List: schemas[:min(n, maxTicketSchemas)]}.Build()
				wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
				require.NoError(t, err)
				gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(h.response)
				require.NoError(t, err)
				require.Equal(t, wantBytes, gotBytes)
				require.Empty(t, h.annos)
			})
		}
	}

	t.Run("connector error is returned without finishing the task", func(t *testing.T) {
		client := pagedClient(makeTicketSchemas(250, 0), 30)
		boom := errors.New("boom")
		client.pages["p60"] = ticketSchemaPage{err: boom}
		h, err := runTicketSchemasTask(client, &v1.Task_ListTicketSchemasTask{})
		require.ErrorIs(t, err, boom)
		require.False(t, h.finished)
	})
}

func TestListTicketSchemasPaged(t *testing.T) {
	all := makeTicketSchemas(250, 0)
	wantIDs := schemaIDs(all)

	t.Run("returns every schema once for each page size", func(t *testing.T) {
		for _, connectorPageSize := range []int{1, 30, 100, 250} {
			for _, size := range []uint32{0, 1, 7, 30, 100, 1000, 5000} {
				t.Run(fmt.Sprintf("connector_page=%d/page_size=%d", connectorPageSize, size), func(t *testing.T) {
					client := pagedClient(all, connectorPageSize)
					ids, _ := collectPaged(t, client, size)
					require.Equal(t, wantIDs, ids)
					for _, req := range client.requests {
						require.Zero(t, req.GetPageSize())
					}
				})
			}
		}
	})

	t.Run("cursor survives page size changes between calls", func(t *testing.T) {
		for _, connectorPageSize := range []int{30, 250} {
			client := pagedClient(all, connectorPageSize)
			ids, _ := collectPaged(t, client, 7, 50, 3, 100, 1, 64)
			require.Equal(t, wantIDs, ids)
		}
	})

	t.Run("splits a connector page larger than the batch", func(t *testing.T) {
		client := pagedClient(all, 250)
		ids, lens := collectPaged(t, client, 100)
		require.Equal(t, wantIDs, ids)
		require.Equal(t, []int{100, 100, 50}, lens)
	})

	t.Run("bounds a batch by serialized size", func(t *testing.T) {
		big := makeTicketSchemas(10, 700<<10)
		client := pagedClient(big, 10)
		ids, lens := collectPaged(t, client, 100)
		require.Equal(t, schemaIDs(big), ids)
		for _, n := range lens {
			require.LessOrEqual(t, n, 2)
		}
	})

	t.Run("returns a schema larger than the byte bound on its own", func(t *testing.T) {
		huge := makeTicketSchemas(3, maxTicketSchemaBatchBytes+1)
		client := pagedClient(huge, 3)
		ids, lens := collectPaged(t, client, 100)
		require.Equal(t, schemaIDs(huge), ids)
		require.Equal(t, []int{1, 1, 1}, lens)
	})

	t.Run("continues past empty connector pages with a next token", func(t *testing.T) {
		s := makeTicketSchemas(3, 0)
		client := &ticketSchemaClient{pages: map[string]ticketSchemaPage{
			"":   {next: "p1"},
			"p1": {list: s[:2], next: "p2"},
			"p2": {next: "p3"},
			"p3": {list: s[2:], next: ""},
		}}
		ids, lens := collectPaged(t, client, 100)
		require.Equal(t, schemaIDs(s), ids)
		require.Equal(t, []int{3}, lens)
	})

	t.Run("returns a resumable token after too many empty connector pages", func(t *testing.T) {
		pages := map[string]ticketSchemaPage{}
		for i := range maxTicketSchemaConnectorCalls + 5 {
			pages["e"+strconv.Itoa(i)] = ticketSchemaPage{next: "e" + strconv.Itoa(i+1)}
		}
		pages[""] = ticketSchemaPage{next: "e0"}
		s := makeTicketSchemas(1, 0)
		pages["e"+strconv.Itoa(maxTicketSchemaConnectorCalls+5)] = ticketSchemaPage{list: s}
		client := &ticketSchemaClient{pages: pages}
		ids, lens := collectPaged(t, client, 100)
		require.Equal(t, schemaIDs(s), ids)
		require.Equal(t, []int{0, 1}, lens)
	})

	t.Run("connector error mid-paging is retryable from the last token", func(t *testing.T) {
		client := pagedClient(all, 30)
		boom := errors.New("boom")
		good := client.pages["p90"]
		client.pages["p90"] = ticketSchemaPage{err: boom}

		h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageSize: 50}.Build())
		require.NoError(t, err)
		resp := h.response.(*v2.TicketsServiceListTicketSchemasResponse)
		require.Equal(t, wantIDs[:50], schemaIDs(resp.GetList()))
		token := resp.GetNextPageToken()

		h, err = runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageSize: 50, PageToken: token}.Build())
		require.ErrorIs(t, err, boom)
		require.False(t, h.finished)

		client.pages["p90"] = good
		h, err = runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageSize: 50, PageToken: token}.Build())
		require.NoError(t, err)
		resp = h.response.(*v2.TicketsServiceListTicketSchemasResponse)
		require.Equal(t, wantIDs[50:100], schemaIDs(resp.GetList()))
	})

	t.Run("no schemas on the first page is an error like legacy", func(t *testing.T) {
		h, err := runTicketSchemasTask(pagedClient(nil, 10), v1.Task_ListTicketSchemasTask_builder{Paginate: true}.Build())
		require.EqualError(t, err, "connector returned no ticket schemas")
		require.Nil(t, h.response)
	})

	t.Run("invalid token is non-retryable", func(t *testing.T) {
		for _, token := range []string{
			"!!!",
			"eyJ0IjoicDMwIiwibyI6NX0", // JSON cursor format from before ticketSchemaCursorVersion
			"AgVwMzA",                 // version 2
			"AYA",                     // truncated offset
			"Af____8P",                // offset over MaxInt32
			"AQD_",                    // connector token is not UTF-8
		} {
			h, err := runTicketSchemasTask(pagedClient(all, 30), v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageToken: token}.Build())
			require.ErrorIs(t, err, ErrTaskNonRetryable, token)
			require.True(t, h.finished)
		}
	})

	t.Run("offset past a shrunken connector page continues with the next page", func(t *testing.T) {
		token, err := encodeTicketSchemaCursor(ticketSchemaCursor{ConnectorPageToken: "p30", Offset: 31})
		require.NoError(t, err)
		h, err := runTicketSchemasTask(pagedClient(all, 30), v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageToken: token, PageSize: 1000}.Build())
		require.NoError(t, err)
		resp := requirePagedMarker(t, h)
		require.Equal(t, wantIDs[60:], schemaIDs(resp.GetList()))
		require.Empty(t, resp.GetNextPageToken())
	})

	t.Run("connector repeating its page token is non-retryable", func(t *testing.T) {
		s := makeTicketSchemas(2, 0)
		client := &ticketSchemaClient{pages: map[string]ticketSchemaPage{
			"":   {list: s[:1], next: "p1"},
			"p1": {list: s[1:], next: "p1"},
		}}
		h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true}.Build())
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.True(t, h.finished)
	})

	t.Run("connector token too long for the task page token fails", func(t *testing.T) {
		s := makeTicketSchemas(2, 0)
		longToken := strings.Repeat("a", maxTicketSchemaPageTokenBytes)
		client := &ticketSchemaClient{pages: map[string]ticketSchemaPage{
			"": {list: s, next: longToken},
		}}
		h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageSize: 2}.Build())
		require.ErrorContains(t, err, "byte limit")
		require.ErrorIs(t, err, ErrTaskNonRetryable)
		require.True(t, h.finished)
		require.Nil(t, h.response)
	})

	t.Run("connector token of HTML characters fits up to the cap", func(t *testing.T) {
		// Offset 0 adds a version byte and a one-byte offset before base64url.
		const maxConnectorToken = maxTicketSchemaPageTokenBytes*3/4 - 2
		for _, tc := range []struct {
			tokenLen int
			fits     bool
		}{
			{tokenLen: 1000, fits: true},
			{tokenLen: maxConnectorToken, fits: true},
			{tokenLen: maxConnectorToken + 1, fits: false},
		} {
			s := makeTicketSchemas(2, 0)
			token := strings.Repeat("<>&", tc.tokenLen/3+1)[:tc.tokenLen]
			client := &ticketSchemaClient{pages: map[string]ticketSchemaPage{
				"":    {list: s[:1], next: token},
				token: {list: s[1:]},
			}}
			if !tc.fits {
				_, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageSize: 1}.Build())
				require.ErrorIs(t, err, ErrTaskNonRetryable, tc.tokenLen)
				continue
			}
			ids, lens := collectPaged(t, client, 1)
			require.Equal(t, schemaIDs(s), ids, tc.tokenLen)
			require.Equal(t, []int{1, 1}, lens)
		}
	})

	t.Run("marks an empty final batch", func(t *testing.T) {
		s := makeTicketSchemas(2, 0)
		client := &ticketSchemaClient{pages: map[string]ticketSchemaPage{
			"":   {list: s, next: "p1"},
			"p1": {},
		}}
		ids, lens := collectPaged(t, client, 2)
		require.Equal(t, schemaIDs(s), ids)
		require.Equal(t, []int{2, 0}, lens)
	})

	t.Run("an empty batch with a token continues", func(t *testing.T) {
		pages := map[string]ticketSchemaPage{"": {next: "e0"}}
		for i := range maxTicketSchemaConnectorCalls {
			pages["e"+strconv.Itoa(i)] = ticketSchemaPage{next: "e" + strconv.Itoa(i+1)}
		}
		s := makeTicketSchemas(3, 0)
		pages["e"+strconv.Itoa(maxTicketSchemaConnectorCalls)] = ticketSchemaPage{list: s}
		client := &ticketSchemaClient{pages: pages}

		h, err := runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true}.Build())
		require.NoError(t, err)
		resp := requirePagedMarker(t, h)
		require.Empty(t, resp.GetList())
		require.NotEmpty(t, resp.GetNextPageToken())

		h, err = runTicketSchemasTask(client, v1.Task_ListTicketSchemasTask_builder{Paginate: true, PageToken: resp.GetNextPageToken()}.Build())
		require.NoError(t, err)
		resp = requirePagedMarker(t, h)
		require.Equal(t, schemaIDs(s), schemaIDs(resp.GetList()))
		require.Empty(t, resp.GetNextPageToken())
	})
}

func TestTicketSchemaCursorFormat(t *testing.T) {
	const golden = "AQVwMzA" // base64url of version 0x01, uvarint offset 5, "p30"
	want := ticketSchemaCursor{ConnectorPageToken: "p30", Offset: 5}

	got, err := decodeTicketSchemaCursor(golden)
	require.NoError(t, err)
	require.Equal(t, want, got)

	token, err := encodeTicketSchemaCursor(want)
	require.NoError(t, err)
	require.Equal(t, golden, token)

	first, err := encodeTicketSchemaCursor(ticketSchemaCursor{})
	require.NoError(t, err)
	got, err = decodeTicketSchemaCursor(first)
	require.NoError(t, err)
	require.Equal(t, ticketSchemaCursor{}, got)
}
