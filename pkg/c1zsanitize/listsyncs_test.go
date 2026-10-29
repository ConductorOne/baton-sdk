package c1zsanitize

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type syncPagesReader struct {
	connectorstore.Reader
	pages [][]*reader_v2.SyncRun
}

func (r syncPagesReader) ListSyncs(
	_ context.Context,
	req *reader_v2.SyncsReaderServiceListSyncsRequest,
) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	page := 0
	if tok := req.GetPageToken(); tok != "" {
		n, err := strconv.Atoi(tok)
		if err != nil {
			return nil, err
		}
		page = n
	}
	if page < 0 || page >= len(r.pages) {
		return reader_v2.SyncsReaderServiceListSyncsResponse_builder{}.Build(), nil
	}
	next := ""
	if page+1 < len(r.pages) {
		next = strconv.Itoa(page + 1)
	}
	return reader_v2.SyncsReaderServiceListSyncsResponse_builder{
		Syncs:         r.pages[page],
		NextPageToken: next,
	}.Build(), nil
}

func TestListAllSyncs(t *testing.T) {
	mk := func(id, parent string) *reader_v2.SyncRun {
		return reader_v2.SyncRun_builder{Id: id, ParentSyncId: parent}.Build()
	}
	ids := func(runs []*reader_v2.SyncRun) []string {
		out := make([]string, len(runs))
		for i, run := range runs {
			out[i] = run.GetId()
		}
		return out
	}

	t.Run("duplicate id", func(t *testing.T) {
		_, err := listAllSyncs(context.Background(), syncPagesReader{
			pages: [][]*reader_v2.SyncRun{
				{mk("dup", "")},
				{mk("dup", "")},
			},
		})
		require.ErrorContains(t, err, "duplicate sync id")
	})

	t.Run("distinct ids across two pages", func(t *testing.T) {
		got, err := listAllSyncs(context.Background(), syncPagesReader{
			pages: [][]*reader_v2.SyncRun{
				{mk("c", "p")},
				{mk("p", "")},
			},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"p", "c"}, ids(got))
	})
}
