package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc"
)

// A connector whose resource types "cost-<n>" each page through a fixed
// number of records, so a test can drive many page commits deterministically.
type ledgerCostConnector struct {
	*mockConnector
	pages, records, streams int
	calls                   atomic.Int64
}

func (c *ledgerCostConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	stream, err := strconv.Atoi(strings.TrimPrefix(req.GetResourceTypeId(), "cost-"))
	if err != nil || stream < 0 || stream >= c.streams {
		return nil, fmt.Errorf("cost connector: invalid stream %q", req.GetResourceTypeId())
	}
	page := 0
	if req.GetPageToken() != "" {
		parsed, err := strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, err
		}
		page = parsed
	}
	globalPage := stream + page*c.streams
	if page < 0 || globalPage >= c.pages {
		return nil, fmt.Errorf("cost connector: page %d outside [0,%d)", page, c.pages)
	}
	records := make([]*v2.Resource, c.records)
	for i := range records {
		records[i] = v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: req.GetResourceTypeId(), Resource: fmt.Sprintf("%012d", globalPage*c.records+i)}.Build(),
			DisplayName: "fixed-resource-payload",
		}.Build()
	}
	next := ""
	if globalPage+c.streams < c.pages {
		next = strconv.Itoa(page + 1)
	}
	c.calls.Add(1)
	return v2.ResourcesServiceListResourcesResponse_builder{List: records, NextPageToken: next}.Build(), nil
}

func writeLedgerTestFile(path string, data []byte, mode os.FileMode) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return root.WriteFile(filepath.Base(path), data, mode)
}
