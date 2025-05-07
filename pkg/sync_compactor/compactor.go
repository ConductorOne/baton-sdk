package sync_compactor

import (
	"context"
	"errors"
	"os"
	"path"
	"reflect"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Compactor struct {
	entries []*CompactableSync
	destDir string
}

type CompactableSync struct {
	filePath string
	syncID   string
}

func NewCompactor(ctx context.Context, destDir string, compactableSyncs ...*CompactableSync) (*Compactor, error) {
	if len(compactableSyncs) == 0 || len(compactableSyncs) == 1 {
		return nil, errors.New("must provide two or more files to compact")
	}

	return &Compactor{entries: compactableSyncs, destDir: destDir}, nil
}

func (c *Compactor) Compact(ctx context.Context) (*CompactableSync, error) {
	if len(c.entries) < 2 {
		return nil, nil
	}

	tempDir := os.TempDir()

	base := c.entries[0]
	var latest *CompactableSync
	for i := 1; i < len(c.entries); i++ {
		applied := c.entries[i]

		compactable, err := c.doOneCompaction(ctx, tempDir, base, applied)
		if err != nil {
			return nil, err
		}
		latest = compactable
		base = compactable
	}

	return latest, nil
}

func getLatestObjects(ctx context.Context, info *CompactableSync) (*reader_v2.SyncRun, *dotc1z.C1File, c1zmanager.Manager, func(), error) {
	baseC1Z, err := c1zmanager.New(ctx, info.filePath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	baseFile, err := baseC1Z.LoadC1Z(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	latestAppliedSync, err := baseFile.GetSync(ctx, &reader_v2.SyncsReaderServiceGetSyncRequest{
		SyncId:      info.syncID,
		Annotations: nil,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return latestAppliedSync.Sync, baseFile, baseC1Z, func() {
		_ = baseFile.Close()
		_ = baseC1Z.Close(ctx)
	}, nil
}

func (c *Compactor) doOneCompaction(ctx context.Context, tempDir string, base *CompactableSync, applied *CompactableSync) (*CompactableSync, error) {
	filePath := path.Join(tempDir, `compacted-%s.c1z`, time.Now().Format(time.RFC3339))

	newFile, err := dotc1z.NewC1ZFile(ctx, filePath, dotc1z.WithTmpDir(tempDir), dotc1z.WithPragma("journal_mode", "WAL"))
	if err != nil {
		return nil, err
	}

	_, baseFile, _, cleanupBase, err := getLatestObjects(ctx, base)
	if err != nil {
		return nil, err
	}
	defer cleanupBase()

	_, appliedFile, _, cleanupApplied, err := getLatestObjects(ctx, applied)
	defer cleanupApplied()
	if err != nil {
		return nil, err
	}

	// TODO: Use the runner when implementing the compaction logic
	_ = &naiveCompactor{
		base:    baseFile,
		applied: appliedFile,
		dest:    newFile,
	}

	//if err := runner.processResourceTypes(ctx); err != nil {
	//	return nil, err
	//}

	//
	return nil, errors.New("NOT IMPLEMENTED")
}

type naiveCompactor struct {
	base    *dotc1z.C1File
	applied *dotc1z.C1File
	dest    *dotc1z.C1File
}

type listRequest interface {
	proto.Message
	GetPageSize() uint32
	GetPageToken() string
	GetAnnotations() []*anypb.Any
}

type listResponse[T proto.Message] interface {
	GetNextPageToken() string
	GetAnnotations() []*anypb.Any
	GetList() []T
}

// createRequest creates a new request object of type REQ using reflection
func createRequest[REQ listRequest]() REQ {
	reqType := reflect.TypeOf((*REQ)(nil)).Elem()
	reqPtrValue := reflect.New(reqType)
	return reqPtrValue.Interface().(REQ)
}

// setFieldIfValid sets a field in a struct if it exists and can be set
func setFieldIfValid(obj interface{}, fieldName string, setValue func(reflect.Value)) {
	val := reflect.ValueOf(obj)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return
	}

	field := val.Elem().FieldByName(fieldName)
	if field.IsValid() && field.CanSet() {
		setValue(field)
	}
}

// setPageSize sets the PageSize field in a request to the specified value
func setPageSize(req listRequest, size uint64) {
	setFieldIfValid(req, "PageSize", func(field reflect.Value) {
		field.SetUint(size)
	})
}

// setPageToken sets the PageToken field in a request to the specified token
func setPageToken(req listRequest, token string) {
	setFieldIfValid(req, "PageToken", func(field reflect.Value) {
		field.SetString(token)
	})
}

func UnrollAll[T proto.Message, REQ listRequest, RESP listResponse[T]](ctx context.Context, list func(context.Context, REQ) (RESP, error)) ([]T, error) {
	var allResults []T

	// Create a new request using reflection
	req := createRequest[REQ]()

	// Set initial page size
	setPageSize(req, 100) // Set a reasonable default page size

	var nextPageToken string
	for {
		// Set the page token for the current request if needed
		if nextPageToken != "" {
			setPageToken(req, nextPageToken)
		}

		// Call the list function with the current request
		resp, err := list(ctx, req)
		if err != nil {
			return nil, err
		}

		// Collect the results
		allResults = append(allResults, resp.GetList()...)

		// Check if there are more pages
		nextPageToken = resp.GetNextPageToken()
		if nextPageToken == "" {
			break // No more pages
		}
	}

	return allResults, nil
}

func (c *naiveCompactor) processResourceTypes(ctx context.Context) error {
	baseResourceTypes, err := UnrollAll(ctx, c.base.ListResourceTypes)
	if err != nil {
		return err
	}

	for _, resourceTypes := range baseResourceTypes {
		if err := c.dest.PutResourceTypes(ctx, resourceTypes); err != nil {
			return err
		}
	}

	appliedResourceTypes, err := UnrollAll(ctx, c.applied.ListResourceTypes)
	if err != nil {
		return err
	}
	for _, resourceTypes := range appliedResourceTypes {
		if err := c.dest.PutResourceTypes(ctx, resourceTypes); err != nil {
			return err
		}
	}

	return nil
}
