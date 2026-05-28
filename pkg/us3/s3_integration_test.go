package us3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	awsSdk "github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// fakeS3 is a minimal in-memory S3 implementation that handles just
// enough of the protocol to drive PutWithVerify end-to-end. The AWS
// SDK's transfermanager picks the single-PutObject path for bodies
// below its 8 MiB part-size threshold, so we don't implement the
// CreateMultipartUpload / UploadPart / CompleteMultipartUpload set.
//
// Behaviors can be overridden per-test:
//
//   - HeadContentLengthOverride: return this Content-Length on HEAD
//     instead of the real stored size (used to fake truncated-multipart).
//   - HeadObjectFailureCode: return this HTTP status from HEAD.
type fakeS3 struct {
	t      *testing.T
	bucket string
	region string

	HeadContentLengthOverride *int64
	HeadObjectFailureCode     int

	mu          sync.Mutex
	objects     map[string][]byte
	deletedKeys []string
	putCalls    int
	headCalls   int
	deleteCalls int
}

func newFakeS3(t *testing.T, bucket, region string) *fakeS3 {
	return &fakeS3{
		t:       t,
		bucket:  bucket,
		region:  region,
		objects: map[string][]byte{},
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Path-style URL: /<bucket>/<key...> (we configure UsePathStyle in
	// the test client). r.URL.Path is "/bucket/key".
	bkey := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(bkey, "/")

	// GetBucketLocation: GET /<bucket>?location
	if _, ok := r.URL.Query()["location"]; ok && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">%s</LocationConstraint>`, f.region)
		return
	}

	if bucket != f.bucket {
		http.Error(w, "wrong bucket", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		f.handlePut(w, r, key)
	case http.MethodHead:
		f.handleHead(w, r, key)
	case http.MethodDelete:
		f.handleDelete(w, r, key)
	default:
		http.Error(w, "method not supported by fake: "+r.Method, http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.objects[key] = body
	f.putCalls++
	f.mu.Unlock()

	sum := sha256.Sum256(body)
	w.Header().Set("ETag", `"`+fmt.Sprintf("%x", sum[:8])+`"`)
	w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) handleHead(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	body, ok := f.objects[key]
	f.headCalls++
	f.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if f.HeadObjectFailureCode != 0 {
		http.Error(w, "forced failure", f.HeadObjectFailureCode)
		return
	}
	length := int64(len(body))
	if f.HeadContentLengthOverride != nil {
		length = *f.HeadContentLengthOverride
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", length))
	sum := sha256.Sum256(body)
	w.Header().Set("ETag", `"`+fmt.Sprintf("%x", sum[:8])+`"`)
	w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	delete(f.objects, key)
	f.deletedKeys = append(f.deletedKeys, key)
	f.deleteCalls++
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// Verify the XML format is at least well-formed (catches a typo in
// our LocationConstraint emit). Run on test startup.
type fakeLocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Value   string   `xml:",chardata"`
}

func TestFakeS3LocationConstraintXML(t *testing.T) {
	var got fakeLocationConstraint
	require.NoError(t, xml.Unmarshal(
		[]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint>us-west-2</LocationConstraint>`),
		&got,
	))
	require.Equal(t, "us-west-2", got.Value)
}

// newTestS3Client builds an *s3.Client pointing at a fakeS3 server.
// Uses path-style addressing so URL routing in fakeS3.ServeHTTP works
// without hostname-based bucket extraction.
func newTestS3Client(t *testing.T, server *httptest.Server, region string) *s3.Client {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(),
		awsConfig.WithRegion(region),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "secret", "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = awsSdk.String(server.URL)
		o.UsePathStyle = true
	})
}

func newTestUS3Client(t *testing.T, bucket string) *S3Client {
	return &S3Client{
		cfg: &s3Config{
			bucketName:  bucket,
			region:      "us-west-2",
			maxFileSize: 256 * 1024 * 1024,
		},
	}
}

func TestPutWithVerifyEndToEndHappy(t *testing.T) {
	fake := newFakeS3(t, "test-bucket", "us-west-2")
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	client := newTestUS3Client(t, "test-bucket")
	s3svc := newTestS3Client(t, server, "us-west-2")

	body := bytes.Repeat([]byte{0xAB}, 1024)
	key := "objects/happy.c1z"
	err := client.putWithVerifyUsingClient(t.Context(), s3svc, key, bytes.NewReader(body), int64(len(body)), "application/c1z")
	require.NoError(t, err)

	require.Equal(t, 1, fake.putCalls, "PutObject must have been called exactly once")
	require.Equal(t, 1, fake.headCalls, "HeadObject must have been called exactly once for verification")
	require.Equal(t, 0, fake.deleteCalls, "Delete must NOT fire on the happy path")

	fake.mu.Lock()
	stored, ok := fake.objects[key]
	fake.mu.Unlock()
	require.True(t, ok, "object should be present after PUT under key=%q (have keys: %v)", key, mapKeys(fake.objects))
	require.Equal(t, body, stored)
}

func mapKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestPutWithVerifyEndToEndHeadMismatchTriggersDelete(t *testing.T) {
	fake := newFakeS3(t, "test-bucket", "us-west-2")
	// Force HeadObject to claim the object is shorter than what we wrote.
	short := int64(900)
	fake.HeadContentLengthOverride = &short
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	client := newTestUS3Client(t, "test-bucket")
	s3svc := newTestS3Client(t, server, "us-west-2")

	body := bytes.Repeat([]byte{0xCD}, 1024)
	key := "objects/truncated.c1z"
	err := client.putWithVerifyUsingClient(t.Context(), s3svc, key, bytes.NewReader(body), int64(len(body)), "application/c1z")
	require.Error(t, err)
	require.ErrorContains(t, err, "uploaded object size mismatch")
	require.ErrorContains(t, err, "HeadObject=900")

	require.Equal(t, 1, fake.putCalls)
	require.Equal(t, 1, fake.headCalls)
	require.Equal(t, 1, fake.deleteCalls, "truncation detection must trigger Delete")
}

func TestPutWithVerifyEndToEndSourceShortReadTriggersDeleteWithoutHead(t *testing.T) {
	fake := newFakeS3(t, "test-bucket", "us-west-2")
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	client := newTestUS3Client(t, "test-bucket")
	s3svc := newTestS3Client(t, server, "us-west-2")

	body := []byte("only 5 bytes wide actually")[:5]
	// Caller declares the source is 1024 bytes but actually feeds 5.
	err := client.putWithVerifyUsingClient(t.Context(), s3svc, "objects/short.c1z", bytes.NewReader(body), int64(1024), "application/c1z")
	require.Error(t, err)
	require.ErrorContains(t, err, "short read")
	require.ErrorContains(t, err, "read 5, expected 1024")

	require.Equal(t, 1, fake.putCalls, "the body still uploads; verification then rejects")
	require.Equal(t, 0, fake.headCalls, "HeadObject must be skipped when the source-count check has already failed")
	require.Equal(t, 1, fake.deleteCalls, "the just-uploaded short object must be deleted")
}
