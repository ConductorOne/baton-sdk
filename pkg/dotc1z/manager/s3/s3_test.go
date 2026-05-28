package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakePutClient captures the args PutWithVerify is called with so the
// test can assert that s3Manager forwards the local file size.
type fakePutClient struct {
	gotKey         string
	gotExpected    int64
	gotContentType string
	gotBody        []byte
	putErr         error
}

func (f *fakePutClient) PutWithVerify(_ context.Context, key string, r io.Reader, expectedSize int64, contentType string) error {
	f.gotKey = key
	f.gotExpected = expectedSize
	f.gotContentType = contentType
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.gotBody = body
	return f.putErr
}

func (f *fakePutClient) Get(_ context.Context, _ string) (io.Reader, error) {
	return nil, errors.New("not used")
}

func TestSaveC1ZForwardsLocalFileSize(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "test.c1z")
	want := []byte("c1z payload for fake upload")
	require.NoError(t, os.WriteFile(tmpFile, want, 0o600))

	fake := &fakePutClient{}
	m := &s3Manager{
		client:   fake,
		fileName: "objects/test.c1z",
		tmpFile:  tmpFile,
	}

	require.NoError(t, m.SaveC1Z(t.Context()))
	require.Equal(t, "objects/test.c1z", fake.gotKey)
	require.Equal(t, int64(len(want)), fake.gotExpected)
	require.Equal(t, "application/c1z", fake.gotContentType)
	require.Equal(t, want, fake.gotBody)
}

func TestSaveC1ZForwardsZeroSizeForEmptyFile(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "empty.c1z")
	require.NoError(t, os.WriteFile(tmpFile, nil, 0o600))

	fake := &fakePutClient{}
	m := &s3Manager{
		client:   fake,
		fileName: "objects/empty.c1z",
		tmpFile:  tmpFile,
	}

	require.NoError(t, m.SaveC1Z(t.Context()))
	require.Equal(t, int64(0), fake.gotExpected, "empty file must forward expectedSize=0, not the -1 sentinel")
	require.Empty(t, fake.gotBody)
}

func TestSaveC1ZPropagatesPutError(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "test.c1z")
	require.NoError(t, os.WriteFile(tmpFile, []byte("body"), 0o600))

	wantErr := errors.New("upload truncated")
	fake := &fakePutClient{putErr: wantErr}
	m := &s3Manager{
		client:   fake,
		fileName: "objects/test.c1z",
		tmpFile:  tmpFile,
	}

	err := m.SaveC1Z(t.Context())
	require.ErrorIs(t, err, wantErr)
}

func TestSaveC1ZRejectsMissingClient(t *testing.T) {
	m := &s3Manager{fileName: "objects/test.c1z", tmpFile: "/tmp/whatever"}
	err := m.SaveC1Z(t.Context())
	require.ErrorContains(t, err, "without a valid client")
}

func TestSaveC1ZRejectsEmptyFileName(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "test.c1z")
	require.NoError(t, os.WriteFile(tmpFile, []byte("body"), 0o600))
	m := &s3Manager{client: &fakePutClient{}, tmpFile: tmpFile}
	err := m.SaveC1Z(t.Context())
	require.ErrorContains(t, err, "without a valid file path")
}

// Sanity: the body the manager hands to PutWithVerify is the exact
// file bytes — no truncation, no envelope-only header.
func TestSaveC1ZUploadsTmpFileBytesUnchanged(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "test.c1z")
	want := bytes.Repeat([]byte{0xC1, 0xE3, 0x00, 0x42}, 64)
	require.NoError(t, os.WriteFile(tmpFile, want, 0o600))

	fake := &fakePutClient{}
	m := &s3Manager{client: fake, fileName: "objects/x.c1z", tmpFile: tmpFile}
	require.NoError(t, m.SaveC1Z(t.Context()))
	require.Equal(t, want, fake.gotBody)
}
