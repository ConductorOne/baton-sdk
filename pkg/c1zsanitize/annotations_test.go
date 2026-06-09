package c1zsanitize

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/anypb"
)

// capturedLog is one recorded zap entry (message + flattened fields).
type capturedLog struct {
	msg    string
	fields map[string]any
}

// recordingCore is a minimal zapcore.Core that captures entries in memory.
// zaptest/observer is not vendored, so this stands in for it.
type recordingCore struct {
	mu   *sync.Mutex
	logs *[]capturedLog
}

func (c recordingCore) Enabled(zapcore.Level) bool        { return true }
func (c recordingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c recordingCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(e, c)
}
func (c recordingCore) Sync() error { return nil }
func (c recordingCore) Write(e zapcore.Entry, fs []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fs {
		f.AddTo(enc)
	}
	c.mu.Lock()
	*c.logs = append(*c.logs, capturedLog{msg: e.Message, fields: enc.Fields})
	c.mu.Unlock()
	return nil
}

func newRecordingLogger() (*zap.Logger, *[]capturedLog) {
	logs := &[]capturedLog{}
	return zap.New(recordingCore{mu: &sync.Mutex{}, logs: logs}), logs
}

func unknownAnno(typeURL string) *anypb.Any {
	return &anypb.Any{TypeUrl: typeURL, Value: []byte{0x08, 0x01}}
}

func TestTransformAnnotationsCountersAndFirstOccurrence(t *testing.T) {
	const (
		urlA = "type.googleapis.com/c1.connector.v2.NotRealA"
		urlB = "type.googleapis.com/c1.connector.v2.NotRealB"
	)

	countFirstOccurrence := func(logs []capturedLog, snippet, typeURL string) int {
		n := 0
		for _, l := range logs {
			if l.msg == snippet && l.fields["type_url"] == typeURL {
				n++
			}
		}
		return n
	}

	for _, tc := range []struct {
		name    string
		dropOn  bool
		snippet string
		counts  func(*sanitizer) map[string]uint64
	}{
		{
			name:    "drop mode",
			dropOn:  true,
			snippet: "c1zsanitize: dropping unknown annotation type (first occurrence; counted thereafter)",
			counts:  func(s *sanitizer) map[string]uint64 { return s.droppedAnnotations },
		},
		{
			name:    "pass-through mode",
			dropOn:  false,
			snippet: "c1zsanitize: passing unknown annotation through unchanged (first occurrence; counted thereafter)",
			counts:  func(s *sanitizer) map[string]uint64 { return s.passedAnnotations },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSanitizer(bytes32("anno-counters"))
			s.dropUnknownAnnotations = tc.dropOn
			logger, logs := newRecordingLogger()
			s.log = logger

			// urlA twice, urlB once.
			s.transformAnnotations([]*anypb.Any{unknownAnno(urlA), unknownAnno(urlA), unknownAnno(urlB)}, newAssetRefSet())

			counts := tc.counts(s)
			require.Equal(t, uint64(2), counts[urlA], "counter for urlA")
			require.Equal(t, uint64(1), counts[urlB], "counter for urlB")

			require.Equal(t, 1, countFirstOccurrence(*logs, tc.snippet, urlA),
				"exactly one first-occurrence line for urlA")
			require.Equal(t, 1, countFirstOccurrence(*logs, tc.snippet, urlB),
				"exactly one first-occurrence line for urlB")
		})
	}
}
