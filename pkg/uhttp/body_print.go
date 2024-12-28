package uhttp

// Implements a debugging facility for request responses. This changes
// the behavior of `BaseHttpClient` with an unexported flag.
//
// If you always wanted to see the actual body of your response, now
// you can 👁️👄👁️undress it🫦 to uncover all its... data!

import (
	"io"
	"log"
)

type printReader struct {
	reader io.Reader
}

func (pr *printReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		log.Print(string(p[:n]))
	}

	return n, err
}

func wrapBodyToUndress(body io.Reader) io.Reader {
	return &printReader{reader: body}
}

type undressOption struct {
	undress bool
}

func (o undressOption) Apply(c *BaseHttpClient) {
	c.debugPrintBody = o.undress
}

func WithUndressBody(undress bool) WrapperOption {
	return undressOption{undress: undress}
}
