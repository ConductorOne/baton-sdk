package uhttp

// Implements a debugging facility for request responses. This changes
// the behavior of `BaseHttpClient` with an unexported flag.

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

func wrapPrintBody(body io.Reader) io.Reader {
	return &printReader{reader: body}
}

type printBodyOption struct {
	debugPrintBody bool
}

func (o printBodyOption) Apply(c *BaseHttpClient) {
	c.debugPrintBody = o.debugPrintBody
}

func WithPrintBody(shouldPrint bool) WrapperOption {
	return printBodyOption{debugPrintBody: shouldPrint}
}
