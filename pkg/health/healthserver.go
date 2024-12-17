package health

import (
	"context"
	"fmt"
	"html"
	"net"
	"net/http"
	"time"
)

type HealthServer struct {
	server *http.Server
}

func NewHealthServer(ctx context.Context, addr string) (*HealthServer, error) {
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Duration(10) * time.Second,
	}

	// http.Handle("/foo", fooHandler)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
	})

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	err = server.Serve(l)
	if err != nil {
		return nil, err
	}

	return &HealthServer{
		server: server,
	}, nil
}
