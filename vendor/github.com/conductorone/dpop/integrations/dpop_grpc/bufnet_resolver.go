package dpop_grpc

import (
	"google.golang.org/grpc/resolver"
)

const bufnetScheme = "bufnet"

// bufnetResolverBuilder is a resolver builder for bufconn tests
type bufnetResolverBuilder struct{}

func (b *bufnetResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &bufnetResolver{
		target: target,
		cc:     cc,
	}
	r.start()
	return r, nil
}

func (b *bufnetResolverBuilder) Scheme() string {
	return bufnetScheme
}

// bufnetResolver is a resolver for bufconn tests
type bufnetResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
}

func (r *bufnetResolver) start() {
	r.cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: r.target.Endpoint()}},
	})
}

func (r *bufnetResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *bufnetResolver) Close() {}

// registerBufnetResolver registers the bufnet resolver builder
func registerBufnetResolver() {
	resolver.Register(&bufnetResolverBuilder{})
}
