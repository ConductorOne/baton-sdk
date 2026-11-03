package dotc1z

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

type wrappedPageTestStore struct {
	*pebbleStore
	wrap func(c1zstore.PageWriter) c1zstore.PageWriter
}

func (s *wrappedPageTestStore) BeginPage() c1zstore.PageWriter {
	return s.wrap(s.pebbleStore.BeginPage())
}
func PebbleStoreForTesting(e *pebble.Engine, wrap func(c1zstore.PageWriter) c1zstore.PageWriter) c1zstore.Store {
	return &wrappedPageTestStore{pebbleStore: &pebbleStore{engine: e}, wrap: wrap}
}

func WrapPebbleStoreForTesting(store c1zstore.Store, wrap func(c1zstore.PageWriter) c1zstore.PageWriter) c1zstore.Store {
	return &wrappedPageTestStore{pebbleStore: store.(*pebbleStore), wrap: wrap}
}
