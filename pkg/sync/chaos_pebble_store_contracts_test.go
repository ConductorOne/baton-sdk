package sync

import (
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var _ c1zstore.Store = (*chaosPebbleStore)(nil)
var _ c1zstore.PageLedgerStore = (*chaosPebbleStore)(nil)
var _ connectorstore.LatestFinishedSyncIDFetcher = (*chaosPebbleStore)(nil)
var _ connectorstore.DBSizeProvider = (*chaosPebbleStore)(nil)
var _ connectorstore.Reader = (*chaosPebbleStore)(nil)
var _ connectorstore.StreamingReader = (*chaosPebbleStore)(nil)
var _ connectorstore.V3GrantReaderProvider = (*chaosPebbleStore)(nil)
var _ dotc1z.IngestInvariantStore = (*chaosPebbleStore)(nil)
var _ resourceRecordDeleter = (*chaosPebbleStore)(nil)
var _ grantByRefsDeleter = (*chaosPebbleStore)(nil)
var _ grantsByRefsBatchDeleter = (*chaosPebbleStore)(nil)
var _ grantPrincipalKeyLister = (*chaosPebbleStore)(nil)
var _ principalSortedGrantLister = (*chaosPebbleStore)(nil)
