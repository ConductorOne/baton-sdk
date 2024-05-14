package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/types"
)

func ticketingCmd[T any, PtrT *T](
	ctx context.Context,
	name string,
	cfg PtrT,
	validateF func(ctx context.Context, cfg PtrT) error,
	getConnector func(ctx context.Context, cfg PtrT) (types.ConnectorServer, error),
	opts ...connectorrunner.Option,
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ticketing",
		Short: "Interact with ticketing systems",
	}

	return cmd
}
