package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
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

	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "Get ticket schema for authorization context",
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
			if err != nil {
				return err
			}

			runCtx, err := initLogger(
				ctx,
				name,
				logging.WithLogFormat(v.GetString("log-format")),
				logging.WithLogLevel(v.GetString("log-level")),
			)
			if err != nil {
				return err
			}

			c, err := getConnector(runCtx, cfg)
			if err != nil {
				return err
			}

			var ticketSchemas []*v2.TicketSchema
			pageToken := ""
			for {
				schemas, err := c.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{
					PageToken: pageToken,
				})
				if err != nil {
					return err
				}

				ticketSchemas = append(ticketSchemas, schemas.GetList()...)

				if schemas.GetNextPageToken() == "" {
					break
				}
				pageToken = schemas.GetNextPageToken()
			}

			if len(ticketSchemas) == 0 {
				return fmt.Errorf("connector returned empty ticket schema")
			}

			protoMarshaller := protojson.MarshalOptions{
				Multiline: true,
				Indent:    "  ",
			}

			raw := make([]json.RawMessage, 0, len(ticketSchemas))
			for _, schema := range ticketSchemas {
				b, err := protoMarshaller.Marshal(schema)
				if err != nil {
					return err
				}
				raw = append(raw, b)
			}

			outBytes, err := json.Marshal(raw)
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(os.Stdout, string(outBytes))
			if err != nil {
				return err
			}

			return nil
		},
	}
	cmd.AddCommand(schemaCmd)

	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Get ticket details",
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
			if err != nil {
				return err
			}

			runCtx, err := initLogger(
				ctx,
				name,
				logging.WithLogFormat(v.GetString("log-format")),
				logging.WithLogLevel(v.GetString("log-level")),
			)
			if err != nil {
				return err
			}

			c, err := getConnector(runCtx, cfg)
			if err != nil {
				return err
			}

			ticketID := v.GetString("ticket-id")
			if ticketID == "" {
				return fmt.Errorf("ticket-id is required")
			}
			ticket, err := c.GetTicket(runCtx, &v2.TicketsServiceGetTicketRequest{
				Id: ticketID,
			})
			if err != nil {
				return err
			}

			if ticket.GetTicket() == nil {
				return fmt.Errorf("connector returned empt ticket schema")
			}

			protoMarshaller := protojson.MarshalOptions{
				Multiline: true,
				Indent:    "  ",
			}

			a := &anypb.Any{}
			err = anypb.MarshalFrom(a, ticket.GetTicket(), proto.MarshalOptions{Deterministic: true})
			if err != nil {
				return err
			}

			outBytes, err := protoMarshaller.Marshal(a)
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(os.Stdout, string(outBytes))
			if err != nil {
				return err
			}

			return nil
		},
	}
	getCmd.Flags().String("ticket-id", "", "Ticket ID to fetch")
	cmd.AddCommand(getCmd)

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a ticket",
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
			if err != nil {
				return err
			}

			runCtx, err := initLogger(
				ctx,
				name,
				logging.WithLogFormat(v.GetString("log-format")),
				logging.WithLogLevel(v.GetString("log-level")),
			)
			if err != nil {
				return err
			}

			schemaID := v.GetString("schema-id")
			if schemaID == "" {
				return fmt.Errorf("--schema-id is required")
			}

			c, err := getConnector(runCtx, cfg)
			if err != nil {
				return err
			}

			schema, err := c.GetTicketSchema(runCtx, &v2.TicketsServiceGetTicketSchemaRequest{
				Id: schemaID,
			})
			if err != nil {
				return err
			}

			ticketTemplate := v.GetString("template")
			if ticketTemplate == "" {
				return fmt.Errorf("template is required")
			}

			tbytes, err := os.ReadFile(ticketTemplate)
			if err != nil {
				return err
			}

			template := &TicketTemplate{}
			err = json.Unmarshal(tbytes, template)
			if err != nil {
				return err
			}

			ticketReq := &v2.TicketsServiceCreateTicketRequest{
				DisplayName: template.DisplayName,
				Description: template.Description,
				Status: &v2.TicketStatus{
					Id: template.StatusId,
				},
				Type: &v2.TicketType{
					Id: template.TypeId,
				},
				Labels:   template.Labels,
				SchemaId: schema.Schema.GetId(),
			}

			cfs := make(map[string]*v2.TicketCustomField)
			for k, v := range template.CustomFields {
				newCfs, err := sdkTicket.CustomFieldForSchemaField(k, schema.Schema, v)
				if err != nil {
					return err
				}
				cfs[k] = newCfs
			}
			ticketReq.CustomFields = cfs

			ticket, err := c.CreateTicket(runCtx, ticketReq)
			if err != nil {
				return err
			}

			if ticket.GetTicket() == nil {
				return fmt.Errorf("connector returned empty ticket")
			}

			protoMarshaller := protojson.MarshalOptions{
				Multiline: true,
				Indent:    "  ",
			}

			a := &anypb.Any{}
			err = anypb.MarshalFrom(a, ticket.GetTicket(), proto.MarshalOptions{Deterministic: true})
			if err != nil {
				return err
			}

			outBytes, err := protoMarshaller.Marshal(a)
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(os.Stdout, string(outBytes))
			if err != nil {
				return err
			}

			return nil
		},
	}
	createCmd.Flags().String("template", "", "Path to JSON template for ticket creation")
	createCmd.Flags().String("schema-id", "", "Schema ID to use for ticket creation")
	cmd.AddCommand(createCmd)
	return cmd
}

type TicketTemplate struct {
	StatusId     string                 `json:"status_id"`
	TypeId       string                 `json:"type_id"`
	DisplayName  string                 `json:"display_name"`
	Description  string                 `json:"description"`
	Labels       []string               `json:"labels"`
	CustomFields map[string]interface{} `json:"custom_fields"`
}
