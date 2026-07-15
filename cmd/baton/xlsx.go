package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/logging"
	restypes "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/xuri/excelize/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func exportXLSX() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "xlsx",
		Short: "Export XLSX for upload",
		RunE:  runExportXLSX,
	}

	cmd.Flags().String("out", "./sync.xlsx", "The path to export the XLSX to")

	return cmd
}

type excelRow struct {
	rowType                string
	lastName               string
	firstName              string
	userID                 string
	userStatus             string
	emailAddress           string
	entitlementDisplayName string
	entitlement            string
	resourceType           string
	resourceName           string
	entitlementDescription string
	entitlementSlug        string
}

func (c excelRow) Row(sheet string, row int, f *excelize.File) error {
	for i := header(1); i < headerTerminator; i++ {
		switch i {
		case headerType:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.rowType))
			if err != nil {
				return err
			}

		case headerLastName:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.lastName))
			if err != nil {
				return err
			}
		case headerFirstName:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.firstName))
			if err != nil {
				return err
			}
		case headerUserID:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.userID))
			if err != nil {
				return err
			}
		case headerUserStatus:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.userStatus))
			if err != nil {
				return err
			}
		case headerEmailAddress:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.emailAddress))
			if err != nil {
				return err
			}
		case headerEntitlementDisplay:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.entitlementDisplayName))
			if err != nil {
				return err
			}
		case headerEntitlement:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.entitlement))
			if err != nil {
				return err
			}
		case headerResourceType:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.resourceType))
			if err != nil {
				return err
			}
		case headerResourceName:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.resourceName))
			if err != nil {
				return err
			}
		case headerEntitlementDescription:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}
			err = f.SetCellValue(sheet, cell, sanitizeCell(c.entitlementDescription))
			if err != nil {
				return err
			}

		case headerEntitlementSlug:
			cell, err := excelize.CoordinatesToCellName(int(i), row)
			if err != nil {
				return err
			}

			err = f.SetCellValue(sheet, cell, sanitizeCell(c.entitlementSlug))
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected header")
		}
	}

	return nil
}

func runExportXLSX(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("error"))
	if err != nil {
		return err
	}
	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}

	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}

	store, err := openReadOnlyC1ZStore(ctx, c1zPath)
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	resourceTypes, err := fetchResourceTypes(ctx, store)
	if err != nil {
		return err
	}
	resourcesByID, resourcesByType, err := fetchResources(ctx, store)
	if err != nil {
		return err
	}
	entitlementsByID, entitlementsByResource, err := fetchEntitlements(ctx, store)
	if err != nil {
		return err
	}
	grantsByID, grantsByPrincipal, err := fetchGrants(ctx, store)
	if err != nil {
		return err
	}

	d := dataBag{
		resourceTypes:      resourceTypes,
		resourcesByID:      resourcesByID,
		resourcesByType:    resourcesByType,
		entitlementsByID:   entitlementsByID,
		entitlementsByType: entitlementsByResource,
		grantsByID:         grantsByID,
		grantsByType:       grantsByPrincipal,
	}

	err = buildXLSX(ctx, d, outPath)
	if err != nil {
		return err
	}

	return nil
}

func buildXLSX(ctx context.Context, d dataBag, outPath string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("building XLSX")

	f := excelize.NewFile()
	sheet := "Sheet1"
	row := 1

	for ii, h := range headers() {
		cell, err := excelize.CoordinatesToCellName(ii+1, row)
		if err != nil {
			return err
		}

		err = f.SetCellValue(sheet, cell, h)
		if err != nil {
			return err
		}
	}
	row++

	// Identity
	var userTypes []string
	for _, rt := range d.resourceTypes {
		for _, t := range rt.Traits {
			if t == v2.ResourceType_TRAIT_USER {
				userTypes = append(userTypes, rt.Id)
			}
		}
	}

	for _, userType := range userTypes {
		var ok bool
		var rs map[string]*v2.Resource

		rs, ok = d.resourcesByType[userType]
		if !ok {
			continue
		}

		for _, r := range rs {
			ut := &v2.UserTrait{}
			annos := annotations.Annotations(r.Annotations)
			ok, err := annos.Pick(ut)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			var emailAddress string
			for _, e := range ut.Emails {
				if e.IsPrimary {
					emailAddress = e.Address
					break
				}
			}

			profile := restypes.GetProfile(r).GetFields()
			er := excelRow{
				rowType:      "Identity",
				lastName:     profile["last_name"].GetStringValue(),
				firstName:    profile["first_name"].GetStringValue(),
				userID:       profile["user_id"].GetStringValue(),
				userStatus:   getUserStatus(r),
				emailAddress: emailAddress,
			}

			err = er.Row(sheet, row, f)
			if err != nil {
				return err
			}
			row++
		}
	}

	for _, e := range d.entitlementsByID {
		er := excelRow{
			rowType:                "Entitlement",
			entitlementDisplayName: e.DisplayName,
			entitlement:            e.Id,
			resourceType:           e.Resource.Id.ResourceType,
			resourceName:           e.Resource.DisplayName,
			entitlementDescription: e.Description,
			entitlementSlug:        e.Slug,
		}

		err := er.Row(sheet, row, f)
		if err != nil {
			return err
		}
		row++
	}

	for _, g := range d.grantsByID {
		if p, ok := d.resourcesByID[fmtResourceID(g.Principal.Id)]; ok {
			ut := &v2.UserTrait{}

			annos := annotations.Annotations(p.Annotations)
			ok, err := annos.Pick(ut)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			var emailAddress string
			for _, e := range ut.Emails {
				if e.IsPrimary {
					emailAddress = e.Address
					break
				}
			}
			profile := restypes.GetProfile(p).GetFields()

			var e *v2.Entitlement
			if en, ok := d.entitlementsByID[g.Entitlement.Id]; ok {
				e = en
			} else {
				// Fallback when the grant references an entitlement that
				// isn't in the c1z. The grant carries the entitlement's
				// ID but not its display fields, so the row renders with
				// blank metadata. There's no source to pull from — the
				// entitlement is genuinely missing.
				e = g.Entitlement
			}

			er := excelRow{
				rowType:                "Grant",
				lastName:               profile["last_name"].GetStringValue(),
				firstName:              profile["first_name"].GetStringValue(),
				userID:                 profile["user_id"].GetStringValue(),
				emailAddress:           emailAddress,
				entitlementDisplayName: e.DisplayName,
				entitlement:            e.Id,
				resourceType:           e.Resource.Id.ResourceType,
				resourceName:           e.Resource.DisplayName,
				entitlementDescription: e.Description,
				entitlementSlug:        e.Slug,
			}

			err = er.Row(sheet, row, f)
			if err != nil {
				return err
			}
			row++
		}
	}

	if err := f.SaveAs(outPath); err != nil {
		return err
	}

	return nil
}
