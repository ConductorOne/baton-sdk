package main

import (
	"fmt"
	"io"
	"os"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/spf13/cobra"
)

func dumpDBCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "dump-db",
		Short:  "Dump the underlying database for the C1Z provided",
		RunE:   runDumpDB,
		Hidden: true,
	}

	cmd.Flags().String("out", "./sync.db", "The path to dump the database to")
	return cmd
}

func runDumpDB(cmd *cobra.Command, args []string) error {
	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}

	f, err := os.Open(c1zPath)
	if err != nil {
		return err
	}
	defer f.Close()

	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}

	if outPath == "" {
		return fmt.Errorf("an output path is required")
	}

	format, err := dotc1z.ReadHeaderFormat(f)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if format == dotc1z.C1ZFormatV3 {
		env, err := formatv3.ReadEnvelope(f)
		if err != nil {
			return err
		}
		defer env.Close()
		if err := os.MkdirAll(outPath, 0o755); err != nil {
			return err
		}
		if err := formatv3.ExtractZstdTar(env.PayloadReader, outPath); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "Extracted Pebble payload directory to %s\n", outPath)
		return nil
	}

	dbFile, err := dotc1z.NewC1ZFileDecoder(f)
	if err != nil {
		return err
	}
	defer dbFile.Close()

	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, dbFile)
	if err != nil {
		return err
	}

	return nil
}
