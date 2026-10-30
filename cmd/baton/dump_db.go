package main

import (
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/spf13/cobra"
)

func dumpDBCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "dump-db",
		Short:  "Extract the Pebble payload directory from a v3 C1Z",
		RunE:   runDumpDB,
		Hidden: true,
	}

	cmd.Flags().String("out", "./sync.db", "Directory to extract the Pebble payload into")
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
	if format != dotc1z.C1ZFormatV3 {
		return fmt.Errorf("dump-db: only v3 c1z files are supported (got %s)", format)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if err := os.MkdirAll(outPath, 0o755); err != nil {
		return err
	}
	// ExtractEnvelopePayload dispatches on the manifest's payload
	// encoding (tar, tar_zstd, indexed_zstd).
	if _, _, err := formatv3.ExtractEnvelopePayload(f, outPath); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stdout, "Extracted Pebble payload directory to %s\n", outPath)
	return nil
}
