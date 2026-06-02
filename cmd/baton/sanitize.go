package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/c1zsanitize"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

func sanitizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sanitize",
		Short: "Write an identity-stripped copy of a c1z whose graph topology and cardinalities are preserved",
		RunE:  runSanitize,
	}

	cmd.Flags().String("out", "", "Path to the sanitized .c1z output file (required)")
	cmd.Flags().String("secret-file", "", "Path to a per-c1z HMAC secret (>=32 random bytes). If unset, a fresh secret is generated and written next to --out.")
	cmd.Flags().String("anchor", "", "RFC3339 timestamp the newest source timestamp lands on. Defaults to now.")
	cmd.Flags().Bool("allow-unknown-annotations", false, "Pass annotations of unknown type through unchanged instead of dropping. Dangerous on real customer data.")

	return cmd
}

func runSanitize(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("info"))
	if err != nil {
		return fmt.Errorf("init logging: %w", err)
	}
	log := ctxzap.Extract(ctx)

	inPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}
	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}
	secretFile, err := cmd.Flags().GetString("secret-file")
	if err != nil {
		return err
	}
	anchorRaw, err := cmd.Flags().GetString("anchor")
	if err != nil {
		return err
	}
	allowUnknown, err := cmd.Flags().GetBool("allow-unknown-annotations")
	if err != nil {
		return err
	}
	if outPath == "" {
		return fmt.Errorf("--out is required")
	}

	secret, generated, err := c1zsanitize.LoadOrGenerateSecret(secretFile, outPath)
	if err != nil {
		return err
	}
	if generated {
		log.Warn("c1zsanitize: generated fresh secret; archive it if you want reversibility",
			zap.String("path", c1zsanitize.SecretPath(secretFile, outPath)))
	}

	var anchor time.Time
	if anchorRaw != "" {
		anchor, err = time.Parse(time.RFC3339, anchorRaw)
		if err != nil {
			return fmt.Errorf("parse --anchor: %w", err)
		}
	}

	if _, err := os.Stat(inPath); err != nil {
		return fmt.Errorf("stat --file: %w", err)
	}
	if _, err := os.Stat(outPath); err == nil {
		return fmt.Errorf("--out path %q already exists; refusing to overwrite", outPath)
	}

	src, err := dotc1z.NewC1ZFile(ctx, inPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return fmt.Errorf("open source c1z: %w", err)
	}
	defer src.Close(ctx)

	// The dst is a net-new intermediate that is discarded on any
	// failure, so durability pragmas only cost throughput: skip the
	// journal and fsync entirely, and give SQLite a 64MB page cache to
	// cut index-maintenance misses on large (multi-million-grant) syncs.
	dst, err := dotc1z.NewC1ZFile(ctx, outPath,
		dotc1z.WithPragma("journal_mode", "OFF"),
		dotc1z.WithPragma("synchronous", "OFF"),
		dotc1z.WithPragma("cache_size", "-65536"),
	)
	if err != nil {
		return fmt.Errorf("open dst c1z: %w", err)
	}
	dstClosed := false
	defer func() {
		if !dstClosed {
			_ = dst.Close(ctx)
		}
	}()

	opts := c1zsanitize.Options{
		Secret:                  secret,
		TimestampAnchor:         anchor,
		AllowUnknownAnnotations: allowUnknown,
	}

	log.Info("c1zsanitize: starting",
		zap.String("in", inPath),
		zap.String("out", outPath),
		zap.Bool("drop_unknown_annotations", !opts.AllowUnknownAnnotations),
	)
	start := time.Now()
	if err := c1zsanitize.Sanitize(ctx, src, dst, opts); err != nil {
		return fmt.Errorf("sanitize: %w", err)
	}
	// Close on the success path flushes and zstd-compresses the sqlite
	// output, so a Close failure means the .c1z is incomplete/corrupt —
	// surface it rather than exit 0 with a broken file. The deferred close
	// above stays as a safety net for the error-return paths only.
	dstClosed = true
	if err := dst.Close(ctx); err != nil {
		return fmt.Errorf("failed to finalize output c1z: %w", err)
	}
	log.Info("c1zsanitize: done", zap.Duration("elapsed", time.Since(start)))
	return nil
}
