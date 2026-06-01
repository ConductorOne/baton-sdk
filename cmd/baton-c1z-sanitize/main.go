// baton-c1z-sanitize transforms a .c1z snapshot into an identity-
// stripped copy whose graph topology and cardinalities are preserved.
// See pkg/c1zsanitize for the transform contract.
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/c1zsanitize"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "baton-c1z-sanitize:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("baton-c1z-sanitize", flag.ContinueOnError)
	inPath := fs.String("in", "", "Path to source .c1z file (required)")
	outPath := fs.String("out", "", "Path to sanitized .c1z output file (required)")
	secretFile := fs.String("secret-file", "", "Path to per-c1z HMAC secret (>=32 random bytes). If unset, a fresh secret is generated and written next to -out.")
	anchorRaw := fs.String("anchor", "", "RFC3339 timestamp the newest source timestamp lands on. Defaults to now.")
	allowUnknown := fs.Bool("allow-unknown-annotations", false, "Pass annotations of unknown type through unchanged instead of dropping. Dangerous on real customer data.")
	logLevel := fs.String("log-level", "info", "Log level: debug, info, warn, error.")
	logFormat := fs.String("log-format", "console", "Log format: console or json.")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *inPath == "" || *outPath == "" {
		fs.Usage()
		return fmt.Errorf("-in and -out are required")
	}

	ctx, err := logging.Init(context.Background(),
		logging.WithLogLevel(*logLevel),
		logging.WithLogFormat(*logFormat),
	)
	if err != nil {
		return fmt.Errorf("init logging: %w", err)
	}
	log := ctxzap.Extract(ctx)

	secret, generated, err := loadOrGenerateSecret(*secretFile, *outPath)
	if err != nil {
		return err
	}
	if generated {
		log.Warn("c1zsanitize: generated fresh secret; archive it if you want reversibility",
			zap.String("path", secretPath(*secretFile, *outPath)))
	}

	var anchor time.Time
	if *anchorRaw != "" {
		anchor, err = time.Parse(time.RFC3339, *anchorRaw)
		if err != nil {
			return fmt.Errorf("parse -anchor: %w", err)
		}
	}

	if _, err := os.Stat(*inPath); err != nil {
		return fmt.Errorf("stat -in: %w", err)
	}
	if _, err := os.Stat(*outPath); err == nil {
		return fmt.Errorf("-out path %q already exists; refusing to overwrite", *outPath)
	}

	src, err := dotc1z.NewC1ZFile(ctx, *inPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return fmt.Errorf("open source c1z: %w", err)
	}
	defer src.Close(ctx)

	dst, err := dotc1z.NewC1ZFile(ctx, *outPath)
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
		AllowUnknownAnnotations: *allowUnknown,
	}

	log.Info("c1zsanitize: starting",
		zap.String("in", *inPath),
		zap.String("out", *outPath),
		zap.Bool("drop_unknown_annotations", !opts.AllowUnknownAnnotations),
	)
	start := time.Now()
	if err := c1zsanitize.Sanitize(ctx, src, dst, opts); err != nil {
		return fmt.Errorf("sanitize: %w", err)
	}
	dstClosed = true
	if err := dst.Close(ctx); err != nil {
		return fmt.Errorf("close dst c1z: %w", err)
	}
	log.Info("c1zsanitize: done", zap.Duration("elapsed", time.Since(start)))
	return nil
}

func secretPath(flagPath, outPath string) string {
	if flagPath != "" {
		return flagPath
	}
	return outPath + ".secret"
}

func loadOrGenerateSecret(flagPath, outPath string) ([]byte, bool, error) {
	if flagPath != "" {
		b, err := os.ReadFile(flagPath)
		if err != nil {
			return nil, false, fmt.Errorf("read -secret-file: %w", err)
		}
		if len(b) < c1zsanitize.MinSecretBytes {
			return nil, false, fmt.Errorf("-secret-file %q is too short: got %d bytes, want at least %d", flagPath, len(b), c1zsanitize.MinSecretBytes)
		}
		return b, false, nil
	}
	path := secretPath(flagPath, outPath)
	if _, err := os.Stat(path); err == nil {
		return nil, false, fmt.Errorf("default secret path %q already exists; pass -secret-file to reuse it", path)
	}
	b := make([]byte, c1zsanitize.MinSecretBytes)
	if _, err := rand.Read(b); err != nil {
		return nil, false, fmt.Errorf("generate secret: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, false, fmt.Errorf("write generated secret: %w", err)
	}
	return b, true, nil
}
