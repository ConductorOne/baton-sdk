package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/conductorone/baton-sdk/pkg/baton/explorer"
	"github.com/spf13/cobra"
)

func explorerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "explorer",
		Short: "Run explorer UI in local browser",
		RunE:  runExplorer,
	}

	addResourceTypeFlag(cmd)
	addSyncIDFlag(cmd)

	cmd.Flags().IntP("port", "p", 8080, "Port to run the explorer server on")
	cmd.Flags().Bool("dev", false, "Runs the frontend in development mode")
	err := cmd.Flags().MarkHidden("dev")
	if err != nil {
		log.Default().Println("error marking dev flag hidden", err)
	}

	return cmd
}

func runNpmInstallAndStart(projectPath string) error {
	ctx := context.Background()
	installCmd := exec.CommandContext(ctx, "npm", "install")
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	installCmd.Dir = projectPath
	if err := installCmd.Run(); err != nil {
		return fmt.Errorf("error running 'npm install': %w", err)
	}

	startCmd := exec.CommandContext(ctx, "npm", "run", "dev")
	startCmd.Stdout = os.Stdout
	startCmd.Stderr = os.Stderr
	startCmd.Dir = projectPath
	if err := startCmd.Run(); err != nil {
		return fmt.Errorf("error running 'npm start': %w", err)
	}

	return nil
}

func startFrontendServer() error {
	err := runNpmInstallAndStart("frontend")
	if err != nil {
		return fmt.Errorf("error running npm start: %w", err)
	}

	return nil
}

// startExplorerAPI loads the c1z and runs the explorer HTTP server.
// Returns error so deferred Closes on m and store fire on the
// return path; the previous log.Fatal-based shape skipped them via
// os.Exit and left the sqlite WAL un-checkpointed and the meta
// store un-flushed.
func startExplorerAPI(cmd *cobra.Command, devMode bool, port int) error {
	ctx := cmd.Context()

	filePath, err := cmd.Flags().GetString("file")
	if err != nil {
		return fmt.Errorf("error fetching file path: %w", err)
	}

	syncID, err := cmd.Flags().GetString("sync-id")
	if err != nil {
		return fmt.Errorf("error fetching syncID: %w", err)
	}

	resourceType, err := cmd.Flags().GetString(resourceTypeFlag)
	if err != nil {
		return fmt.Errorf("error fetching resourceType: %w", err)
	}

	store, err := openReadOnlyC1ZStore(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error loading c1z: %w", err)
	}
	defer store.Close(ctx)

	addr := fmt.Sprintf(":%d", port)
	ctrl, err := explorer.NewController(ctx, store, syncID, resourceType, devMode)
	if err != nil {
		return fmt.Errorf("error creating explorer controller: %w", err)
	}
	if err := ctrl.Run(addr); err != nil {
		return fmt.Errorf("error running explorer: %w", err)
	}
	return nil
}

func runExplorer(cmd *cobra.Command, args []string) error {
	isDevMode, err := cmd.Flags().GetBool("dev")
	if err != nil {
		return fmt.Errorf("error getting dev flag: %w", err)
	}

	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return fmt.Errorf("error getting port flag: %w", err)
	}

	if isDevMode {
		// API goroutine has no return channel; log + continue.
		// The frontend server's exit is the loop's terminator.
		go func() {
			if apiErr := startExplorerAPI(cmd, isDevMode, port); apiErr != nil {
				log.Default().Printf("explorer API exited: %v", apiErr)
			}
		}()
		if err := startFrontendServer(); err != nil {
			return fmt.Errorf("error running frontend server: %w", err)
		}
		return nil
	}
	return startExplorerAPI(cmd, isDevMode, port)
}
