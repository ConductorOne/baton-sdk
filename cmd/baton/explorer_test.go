package main

import (
	"github.com/spf13/cobra"
)

// _ pins the post-bd-ikx3 signature of startExplorerAPI: returns
// error so the deferred m.Close and store.Close run on every exit.
// The previous shape was log.Fatal-based, which called os.Exit(1)
// and skipped the defers, leaving the sqlite WAL un-checkpointed
// and the meta store un-flushed.
//
// If a future change reverts to a no-return shape, this assignment
// stops compiling. runExplorer also propagates the error
// (`return startExplorerAPI(...)`), so the type system pins the
// fix from both sides.
var _ func(*cobra.Command, bool, int) error = startExplorerAPI
