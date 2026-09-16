//go:build windows

package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
)

// configureProcessGroup puts the child in its own process group.
//
// On Windows this is what stops a Ctrl-C aimed at the console from also
// travelling to whatever experiment it is supervising.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// killTree kills the process and everything it started.
//
// Killing only the direct child is not enough, and the difference is not
// academic: `go run` compiles to a temporary binary and runs it as a grandchild,
// and every chaos script is a bash process whose real work is the docker
// commands underneath it. Kill the parent alone and the load generator keeps
// generating load with nothing left watching it.
//
// Windows has no process-group signal that walks a tree, so this shells out to
// taskkill, which does. /T is the tree, /F is unconditional — the process is
// already being cancelled, so there is nothing to negotiate.
func killTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	if out, err := exec.Command("taskkill", "/T", "/F", "/PID", pid).CombinedOutput(); err != nil {
		// Fall back to killing just the parent. Better an orphaned grandchild
		// than a runner that never releases its lock.
		if killErr := cmd.Process.Kill(); killErr != nil {
			return fmt.Errorf("taskkill: %v (%s); kill: %w", err, out, killErr)
		}
	}
	return nil
}
