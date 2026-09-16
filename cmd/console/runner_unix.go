//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// configureProcessGroup gives the child its own process group, so the whole
// group can be signalled as a unit.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killTree kills the process and everything it started.
//
// Killing only the direct child is not enough, and the difference is not
// academic: `go run` compiles to a temporary binary and runs it as a grandchild,
// and every chaos script is a bash process whose real work is the docker
// commands underneath it. Kill the parent alone and the load generator keeps
// generating load with nothing left watching it.
//
// A negative PID signals the whole process group, which is why
// configureProcessGroup had to run first — without Setpgid the child shares the
// console's own group and this would kill the console.
func killTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return cmd.Process.Kill()
	}
	return nil
}
