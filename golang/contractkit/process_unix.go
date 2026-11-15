//go:build unix

package contractkit

import (
	"os/exec"
	"syscall"
)

// killProcessGroupOnCancel starts the codec in its own process group and
// makes cancellation kill the whole group, so a codec that is a shell
// script cannot leave a child running past the timeout.
func killProcessGroupOnCancel(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
}
