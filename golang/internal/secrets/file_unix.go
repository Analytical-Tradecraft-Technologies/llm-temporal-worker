//go:build aix || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd || solaris

package secrets

import (
	"os"
	"syscall"
)

func openSecretFile(path string) (*os.File, error) {
	// Nonblocking prevents a FIFO introduced between Stat and Open from
	// stalling, while O_NOCTTY prevents a raced terminal device from becoming
	// the process controlling terminal. ReadSecretFile rejects either target
	// from the opened descriptor before reading it.
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOCTTY, 0)
}
