//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris

package secrets

import "os"

func openSecretFile(path string) (*os.File, error) {
	return os.Open(path)
}
