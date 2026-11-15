//go:build !unix

package contractkit

import "os/exec"

// killProcessGroupOnCancel keeps exec's default of killing the codec
// process alone; WaitDelay still bounds the wait for its pipes.
func killProcessGroupOnCancel(*exec.Cmd) {}
