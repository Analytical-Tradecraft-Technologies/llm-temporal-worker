package app

import "errors"

// ReloadStage names the step of Reload that rejected a replacement. It lets
// the process boundary report a bounded cause without parsing error text.
type ReloadStage string

const (
	// ReloadStageBuild is decoding, validating and resolving the replacement.
	ReloadStageBuild ReloadStage = "build"
	// ReloadStageReplacement is the comparison with the active snapshot.
	ReloadStageReplacement ReloadStage = "replacement"
	// ReloadStageClients is constructing the replacement's clients.
	ReloadStageClients ReloadStage = "clients"
	// ReloadStageVerify is checking the replacement's dependencies.
	ReloadStageVerify ReloadStage = "verify"
)

// reloadStageError tags a rejection with its stage. The message is unchanged.
type reloadStageError struct {
	stage ReloadStage
	err   error
}

func (err *reloadStageError) Error() string { return err.err.Error() }

func (err *reloadStageError) Unwrap() error { return err.err }

// ReloadStageOf returns the stage that rejected a replacement and the error it
// returned. The stage is empty when err did not come from a Reload stage, for
// example when the file could not be read.
func ReloadStageOf(err error) (ReloadStage, error) {
	var staged *reloadStageError
	if !errors.As(err, &staged) {
		return "", err
	}
	return staged.stage, staged.err
}
