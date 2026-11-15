// Command contractkit runs the v1 contract kit against a consumer's codec
// from any language, and packages the kit for a release (#1575).
//
//	contractkit validate [-kit DIR] -contract NAME [FILE...]
//	contractkit roundtrip [-kit DIR] [-timeout D] -- COMMAND [ARG...]
//	contractkit list [-kit DIR]
//	contractkit export [-kit DIR] -dir DIR
//	contractkit manifest [-source-root DIR] -output FILE
//	contractkit package -source-root DIR -version vX.Y.Z -output-dir DIR [-expect FILE]
//
// -kit reads an extracted release tarball instead of the kit compiled into
// this command. docs/testing/contract-kit.md describes each command.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
)

// errFailed reports checks that ran and failed, as opposed to misuse.
var errFailed = errors.New("contract kit checks failed")

func main() {
	err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	switch {
	case err == nil:
	case errors.Is(err, errFailed):
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, "contractkit:", err)
		os.Exit(2)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: contractkit {validate|roundtrip|list|export|manifest|package} [flags]")
	}
	commands := map[string]func([]string, io.Reader, io.Writer, io.Writer) error{
		"validate":  runValidate,
		"roundtrip": runRoundTrip,
		"list":      runList,
		"export":    runExport,
		"manifest":  runManifest,
		"package":   runPackage,
	}
	command, ok := commands[args[0]]
	if !ok {
		return fmt.Errorf("unknown command %q", args[0])
	}
	return command(args[1:], stdin, stdout, stderr)
}

func newFlags(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func loadKit(directory string) (*contractkit.Kit, error) {
	if directory == "" {
		return contractkit.Embedded()
	}
	return contractkit.Load(os.DirFS(directory))
}

// runValidate checks caller-emitted documents, one per file or standard
// input, against the named contract.
func runValidate(args []string, stdin io.Reader, stdout, _ io.Writer) error {
	flags := newFlags("validate")
	kitDirectory := flags.String("kit", "", "extracted contract kit directory (default: the embedded kit)")
	contract := flags.String("contract", "", "contract name, for example generate-request")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *contract == "" {
		return errors.New("validate requires -contract")
	}
	kit, err := loadKit(*kitDirectory)
	if err != nil {
		return err
	}
	inputs := flags.Args()
	if len(inputs) == 0 {
		inputs = []string{"-"}
	}
	failed := false
	for _, input := range inputs {
		var data []byte
		if input == "-" {
			data, err = io.ReadAll(io.LimitReader(stdin, 16<<20+1))
		} else {
			data, err = os.ReadFile(input)
		}
		if err != nil {
			return err
		}
		if err := kit.Check(contractkit.Contract(*contract), data); err != nil {
			fmt.Fprintf(stdout, "FAIL %s: %v\n", input, err)
			failed = true
			continue
		}
		fmt.Fprintf(stdout, "ok   %s\n", input)
	}
	if failed {
		return errFailed
	}
	return nil
}

// runRoundTrip runs a codec command over every fixture; see
// contractkit.Command for the protocol.
func runRoundTrip(args []string, _ io.Reader, stdout, _ io.Writer) error {
	flags := newFlags("roundtrip")
	kitDirectory := flags.String("kit", "", "extracted contract kit directory (default: the embedded kit)")
	timeout := flags.Duration("timeout", 30*time.Second, "time limit for one codec run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() == 0 || *timeout <= 0 {
		return errors.New("roundtrip requires a positive -timeout and a codec command after --")
	}
	kit, err := loadKit(*kitDirectory)
	if err != nil {
		return err
	}
	results := kit.RoundTrip(contractkit.Command(*timeout, flags.Arg(0), flags.Args()[1:]...))
	for _, result := range results {
		if result.Err != nil {
			fmt.Fprintf(stdout, "FAIL %s (%s): %v\n", result.Fixture, result.Contract, result.Err)
		} else {
			fmt.Fprintf(stdout, "ok   %s (%s)\n", result.Fixture, result.Contract)
		}
	}
	failed := len(contractkit.Failed(results))
	fmt.Fprintf(stdout, "%d of %d fixtures passed against schema digest %s\n", len(results)-failed, len(results), kit.SchemaDigest())
	if failed > 0 {
		return errFailed
	}
	return nil
}

// runList prints each fixture with its contract and polarity, tab
// separated, for consumers that drive their own loop.
func runList(args []string, _ io.Reader, stdout, _ io.Writer) error {
	flags := newFlags("list")
	kitDirectory := flags.String("kit", "", "extracted contract kit directory (default: the embedded kit)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	kit, err := loadKit(*kitDirectory)
	if err != nil {
		return err
	}
	for _, fixture := range kit.Fixtures() {
		polarity := "positive"
		if fixture.Negative {
			polarity = "negative"
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", fixture.Name, fixture.Contract, polarity)
	}
	return nil
}

// runExport writes the kit as a directory Load (and -kit) reads back.
func runExport(args []string, _ io.Reader, stdout, _ io.Writer) error {
	flags := newFlags("export")
	kitDirectory := flags.String("kit", "", "extracted contract kit directory (default: the embedded kit)")
	directory := flags.String("dir", "", "new directory to write the kit to")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" {
		return errors.New("export requires -dir")
	}
	kit, err := loadKit(*kitDirectory)
	if err != nil {
		return err
	}
	if err := kit.Export(*directory); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "exported contract kit with schema digest %s\n", kit.SchemaDigest())
	return nil
}

// runManifest writes contract-kit.json, the contract_kit release-evidence
// artifact, for the embedded kit or for a repository tree.
func runManifest(args []string, _ io.Reader, _, _ io.Writer) error {
	flags := newFlags("manifest")
	sourceRoot := flags.String("source-root", "", "repository root to read instead of the embedded kit")
	output := flags.String("output", "", "file to write contract-kit.json to")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("manifest requires -output")
	}
	kit, err := contractkit.Embedded()
	if *sourceRoot != "" {
		kit, err = contractkit.FromRepository(os.DirFS(*sourceRoot))
	}
	if err != nil {
		return err
	}
	return writeNewFile(*output, kit.ManifestJSON())
}

// runPackage builds the versioned release tarball and its sha256 file from
// a repository tree. With -expect it first requires the tree to be exactly
// the kit that manifest describes, the one the release evidence recorded.
func runPackage(args []string, _ io.Reader, stdout, _ io.Writer) error {
	flags := newFlags("package")
	sourceRoot := flags.String("source-root", "", "repository root holding golang/api/schema/v1 and golang/llm/testdata/v1")
	version := flags.String("version", "", "release tag, for example v1.2.3")
	outputDirectory := flags.String("output-dir", "", "existing empty directory for the tarball, its .sha256 and contract-kit.json")
	expect := flags.String("expect", "", "contract-kit.json the packaged kit must match")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *sourceRoot == "" || *version == "" || *outputDirectory == "" || flags.NArg() != 0 {
		return errors.New("package requires -source-root, -version and -output-dir")
	}
	kit, err := contractkit.FromRepository(os.DirFS(*sourceRoot))
	if err != nil {
		return err
	}
	if *expect != "" {
		expected, err := os.ReadFile(*expect)
		if err != nil {
			return err
		}
		recorded, err := contractkit.ParseManifest(expected)
		if err != nil {
			return fmt.Errorf("%s: %w", *expect, err)
		}
		if err := kit.Manifest().Compare(recorded); err != nil {
			return fmt.Errorf("the kit at the source root is not the kit %s records: %w", *expect, err)
		}
	}
	entries, err := os.ReadDir(*outputDirectory)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("output directory %s is not empty", *outputDirectory)
	}
	var tarball bytes.Buffer
	if err := kit.WriteTarball(&tarball, *version); err != nil {
		return err
	}
	name := contractkit.TarballName(*version)
	sum := sha256.Sum256(tarball.Bytes())
	digest := hex.EncodeToString(sum[:])
	for file, data := range map[string][]byte{
		name:                     tarball.Bytes(),
		name + ".sha256":         []byte(digest + "  " + name + "\n"),
		contractkit.ManifestName: kit.ManifestJSON(),
	} {
		if err := writeNewFile(filepath.Join(*outputDirectory, file), data); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "packaged %s sha256:%s with schema digest %s\n", name, digest, kit.SchemaDigest())
	return nil
}

func writeNewFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
