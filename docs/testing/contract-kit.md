# v1 consumer contract kit

The v1 wire contract is the JSON Schemas in `golang/api/schema/v1` and the
shared fixture corpus in `golang/llm/testdata/v1`, including the two manifests
that classify it: `negative-fixtures.tsv` (the contract and field each negative
fixture breaks) and `media-url-policy.tsv` (remote-media URLs the worker blocks
or allows). The contract kit packages exactly those files so a caller outside
this repository can prove its own encoder and decoder against the schema
revision it deploys against, without cloning the repository (#1575).

The kit is available three ways. All three carry the same files and the same
`contract-kit.json` manifest.

| Form | Where | Use it to |
| --- | --- | --- |
| Go package | `github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit` | test a Go codec in `go test` |
| Command | `golang/contractkit/cmd/contractkit` | test a codec in any language |
| Release tarball | `llmtw-contract-kit-vX.Y.Z.tar.gz` and its `.sha256`, in the `contract-kit` artifact of a guarded release run | pin the exact kit of a release |

## Layout and schema digest

```text
llmtw-contract-kit-vX.Y.Z/
├── contract-kit.json        manifest: digests and every file's size and SHA-256
├── schema/v1/*.schema.json  the published schemas
└── fixtures/v1/             *.json fixtures, negative-fixtures.tsv, media-url-policy.tsv
```

`contract-kit.json` names the kit's API version and two digests.
`schema_digest` identifies the schema revision; `fixtures_digest` does the
same for the fixtures. Each is the SHA-256 of the `sha256sum` listing of that
directory's files in byte-wise path order, so you can recompute either one
without this repository:

```sh
cd llmtw-contract-kit-vX.Y.Z
find schema/v1 -type f | LC_ALL=C sort | xargs sha256sum | sha256sum
```

The output, prefixed with `sha256:`, is `schema_digest`. The release evidence
records the same manifest as its `contract_kit` artifact. See
[Release artifact](#release-artifact).

## Running the kit against a codec

A codec passes the kit when, for every fixture:

- **Positive fixture**: the codec decodes it and writes an encoding, and that
  encoding:
  - satisfies the contract's schema;
  - decodes with the worker's own strict codec;
  - carries the fixture's value. The worker codec's re-encodings of the
    fixture and of your encoding must match. They may differ only in member
    order, number spelling, how one RFC 3339 instant is written, or an
    optional member written as `null` or `[]` instead of being omitted.
- **Negative fixture**: the codec rejects it. `negative-fixtures.tsv` names the
  contract and the field each one breaks. The Go and OCaml codecs in this
  repository also name that field in their error, but the kit only requires
  the rejection.

The contract follows from the fixture name: `generate-response*`,
`compact-response*`, `generate-*`, `compact-*`, `query-*-response.json` and
`query-*`, checked in that order. For negative fixtures, `negative-fixtures.tsv`
names the contract instead.

### From any language

The `roundtrip` command runs your codec as a subprocess once per fixture. For
each run:

- standard input holds the fixture bytes;
- `LLMTW_CONTRACT` names the contract (for example `generate-request`) and
  `LLMTW_FIXTURE` names the fixture file;
- your codec decodes the input as that contract, writes its own encoding to
  standard output, and exits 0;
- if it rejects the input, it exits nonzero (standard error becomes the
  reported reason).

```sh
go run github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit/cmd/contractkit@<commit> \
  roundtrip -timeout 30s -- ./my-codec --wire-test
```

The command prints one `ok` or `FAIL` line per fixture and ends with the schema
digest it tested against. It exits 1 when a fixture fails and 2 on misuse.

Other commands:

- `validate -contract generate-request FILE...` checks documents your caller
  emitted, or standard input when no file is given. It applies the schema and,
  for the six fixture contracts, the worker's strict decoder. Every schema in
  the kit can be named, including `activity-request`, `prepare-execution` and
  `config`. Contracts without a worker codec are checked against the schema
  only.
- `list` prints each fixture, its contract and `positive` or `negative`,
  separated by tabs, if you would rather drive the corpus from your own test
  runner.
- `export -dir DIR` writes the kit as a directory.

`roundtrip`, `validate`, `list` and `export` all accept `-kit DIR`, which reads
an extracted release tarball instead of the kit built into the command.
Loading a directory fails unless `contract-kit.json` describes exactly the
files present.

The Go module is the `golang/` subdirectory of this repository, and release
tags such as `v1.2.3` are repository tags, not versions of that module. Pin the
command with the release tag's commit (`@<commit>`), or run it from any recent
checkout with `-kit` pointing at the tarball of the release you deploy against.

### From Go

```go
kit, err := contractkit.Embedded()
if err != nil {
	t.Fatal(err)
}
for _, result := range contractkit.Failed(kit.RoundTrip(myCodec)) {
	t.Errorf("%s: %v", result.Fixture, result.Err)
}
```

`myCodec` has the signature `func(contract contractkit.Contract, fixture string,
data []byte) ([]byte, error)`. Other entry points:

- `kit.Check` and `kit.Validate` judge a single document.
- `kit.CheckEncoding` judges your encoding of one named fixture.
- `kit.Fixtures` and `kit.MediaURLPolicy` expose the parsed corpus.
- `contractkit.Load(os.DirFS(dir))` reads an extracted tarball.
- `contractkit.Command` runs an external codec, as `roundtrip` does.

If you need only the raw files, `schemav1.FS()` (package
`golang/api/schema/v1`) embeds the schemas and `llm.V1ContractFixtures()`
embeds the fixtures.

## Release artifact

The guarded release workflow (`.github/workflows/release.yml`) packages the
kit at the end of its preflight, after it has verified the evidence and
rescanned the tag:

1. `scripts/release/guard.sh verify-evidence` requires the master evidence
   bundle to record a `contract_kit` artifact. The master `release-evidence`
   job writes that artifact (`contract-kit.json`) from the kit embedded at the
   evidence revision. `releaseverify` recomputes its digests when it records
   and verifies the bundle.
2. `guard.sh package-contract-kit` reads only `golang/api/schema/v1` and
   `golang/llm/testdata/v1` from the tag commit with `git archive`, so no tag
   code runs. The packager is built from the trusted master module. Packaging
   fails unless the tag's files match that `contract-kit.json`, schema digest
   included.
3. The workflow uploads `llmtw-contract-kit-vX.Y.Z.tar.gz`, its `.sha256` and
   `contract-kit.json` as the `contract-kit` artifact. The tarball is
   deterministic: packaging the same kit and version always produces the same
   bytes.

The protected publication job still stops before creating a GitHub release
(see the [release runbook](../release/runbook.md#guarded-manual-publication-boundary)),
and no guarded release can run yet: the workflow needs release variables that
are not configured (#1325). Until a release job attaches the tarball to a
GitHub release, the `contract-kit` artifact of the guarded run is the release
artifact. Check it with `sha256sum --check llmtw-contract-kit-vX.Y.Z.tar.gz.sha256`
before use.

## The OCaml client

The OCaml client's tests do not read the kit directly. CI installs the opam
package from the `ocaml/llm_temporal_worker` subdirectory alone (`opam pin
--subpath`), so its tests cannot reach `golang/`. They therefore keep a copy in
`ocaml/llm_temporal_worker/test/fixtures/v1`. `golang/contractkit` checks that
copy against the kit:

- `TestOCamlFixturesMirrorTheKit` fails unless the copy is byte-identical to
  the kit's `fixtures/v1`, both manifests included, with no file missing or
  extra.
- `TestOCamlEncodingsPassTheKit` runs `CheckEncoding`, the consumer runner's
  own positive check, on the OCaml codec's pinned encoding of every positive
  fixture (`fixtures/v1-encoded`).

## Changing the contract

1. Edit the schema or fixture under `golang/api/schema/v1` or
   `golang/llm/testdata/v1`.
2. List every new negative fixture in `negative-fixtures.tsv`. The kit refuses
   to load an unlisted negative fixture, and also a positive fixture whose
   name does not identify its contract.
3. Copy the changed files to `ocaml/llm_temporal_worker/test/fixtures/v1`.
4. Regenerate the OCaml encodings with `LLMTW_WRITE_ENCODED_FIXTURES`.

The embedded kit, the evidence manifest and the next release tarball pick up
the change automatically.
