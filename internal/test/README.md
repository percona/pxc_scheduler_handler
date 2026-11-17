# Testing

## Unit tests

Standard Go tests — no build tags required, no external dependencies:

```bash
go test ./...
```

Run with race detector:

```bash
go test -race ./...
```

## Property-based tests (Hegel)

Property tests live in `internal/test/hegel/` and are compiled only when the
`hegel` build tag is set.  They use [Hegel](https://hegel.dev/go) to generate
random inputs and verify invariants across many trials.

### Quick run — all properties, default 100 trials each

```bash
go test -tags hegel -count=100 ./internal/test/hegel/...
```

### Verbose output

```bash
go test -tags hegel -v -count=100 ./internal/test/hegel/...
```

### Single test — useful after a failure

```bash
go test -tags hegel -v -count=1 -run '^TestProp_ClusterLock_SecondSchedulerDoesNotWinActiveLock$' ./internal/test/hegel/...
```

### Deep search — increase trials for a harder counterexample hunt

```bash
go test -tags hegel -v -count=10000 -run '^TestProp_getRetry_WrongHGPairNotParsed$' ./internal/test/hegel/...
```

### Fuzz tests

Fuzz targets are in `internal/test/hegel/fuzz_test.go`.  Their seed corpora run
as ordinary unit tests on every `-tags hegel` invocation.  To explore beyond the
seeds, pass `-fuzz`:

```bash
# Run seeds only (part of normal test run)
go test -tags hegel -run '^FuzzConfigSanityCheck$' ./internal/test/hegel/...

# Continuous fuzzing — runs until interrupted with Ctrl-C
go test -tags hegel -fuzz=FuzzConfigSanityCheck ./internal/test/hegel/...
go test -tags hegel -fuzz=FuzzCheckLockFileExists ./internal/test/hegel/...
go test -tags hegel -fuzz=FuzzSaveRetrySQL ./internal/test/hegel/...
```

### Full report with priority badges

The report script runs all property tests and generates a Markdown summary
grouped by component and annotated with priority badges
(🔴 HIGH / 🟡 MEDIUM / 🟢 LOW):

```bash
# Default — 100 trials, report written to hegel_report.md
./scripts/hegel_report.sh

# Explicit Go binary (e.g. when go.mod requires a newer toolchain)
./scripts/hegel_report.sh /usr/local/go1.26/bin/go

# Custom trial count
HEGEL_COUNT=500 ./scripts/hegel_report.sh

# Custom output file
HEGEL_REPORT=reports/nightly.md ./scripts/hegel_report.sh
```

### Priority legend

| Badge | Meaning |
|---|---|
| 🔴 HIGH | Critical cluster safety invariant — failure can cause data unavailability or incorrect failover |
| 🟡 MEDIUM | Important management feature misbehaves — retries, locking, config validation |
| 🟢 LOW | Edge case or secondary check — no immediate production impact |

### How bridge files work

Hegel tests live in the external package `internal/test/hegel` and cannot
access unexported functions directly.  Each internal package that needs testing
ships a `hegel_bridge.go` file (compiled only with `-tags hegel`) that exports
thin wrappers:

```
internal/cluster/hegel_bridge.go
internal/locker/hegel_bridge.go
internal/proxysql/hegel_bridge.go
```

If you add a new unexported function that needs property testing, add a
`Hegel<FuncName>` wrapper in the corresponding bridge file.
