#!/usr/bin/env bash
# scripts/hegel_report.sh
#
# Run ALL Hegel property-based tests in one pass, then emit a grouped
# markdown report with counterexample details for every failure.
#
# Usage:
#   ./scripts/hegel_report.sh                          # uses 'go' on $PATH
#   ./scripts/hegel_report.sh /usr/local/go1.26/bin/go # explicit binary
#   HEGEL_REPORT=out.md    ./scripts/hegel_report.sh   # custom report path
#   HEGEL_COUNT=500        ./scripts/hegel_report.sh   # trials per property

set -uo pipefail

GO_BIN="${1:-go}"
PKG="./internal/test/hegel/..."
TAGS="hegel"
COUNT="${HEGEL_COUNT:-100}"
REPORT="${HEGEL_REPORT:-hegel_report.md}"
TMPDIR_BASE=$(mktemp -d)
trap 'rm -rf "$TMPDIR_BASE"' EXIT

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "$*" >&2; }
pass() { echo -e "${GREEN}✅ $*${RESET}" >&2; }
fail() { echo -e "${RED}❌ $*${RESET}" >&2; }
info() { echo -e "${YELLOW}→  $*${RESET}" >&2; }

# ─── pre-flight: python3 ──────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  fail "python3 is required for report generation but was not found on PATH."
  exit 1
fi

# ─── pre-flight: build check ──────────────────────────────────────────────────
log ""
log "${BOLD}Hegel Property-Based Test Runner${RESET}"
log "Go binary : $GO_BIN"
log "Package   : $PKG"
log "Build tag : $TAGS"
log "Trials    : $COUNT per property"
log "Report    : $REPORT"
log ""
info "Verifying build..."
BUILD_OUT=$(mktemp -p "$TMPDIR_BASE")
if ! "$GO_BIN" test -tags "$TAGS" -count=0 "$PKG" >"$BUILD_OUT" 2>&1; then
  fail "Build failed — cannot run tests."
  echo "" >&2
  cat "$BUILD_OUT" >&2
  {
    echo "# Hegel Report — Build Failed"
    echo ""
    echo "**Generated:** $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "## Build Error"
    echo '```'
    cat "$BUILD_OUT"
    echo '```'
    echo ""
    echo "Fix the build errors above, then re-run \`./scripts/hegel_report.sh\`."
  } > "$REPORT"
  log "Partial report written to $REPORT"
  exit 1
fi
pass "Build OK"

# ─── run ALL tests in one pass ────────────────────────────────────────────────
JSON_OUT=$(mktemp -p "$TMPDIR_BASE")
info "Running all tests (count=$COUNT)..."
set +e
"$GO_BIN" test \
  -tags "$TAGS" \
  -v \
  -json \
  -count="$COUNT" \
  "$PKG" > "$JSON_OUT" 2>&1
TEST_EXIT=$?
set -e

if [[ "$TEST_EXIT" -eq 0 ]]; then
  pass "All tests passed"
else
  fail "Some tests failed — see $REPORT for details"
fi
log ""

# ─── parse output and generate report ────────────────────────────────────────
python3 - "$JSON_OUT" "$REPORT" "$GO_BIN" "$TAGS" "$COUNT" "$PKG" <<'PYEOF'
import json, sys, re
from collections import defaultdict, OrderedDict

json_file, report_file, go_bin, tags, count, pkg = sys.argv[1:]

# ── Priority legend ───────────────────────────────────────────────────────────
# HIGH   🔴 — failure means a critical cluster safety invariant is broken;
#             could cause data unavailability or incorrect failover
# MEDIUM 🟡 — failure means an important management feature misbehaves;
#             degraded behaviour but cluster remains operational
# LOW    🟢 — failure means a secondary feature or edge case is broken;
#             minimal operational impact

PRIORITY_BADGE = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🟢 LOW"}
PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

# ── Group registry ────────────────────────────────────────────────────────────
# (section, group_label, test_name_prefix, priority)
# A test belongs to the first group whose prefix matches its name.
GROUPS = [
    # Cluster Node State — core safety checks
    ("Cluster Node State",    "checkWsrepDesync",                   "TestProp_checkWsrepDesync",                  "HIGH"),
    ("Cluster Node State",    "checkAnyNotReadyStatus",              "TestProp_checkAnyNotReadyStatus",             "HIGH"),
    ("Cluster Node State",    "checkNotPrimary",                     "TestProp_checkNotPrimary",                    "HIGH"),
    ("Cluster Node State",    "checkRejectQueries",                  "TestProp_checkRejectQueries",                 "HIGH"),
    ("Cluster Node State",    "checkDonorReject",                    "TestProp_checkDonorReject",                   "HIGH"),
    ("Cluster Node State",    "checkPxcMaint",                       "TestProp_checkPxcMaint",                      "HIGH"),
    ("Cluster Node State",    "checkReadOnly",                       "TestProp_checkReadOnly",                      "HIGH"),
    ("Cluster Node State",    "checkBackOffline",                    "TestProp_checkBackOffline",                   "HIGH"),
    ("Cluster Node State",    "checkBackDesyncButUnderReplicaLag",   "TestProp_checkBackDesync",                    "HIGH"),
    ("Cluster Node State",    "checkUpSaveRetry",                    "TestProp_checkUpSaveRetry",                   "HIGH"),
    ("Cluster Node State",    "checkBackNew",                        "TestProp_checkBackNew",                       "MEDIUM"),
    ("Cluster Node State",    "checkBackPrimary",                    "TestProp_checkBackPrimary",                   "MEDIUM"),
    ("Cluster Node State",    "checkActiveFailover",                 "TestProp_checkActiveFailover",                "MEDIUM"),
    # Cluster Management
    ("Cluster Management",    "cleanWriters",                        "TestProp_cleanWriters",                       "HIGH"),
    ("Cluster Management",    "checkFailoverIfFound",                "TestProp_checkFailoverIfFound",               "MEDIUM"),
    ("Cluster Management",    "processWriterIsAlsoReader",           "TestProp_processWriterIsAlsoReader",          "MEDIUM"),
    ("Cluster Management",    "identifyLowerNodeToRemove",           "TestProp_identifyLowerNodeToRemove",          "MEDIUM"),
    ("Cluster Management",    "processUpAndDownReaders",             "TestProp_processUpAndDownReaders",            "MEDIUM"),
    # Cluster Operations
    ("Cluster Operations",    "calculateHgOnlineSize",               "TestProp_calculateHgOnlineSize",              "MEDIUM"),
    ("Cluster Operations",    "alignNodeValues",                     "TestProp_alignNodeValues",                    "MEDIUM"),
    ("Cluster Operations",    "compareNodeDefault / resetNodeDefault","TestProp_compareAndResetNodeDefault",         "LOW"),
    ("Cluster Operations",    "compareNodeDefault / resetNodeDefault","TestProp_resetNodeDefault",                   "LOW"),
    ("Cluster Operations",    "ReturnTextFromCode",                  "TestProp_ReturnTextFromCode",                 "LOW"),
    ("Cluster Operations",    "ReturnActionCategory",                "TestProp_ReturnActionCategory",               "LOW"),
    ("Cluster Operations",    "getRetry",                            "TestProp_getRetry",                           "HIGH"),
    ("Cluster Operations",    "checkMissingForNodes",                "TestProp_checkMissingForNodes",               "MEDIUM"),
    # ProxySQL SQL Builders
    ("ProxySQL SQL Builders", "MoveNodeUpFromOfflineSoft",           "TestProp_MoveNodeUpFromOfflineSoft",          "HIGH"),
    ("ProxySQL SQL Builders", "MoveNodeDownToOfflineSoft",           "TestProp_MoveNodeDownToOfflineSoft",          "HIGH"),
    ("ProxySQL SQL Builders", "MoveNodeDownToHGCange",               "TestProp_MoveNodeDownToHGCange",              "HIGH"),
    ("ProxySQL SQL Builders", "MoveNodeUpFromHGCange",               "TestProp_MoveNodeUpFromHGCange",              "HIGH"),
    ("ProxySQL SQL Builders", "InsertRead",                          "TestProp_InsertRead",                         "HIGH"),
    ("ProxySQL SQL Builders", "InsertWrite",                         "TestProp_InsertWrite",                        "HIGH"),
    ("ProxySQL SQL Builders", "SaveRetry",                           "TestProp_SaveRetry",                          "HIGH"),
    ("ProxySQL SQL Builders", "DeleteDataNode",                      "TestProp_DeleteDataNode",                     "MEDIUM"),
    ("ProxySQL SQL Builders", "ResetNodeDefaults",                   "TestProp_ResetNodeDefaults",                  "LOW"),
    # File Locker
    ("File Locker",           "EvaluateFileLockForRemoval",          "TestProp_EvaluateFileLockForRemoval",         "HIGH"),
]

def find_group(test_name):
    for section, group, prefix, priority in GROUPS:
        if test_name.startswith(prefix):
            return (section, group)
    return ("Ungrouped", test_name)

def group_priority(gk):
    for section, group, prefix, priority in GROUPS:
        if (section, group) == gk:
            return priority
    return "LOW"

# ── Parse the JSON test stream ────────────────────────────────────────────────
# go test -json emits one JSON object per line.
# Each object has Action in: run, output, pass, fail, skip, build_fail, ...
test_output  = defaultdict(list)   # test_name -> [output lines]
test_result  = {}                  # test_name -> 'pass' | 'fail' | 'skip'
test_elapsed = {}                  # test_name -> float seconds
raw_lines    = []                  # non-JSON lines (panic, build errors)

with open(json_file) as f:
    for raw in f:
        raw = raw.rstrip("\n")
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            raw_lines.append(raw)
            continue

        action  = obj.get("Action", "")
        test    = obj.get("Test", "")
        output  = obj.get("Output", "")
        elapsed = obj.get("Elapsed", 0.0)

        if not test:
            # Package-level output (compile errors, panics)
            if output.strip():
                raw_lines.append(output.rstrip("\n"))
            continue

        if action == "output":
            test_output[test].append(output.rstrip("\n"))
        elif action in ("pass", "fail", "skip"):
            test_result[test]  = action
            test_elapsed[test] = elapsed

# ── Aggregate by (section, group) ─────────────────────────────────────────────
# Use OrderedDict to preserve group order as defined in GROUPS.
group_key_order = []
seen_group_keys = set()
for section, group, _, _p in GROUPS:
    gk = (section, group)
    if gk not in seen_group_keys:
        group_key_order.append(gk)
        seen_group_keys.add(gk)

group_tests   = defaultdict(list)   # (section, group) -> [test_name]
group_passed  = defaultdict(int)
group_failed  = defaultdict(int)
group_skipped = defaultdict(int)
ungrouped     = []

for test, result in test_result.items():
    gk = find_group(test)
    group_tests[gk].append(test)
    if result == "pass":
        group_passed[gk]  += 1
    elif result == "fail":
        group_failed[gk]  += 1
    elif result == "skip":
        group_skipped[gk] += 1
    if gk == ("Ungrouped", test):
        ungrouped.append(test)

# Add any ungrouped tests to the order
for test in ungrouped:
    gk = ("Ungrouped", test)
    if gk not in seen_group_keys:
        group_key_order.append(gk)
        seen_group_keys.add(gk)

total_pass    = sum(group_passed.values())
total_fail    = sum(group_failed.values())
total_skip    = sum(group_skipped.values())
total_tests   = total_pass + total_fail + total_skip
groups_failed = sum(1 for gk in group_key_order if group_failed[gk] > 0)
groups_passed = len(group_key_order) - groups_failed

import datetime
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ── Write report ──────────────────────────────────────────────────────────────
with open(report_file, "w") as r:
    def w(line=""):
        r.write(line + "\n")

    w("# Hegel Property-Based Test Report")
    w()
    w(f"**Generated:** {now}  ")
    w(f"**Go binary:** `{go_bin}`  ")
    w(f"**Build tag:** `{tags}`  ")
    w(f"**Trials per property:** {count}  ")
    w()

    # Priority legend
    w("## Priority Legend")
    w()
    w("| Badge | Meaning |")
    w("|-------|---------|")
    w("| 🔴 HIGH | Critical cluster safety invariant — failure can cause data unavailability or incorrect failover |")
    w("| 🟡 MEDIUM | Important management feature — failure degrades behaviour but cluster remains operational |")
    w("| 🟢 LOW | Secondary feature or edge case — minimal operational impact |")
    w()

    # Summary
    w("## Summary")
    w()
    # Count failures by priority
    high_fail = sum(group_failed[gk] for gk in group_key_order if group_priority(gk) == "HIGH")
    med_fail  = sum(group_failed[gk] for gk in group_key_order if group_priority(gk) == "MEDIUM")
    low_fail  = sum(group_failed[gk] for gk in group_key_order if group_priority(gk) == "LOW")

    w("| Metric | Value |")
    w("|--------|------:|")
    w(f"| Groups total | {len(group_key_order)} |")
    w(f"| Groups all-pass | {groups_passed} |")
    w(f"| Groups with failures | {groups_failed} |")
    w(f"| Tests passed | {total_pass} |")
    w(f"| Tests failed | {total_fail} |")
    if total_skip:
        w(f"| Tests skipped | {total_skip} |")
    w(f"| 🔴 HIGH failures | {high_fail} |")
    w(f"| 🟡 MEDIUM failures | {med_fail} |")
    w(f"| 🟢 LOW failures | {low_fail} |")
    w()

    if total_fail == 0:
        w("> ✅ **All properties hold.** No counterexamples found in "
          f"{total_pass} test{'s' if total_pass != 1 else ''} × {count} trials each.")
    else:
        word = "property" if total_fail == 1 else "properties"
        crit = f" **{high_fail} are 🔴 HIGH priority.**" if high_fail else ""
        w(f"> ⚠️ **{total_fail} {word} falsified** across {groups_failed} group"
          f"{'s' if groups_failed != 1 else ''}.{crit} "
          "Counterexamples and remediation steps are in the **Failures** section below.")
    w()

    # Results by group
    w("## Results by Group")
    w()
    current_section = None
    for gk in group_key_order:
        section, group = gk
        if section != current_section:
            w(f"### {section}")
            w()
            current_section = section
        p = group_passed[gk]
        f = group_failed[gk]
        s = group_skipped[gk]
        total = p + f + s
        pri   = PRIORITY_BADGE.get(group_priority(gk), "")
        parts = [f"{p} passed"]
        if f: parts.append(f"**{f} FAILED**")
        if s: parts.append(f"{s} skipped")
        icon = "✅" if f == 0 else "❌"
        w(f"- {icon} {pri} **`{group}`** — {' / '.join(parts)} ({total} total)")
    w()

    # Failure details — sorted so HIGH failures appear first
    if total_fail > 0:
        w("---")
        w()
        w("## Failure Details")
        w()
        w("Failures are ordered by priority (🔴 HIGH first).")
        w()
        w("Each block shows:")
        w("1. The **priority badge** so you know how urgently to fix it.")
        w("2. The **shrunk counterexample** Hegel found — the minimal input that triggers the bug.")
        w("3. **How to reproduce** the failure in isolation.")
        w()

        failed_groups = [gk for gk in group_key_order if group_failed[gk] > 0]
        failed_groups.sort(key=lambda gk: PRIORITY_ORDER.get(group_priority(gk), 99))

        for gk in failed_groups:
            section, group = gk
            pri = PRIORITY_BADGE.get(group_priority(gk), "")
            w(f"### ❌ {pri} — `{group}` ({section})")
            w()

            for test in sorted(group_tests[gk]):
                if test_result.get(test) != "fail":
                    continue
                elapsed_s = test_elapsed.get(test, 0)
                w(f"#### `{test}`")
                w(f"*Priority: {pri} — Elapsed: {elapsed_s:.2f}s*")
                w()

                lines = test_output.get(test, [])
                if lines:
                    w("```")
                    for l in lines:
                        w(l)
                    w("```")
                else:
                    w("*(no captured output)*")
                w()

                w("**Remediation:**")
                w()
                w(f"- Reproduce in isolation:")
                w(f"  ```bash")
                w(f"  {go_bin} test -tags {tags} -v -count=1 -run '^{test}$' {pkg}")
                w(f"  ```")
                w(f"- Increase trials for a deeper counterexample search:")
                w(f"  ```bash")
                w(f"  {go_bin} test -tags {tags} -v -count=10000 -run '^{test}$' {pkg}")
                w(f"  ```")
                w(f"- Add `ht.Log(\"label\", value)` inside the test body to print")
                w(f"  intermediate values on each trial.")
                w()

    # Package-level errors (panics, build output that slipped through)
    if raw_lines:
        w("---")
        w()
        w("## Package-Level Output")
        w()
        w("Output not attached to any individual test (panics, race detector, etc.):")
        w()
        w("```")
        for l in raw_lines:
            w(l)
        w("```")
        w()

    # How to re-run
    w("---")
    w()
    w("## How to Re-run")
    w()
    w("```bash")
    w("# Full report")
    w(f"./scripts/hegel_report.sh {go_bin}")
    w()
    w("# More trials (find rarer counterexamples)")
    w(f"HEGEL_COUNT=1000 ./scripts/hegel_report.sh {go_bin}")
    w()
    w("# One group only")
    w(f"{go_bin} test -tags {tags} -v -count={count} -run 'TestProp_checkWsrepDesync' {pkg}")
    w()
    w("# One test only")
    w(f"{go_bin} test -tags {tags} -v -count=10000 -run '^TestProp_checkBackOffline_RejectQueriesBlocksReturn$' {pkg}")
    w("```")

print(f"Report written to {report_file}", file=sys.stderr)
sys.exit(0 if total_fail == 0 else 1)
PYEOF
PYTHON_EXIT=$?

# ─── terminal summary ─────────────────────────────────────────────────────────
log "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
log "  Report written → $REPORT"
log "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
log ""

exit $PYTHON_EXIT
