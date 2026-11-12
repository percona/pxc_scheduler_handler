# Hegel Property-Based Test Report

**Generated:** 2026-11-03 18:25:19  
**Go binary:** `go`  
**Build tag:** `hegel`  
**Trials per property:** 1000  

## Priority Legend

| Badge | Meaning |
|-------|---------|
| 🔴 HIGH | Critical cluster safety invariant — failure can cause data unavailability or incorrect failover |
| 🟡 MEDIUM | Important management feature — failure degrades behaviour but cluster remains operational |
| 🟢 LOW | Secondary feature or edge case — minimal operational impact |

## Summary

| Metric | Value |
|--------|------:|
| Groups total | 76 |
| Groups all-pass | 76 |
| Groups with failures | 0 |
| Tests passed | 126 |
| Tests failed | 0 |
| 🔴 HIGH failures | 0 |
| 🟡 MEDIUM failures | 0 |
| 🟢 LOW failures | 0 |

> ✅ **All properties hold.** No counterexamples found in 126 tests × 1000 trials each.

## Results by Group

### Cluster Node State

- ✅ 🔴 HIGH **`checkWsrepDesync`** — 7 passed (7 total)
- ✅ 🔴 HIGH **`checkAnyNotReadyStatus`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`checkNotPrimary`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`checkRejectQueries`** — 3 passed (3 total)
- ✅ 🔴 HIGH **`checkDonorReject`** — 3 passed (3 total)
- ✅ 🔴 HIGH **`checkPxcMaint`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`checkReadOnly`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`checkBackOffline`** — 4 passed (4 total)
- ✅ 🔴 HIGH **`checkBackDesyncButUnderReplicaLag`** — 3 passed (3 total)
- ✅ 🔴 HIGH **`checkUpSaveRetry`** — 4 passed (4 total)
- ✅ 🟡 MEDIUM **`checkBackNew`** — 2 passed (2 total)
- ✅ 🟡 MEDIUM **`checkBackPrimary`** — 2 passed (2 total)
- ✅ 🟡 MEDIUM **`checkActiveFailover`** — 2 passed (2 total)
### Cluster Management

- ✅ 🔴 HIGH **`cleanWriters`** — 2 passed (2 total)
- ✅ 🟡 MEDIUM **`checkFailoverIfFound`** — 3 passed (3 total)
- ✅ 🟡 MEDIUM **`processWriterIsAlsoReader`** — 3 passed (3 total)
- ✅ 🟡 MEDIUM **`identifyLowerNodeToRemove`** — 2 passed (2 total)
- ✅ 🟡 MEDIUM **`processUpAndDownReaders`** — 3 passed (3 total)
### Cluster Operations

- ✅ 🟡 MEDIUM **`calculateHgOnlineSize`** — 3 passed (3 total)
- ✅ 🟡 MEDIUM **`alignNodeValues`** — 2 passed (2 total)
- ✅ 🟢 LOW **`compareNodeDefault / resetNodeDefault`** — 2 passed (2 total)
- ✅ 🟢 LOW **`ReturnTextFromCode`** — 1 passed (1 total)
- ✅ 🟢 LOW **`ReturnActionCategory`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`getRetry`** — 4 passed (4 total)
- ✅ 🟡 MEDIUM **`checkMissingForNodes`** — 2 passed (2 total)
### ProxySQL SQL Builders

- ✅ 🔴 HIGH **`MoveNodeUpFromOfflineSoft`** — 1 passed (1 total)
- ✅ 🔴 HIGH **`MoveNodeDownToOfflineSoft`** — 1 passed (1 total)
- ✅ 🔴 HIGH **`MoveNodeDownToHGCange`** — 1 passed (1 total)
- ✅ 🔴 HIGH **`MoveNodeUpFromHGCange`** — 1 passed (1 total)
- ✅ 🔴 HIGH **`InsertRead`** — 3 passed (3 total)
- ✅ 🔴 HIGH **`InsertWrite`** — 2 passed (2 total)
- ✅ 🔴 HIGH **`SaveRetry`** — 3 passed (3 total)
- ✅ 🟡 MEDIUM **`DeleteDataNode`** — 1 passed (1 total)
- ✅ 🟢 LOW **`ResetNodeDefaults`** — 1 passed (1 total)
### File Locker

- ✅ 🔴 HIGH **`EvaluateFileLockForRemoval`** — 4 passed (4 total)
### Ungrouped

- ✅ 🟢 LOW **`TestProp_GetActionList_EmptyClusterProducesNoActions`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_GetActionList_EmptyClusterIsIdempotent`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_SanityCheck_Idempotent`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_ClusterLock_SecondSchedulerDoesNotWinActiveLock`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_ClusterLock_ExpiredLockAllowsTakeover`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_ClusterLock_TwoClustersDoNotInterfere`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_ClusterLock_ArbitraryNodeCommentNeverCausesLockPanic`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_SetLock_ShorterLockLeavesNoTail`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_SQLBuilders_ArbitraryCommentNeverEmpty`** — 1 passed (1 total)
- ✅ 🟢 LOW **`TestProp_SQLBuilders_SingleQuoteInCommentIsNotEscaped`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#0`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#1`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#2`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#3`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#4`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#5`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#6`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck/seed#7`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzConfigSanityCheck`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#0`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#1`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#2`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#3`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#4`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#5`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#6`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#7`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#8`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#9`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists/seed#10`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzCheckLockFileExists`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#0`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#1`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#2`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#3`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#4`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#5`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#6`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#7`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL/seed#8`** — 1 passed (1 total)
- ✅ 🟢 LOW **`FuzzSaveRetrySQL`** — 1 passed (1 total)

---

## Package-Level Output

Output not attached to any individual test (panics, race detector, etc.):

```
PASS
ok  	pxc_scheduler_handler/internal/test/hegel	183.047s
```

---

## How to Re-run

```bash
# Full report
./scripts/hegel_report.sh go

# More trials (find rarer counterexamples)
HEGEL_COUNT=1000 ./scripts/hegel_report.sh go

# One group only
go test -tags hegel -v -count=1000 -run 'TestProp_checkWsrepDesync' ./internal/test/hegel/...

# One test only
go test -tags hegel -v -count=10000 -run '^TestProp_checkBackOffline_RejectQueriesBlocksReturn$' ./internal/test/hegel/...
```
