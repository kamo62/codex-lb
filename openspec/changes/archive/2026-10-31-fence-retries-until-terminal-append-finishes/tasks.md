## 1. Implementation

- [x] 1.1 Retain an operation-level retry/reset fence for the full lifetime of terminal append tasks.
- [x] 1.2 Reject stale in-memory terminal attempts before draining or reading operation contexts.
- [x] 1.3 Reproduce late writes with real SQLite for rows_v1 and chunks_v2, and cover stale context replacement.

## 2. Verification

- [x] 2.1 Prove regressions fail without the fixes and pass with them.
- [x] 2.2 Run Python 3.14 Ruff, architecture, and complete unit/integration suites.
- [x] 2.3 Attempt strict OpenSpec validation (CLI unavailable; gracefully skipped), sync once, and archive the verified change.
