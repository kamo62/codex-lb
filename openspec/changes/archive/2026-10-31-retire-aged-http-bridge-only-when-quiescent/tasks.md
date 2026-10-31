## 1. Implementation

- [x] 1.1 Keep idle/age retirement from cancelling in-flight upstream-event processing or pending requests.
- [x] 1.2 Reproduce a claimed completion paused before publication and verify delivery before retirement.
- [x] 1.3 Assert SQLite terminal appends remain running past the bound and complete without cancellation.
- [x] 1.4 Defer closed-session pruning and LRU capacity eviction during claimed terminal delivery; update native session doubles.

## 2. Verification

- [x] 2.1 Verify the race regression fails without the fix and passes with it.
- [x] 2.2 Run Ruff, the proxy architecture check, and the complete unit and integration suites with Python 3.14.
- [x] 2.3 Attempt strict OpenSpec validation (CLI unavailable; gracefully skipped), sync the requirement once, and archive the verified change.
