## Ownership gap

The upstream reader removes a terminal request from pending ownership before
asynchronous continuity bookkeeping and downstream publication finish. An empty
pending queue therefore does not mean the session is quiescent. Count the whole
upstream event-processing scope, including exception/cancellation cleanup.

For example, duplicate-tool-call suppression sets `session.closed` and awaits
upstream close before rewriting a completion to `response.failed`. A sweep must
not treat `closed` as permission to cancel that reader. LRU eviction has the
same ownership gap and must skip it too. Explicit error/shutdown resource close
is unchanged; this change governs sweep/capacity retirement eligibility.

## Verification

The regression pauses a claimed terminal event, invokes real retirement and
resource closure if selected, then verifies the terminal block and end marker.
It covers idle/age, closed-session, and LRU retirement, and both expired and
unexpired idle TTLs. A temporary mutation probe disabling the guards loses the
terminal block in all six variants; no probe is installed in the repository.

Final verification used Python 3.14: Ruff and the architecture check passed;
the HTTP bridge mixin remains at its 2,436-line budget. The final complete unit
run had 10,523 passed, 97 skipped, and one Linux-only filesystem-policy failure
also reproduced on upstream/main in `/tmp/lb-base`. The complete integration
rerun had 3,398 passed and 405 skipped after building missing dashboard assets.
Earlier timing failures disappeared on the complete reruns. Native wire tests
remain skipped because `CODEX_LB_NATIVE_EGRESS_TEST_BINARY` is not configured.
Strict OpenSpec validation was attempted but gracefully skipped because no
OpenSpec CLI is installed. Requirements were manually synced exactly once and
the change archived after both full suites finished.
