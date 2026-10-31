## Retained append ownership

Use the permitted blocking strategy rather than adding an attempt-generation
schema migration. A process-local reference count follows each retained
terminal task through its pending-event drain, durable write, and cleanup.
The repository refuses rebind/reset/recovery of the resolved operation while
that count is nonzero. This includes fingerprint lookups whose incoming
operation ID differs from the recorded ID. Existing session and owner-epoch
predicates still fence writes from other processes after ownership transfer.

The counter is independent of the batcher's in-memory context: timing out may
drop optional transcript state, but it does not release the retry fence or
cancel the database statement. A done callback releases the fence only after
the retained task finishes. Duplicate admission then resumes normally.

For example, an old `resp-old` failure append pauses before durable persistence,
times out, and is settled as failed. An identical request must not reset that
operation to submitted while the old append is retained. After it finishes,
the retry may start a fresh transcript with `resp-new` only.

Stale task tokens are checked under the spooler's lock before taking a batch
and before reading an operation context. Thus a task paused on the flush lock
cannot drain replacement events or stamp its old terminal outcome with the
replacement session/owner context. Tests cover both spool formats and retain
the SQLite unfinished-RETURNING regression proving no mid-statement timeout
cancellation or stranded writer lock.

Final Python 3.14 verification: Ruff and architecture checks passed; the full
unit rerun had 10,523 passed, 97 skipped, and one Linux-only filesystem-policy
failure also reproduced on upstream/main in `/tmp/lb-base`. The full integration
rerun had 3,398 passed and 405 skipped after building missing dashboard assets.
The focused event-batcher, SQLite timeout, and JSON parsing run had 75 passed.
A temporary mutation probe disabling both fixes made all ten targeted race
cases fail (six retirement, two stale-context, two SQLite retry cases); all ten
passed with the fixes. Strict OpenSpec validation was gracefully skipped because
the CLI is unavailable. Requirements were manually synced once and archived
after both complete suites finished.
