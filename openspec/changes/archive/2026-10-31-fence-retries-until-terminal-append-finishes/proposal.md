## Why

A terminal transcript append retained after its wait bound can outlive fallback
settlement. Resetting that same operation for a retry clears its terminal-phase
fence, allowing the retained append to overwrite the retry and contaminate its
transcript with the previous response's terminal event.

## What Changes

- Block retry/rebind and transcript reset while this process retains a terminal
  append for the operation, including its pending-event drain and final cleanup.
- Reject stale in-memory attempt tokens before draining or reading replacement
  operation contexts. Keep timed-out SQLite appends alive to finish safely.

## Impact

- HTTP bridge operation retry admission and both durable spool formats.
- No schema, settings, or public error-envelope changes. A blocked retry uses
  the existing operation-status-unknown response and may retry after cleanup.
