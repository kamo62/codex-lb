## Why

The upstream socket age sweep can see an empty pending queue after the reader
claims a completion but before it publishes that completion. Retiring the
socket then cancels the reader and loses the selected terminal response.

## What Changes

- Require fully quiescent request and upstream-event processing before idle or
  age-based retirement, including claimed-but-undelivered terminal events.
- Preserve retirement of aged sockets on a later quiescent sweep.
- Apply the same in-flight protection to the closed-session sweep shortcut and
  LRU capacity eviction, including duplicate-tool-call replacement failures.
- Regress the completion-publication race and align SQLite timeout tests with
  the existing detached terminal-append contract from upstream PR #2555.

## Impact

- Affects HTTP bridge sweep/capacity retirement and its regression coverage.
- No settings, API, schema, or routing changes. Refs Soju06/codex-lb#2658.
