## ADDED Requirements

### Requirement: Retained terminal appends fence operation retries

An HTTP bridge terminal append that outlives its persistence wait bound MUST
remain owned until it finishes without mid-statement cancellation. While that
append or its pending-event drain remains active, the same operation MUST NOT
be rebound for retry, have its transcript reset, or be claimed for recovery.
After the retained task finishes, ordinary retry admission MUST resume.
A stale in-memory terminal attempt MUST NOT drain or read a replacement
attempt's pending events or operation context.

#### Scenario: A timed-out failure append resumes after retry admission

- **GIVEN** a failed operation has a retained terminal append paused before its durable write
- **AND** fallback settlement has recorded the published failure
- **WHEN** an identical request attempts retry or transcript reset
- **THEN** admission and reset are refused until the retained append finishes
- **AND** a later admitted retry contains only its own response events and outcome

#### Scenario: A stale terminal task is waiting to drain

- **GIVEN** a terminal task has timed out while waiting to drain pending events
- **AND** its in-memory attempt token has been cleared or replaced
- **WHEN** the task resumes
- **THEN** it does not drain replacement events or append its old terminal outcome using the replacement context
