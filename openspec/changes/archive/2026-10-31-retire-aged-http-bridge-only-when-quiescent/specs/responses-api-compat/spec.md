## ADDED Requirements

### Requirement: HTTP bridge idle retirement waits for quiescence

Idle or upstream-socket-age retirement MUST NOT cancel a session while it has
pending requests or in-flight upstream-event processing, including terminal
responses claimed out of pending ownership but not yet published downstream.
Once that work finishes, an aged upstream socket MUST remain eligible for
retirement on the next quiescent sweep even before the ordinary idle TTL.
Closed-session sweeping and LRU capacity eviction MUST also defer retirement
while upstream-event processing remains in flight, even when admission is closed.

#### Scenario: An aged socket has a claimed completion awaiting publication

- **GIVEN** an HTTP bridge upstream socket has reached its retirement age
- **AND** its reader has removed a completed request from pending ownership
- **AND** completion bookkeeping is paused before downstream publication
- **WHEN** an idle sweep examines the session
- **THEN** the session and reader remain live
- **AND** the selected terminal response and end marker reach the downstream queue
- **AND** a later sweep can retire the socket after event processing finishes

#### Scenario: Duplicate-tool-call suppression closes admission before failure delivery

- **GIVEN** a reader has claimed a terminal response and closed session admission
- **AND** duplicate-tool-call suppression is awaiting upstream close before publishing a replacement failure
- **WHEN** a sweep sees the closed session
- **THEN** it leaves the reader alive until the replacement failure and end marker reach the downstream queue
- **AND** a later quiescent sweep can retire the closed session

#### Scenario: Capacity eviction examines a claimed terminal event

- **GIVEN** the least-recently-used session has no pending requests but has in-flight terminal processing
- **WHEN** capacity eviction scans for a session to close
- **THEN** it does not select that session before terminal delivery finishes
