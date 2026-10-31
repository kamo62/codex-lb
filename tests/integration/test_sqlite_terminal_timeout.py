from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import asynccontextmanager, closing
from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.scheduling import leader_election
from app.db.models import Base, HttpBridgeOperationRecord, HttpBridgeSessionRecord
from app.db.session import _configure_sqlite_engine, _sqlite_file_async_engine_kwargs, close_session
from app.modules.proxy.durable_bridge_coordinator import DurableBridgeSessionCoordinator
from app.modules.proxy.http_bridge_event_batcher import (
    HttpBridgeOperationEventBatcher,
    TerminalOperationEventAppendResult,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("spool_format", ["rows_v1", "chunks_v2"])
async def test_terminal_timeout_releases_writer_for_bridge_and_leader(
    tmp_path: Path, monkeypatch, spool_format
) -> None:
    """An append outlives its wait bound, commits, and releases SQLite writes."""
    path = tmp_path / "bridge.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", **_sqlite_file_async_engine_kwargs())
    _configure_sqlite_engine(engine.sync_engine, enable_wal=True)
    entered = threading.Event()
    release = threading.Event()
    retained_errors: list[BaseException] = []
    returning_inserts: list[str] = []

    def pause_write() -> int:
        """Keep the transcript INSERT executing until its persistence deadline expires."""
        entered.set()
        assert release.wait(5)
        return 1

    @event.listens_for(engine.sync_engine, "connect")
    def register_pause(connection, _record) -> None:
        """Register the trigger's blocking function on each SQLite connection."""
        connection.create_function("pause_write", 0, pause_write)

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def record_returning(_connection, _cursor, statement, _parameters, _context, _executemany) -> None:
        """Verify the regression exercises the driver's unfinished RETURNING path."""
        if statement.startswith("INSERT INTO http_bridge_operation_event") and "RETURNING" in statement:
            returning_inserts.append(statement)

    class RetainingSession(AsyncSession):
        async def commit(self) -> None:
            """Retain cancellation frames independently of the batcher's task ownership."""
            try:
                await super().commit()
            except asyncio.CancelledError as exc:
                retained_errors.append(exc)
                raise

    sessions = async_sessionmaker(engine, class_=RetainingSession, expire_on_commit=False)
    coordinator = DurableBridgeSessionCoordinator(sessions)
    batcher = HttpBridgeOperationEventBatcher(
        coordinator, max_bytes=4096, spool_format=spool_format, terminal_append_timeout_seconds=0.2
    )

    @asynccontextmanager
    async def background_session():
        """Run leader writes against the same file with independently owned sessions."""
        session = sessions()
        try:
            yield session
        finally:
            await close_session(session)

    monkeypatch.setattr(leader_election, "get_background_session", background_session)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with sessions() as session:
        session.add(
            HttpBridgeSessionRecord(
                id="session",
                session_key_kind="thread_header",
                session_key_value="thread",
                session_key_hash="hash",
                api_key_scope="__anonymous__",
                owner_instance_id="instance",
                owner_epoch=1,
            )
        )
        await session.commit()
        session.add(
            HttpBridgeOperationRecord(
                operation_id="operation",
                session_id="session",
                request_fingerprint="fingerprint",
                state="completed",
                spool_format=spool_format,
            )
        )
        await session.commit()
    table = "http_bridge_operation_events" if spool_format == "rows_v1" else "http_bridge_operation_event_chunks"
    async with engine.begin() as connection:
        await connection.execute(text(f"CREATE TRIGGER stall BEFORE INSERT ON {table} BEGIN SELECT pause_write(); END"))
    append = asyncio.create_task(
        batcher.append_terminal_event(
            operation_id="operation",
            session_id="session",
            instance_id="instance",
            owner_epoch=1,
            event_text='data: {"type":"response.completed"}\n\n',
            max_bytes=4096,
            state="completed",
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        retained_tasks = tuple(batcher._terminal_append_tasks)
        result = await asyncio.wait_for(append, 2)
        assert not result.persisted
        assert result.settlement_required
        assert len(retained_tasks) == 1
        assert not retained_tasks[0].done()
        assert not retained_tasks[0].cancelled()
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*retained_tasks, return_exceptions=True), 3)
        assert results == [TerminalOperationEventAppendResult(persisted=True, settlement_required=False)]
        assert not retained_tasks[0].cancelled()
        assert retained_errors == []
        assert returning_inserts
        # Fail fast here if the writer lock is stranded, rather than waiting
        # through production's 30-second busy timeout on the next bridge write.
        with closing(sqlite3.connect(path, timeout=0.2)) as independent, independent:
            independent.execute("DROP TRIGGER stall")
            assert independent.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)
        assert await coordinator.settle_terminal_append_failure(
            operation_id="operation",
            session_id="session",
            instance_id="instance",
            owner_epoch=1,
            state="completed",
            expected_response_id=None,
        )
        operation = await coordinator.get_operation(operation_id="operation")
        assert operation is not None
        assert operation.state == "completed"
        assert not operation.event_spool_complete
        # Settlement fences the late-committed attempt against another append.
        append_method = (
            coordinator.append_terminal_operation_event
            if spool_format == "rows_v1"
            else coordinator.append_terminal_operation_chunk
        )
        assert not await append_method(
            operation_id="operation",
            session_id="session",
            instance_id="instance",
            owner_epoch=1,
            event_text="late",
            max_bytes=4096,
            state="completed",
        )
        leader = leader_election.LeaderElection("after-timeout")
        assert await leader.try_acquire()
        await leader.release()
    finally:
        release.set()
        await asyncio.gather(append, return_exceptions=True)
        await batcher.close()
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("spool_format", ["rows_v1", "chunks_v2"])
async def test_retained_terminal_append_blocks_retry_and_reset(tmp_path: Path, monkeypatch, spool_format: str) -> None:
    """A retained old failure cannot write into a reset retry on real SQLite."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'retry.db'}", **_sqlite_file_async_engine_kwargs())
    _configure_sqlite_engine(engine.sync_engine, enable_wal=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    coordinator = DurableBridgeSessionCoordinator(sessions)
    batcher = HttpBridgeOperationEventBatcher(
        coordinator, max_bytes=4096, spool_format=spool_format, terminal_append_timeout_seconds=0.01
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    method_name = "append_terminal_operation_event" if spool_format == "rows_v1" else "append_terminal_operation_chunk"
    append_method = getattr(coordinator, method_name)

    async def pause_before_durable_write(**kwargs):
        entered.set()
        await release.wait()
        return await append_method(**kwargs)

    monkeypatch.setattr(coordinator, method_name, pause_before_durable_write)
    owner = dict(session_id="session", instance_id="instance", owner_epoch=1)
    identity = dict(
        operation_id="operation",
        **owner,
        request_fingerprint="fingerprint",
        account_id=None,
        model=None,
        parent_response_id=None,
    )
    old_terminal = 'data: {"type":"response.failed","response":{"id":"resp-old"}}\n\n'
    new_created = 'data: {"type":"response.created","response":{"id":"resp-new"}}\n\n'
    new_terminal = 'data: {"type":"response.completed","response":{"id":"resp-new"}}\n\n'
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with sessions() as session:
            session.add(
                HttpBridgeSessionRecord(
                    id="session",
                    session_key_kind="thread_header",
                    session_key_value="thread",
                    session_key_hash="hash",
                    api_key_scope="__anonymous__",
                    owner_instance_id="instance",
                    owner_epoch=1,
                )
            )
            await session.commit()
        assert await coordinator.record_operation(**identity) is not None
        assert await coordinator.update_operation(
            operation_id="operation", **owner, state="acknowledged", response_id="resp-old"
        )
        result = await batcher.append_terminal_event(
            operation_id="operation",
            **owner,
            event_text=old_terminal,
            max_bytes=4096,
            state="failed",
            response_id="resp-old",
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert not result.persisted and result.settlement_required
        retained = tuple(batcher._terminal_append_tasks)
        assert len(retained) == 1 and not retained[0].done()
        assert await coordinator.settle_terminal_append_failure(
            operation_id="operation", **owner, state="failed", expected_response_id="resp-old", response_id="resp-old"
        )

        # A different derived ID still resolves the same fingerprint. The fence
        # must apply to the resolved operation, not just the incoming ID.
        retry = await coordinator.record_operation(**{**identity, "operation_id": "retry-derived-id"})
        reset = await coordinator.reset_operation_event_spool(operation_id="operation", **owner)
        assert await coordinator.update_operation(operation_id="operation", **owner, state="unknown")
        recovery_claimed = await coordinator.claim_unknown_operation_for_recovery(operation_id="operation", **owner)
        assert await coordinator.update_operation(
            operation_id="operation", **owner, state="failed", response_id="resp-old"
        )
        if retry is not None and retry.rebound:
            await batcher.enqueue(operation_id="operation", **owner, event_text=new_created)
            await batcher.flush_pending_operation(operation_id="operation")
        release.set()
        late_results = await asyncio.wait_for(asyncio.gather(*retained), 2)
        operation = await coordinator.get_operation(operation_id="operation")
        assert retry is not None and not retry.rebound, (retry, operation)
        assert retry.state == "failed" and retry.response_id == "resp-old"
        assert not reset
        assert not recovery_claimed
        assert late_results == [TerminalOperationEventAppendResult(persisted=False, settlement_required=True)]

        # Once the old task has finished, admission and a fresh transcript work
        # normally. Neither spool format may contain the old response.failed.
        retry = await coordinator.record_operation(**identity)
        assert retry is not None and retry.rebound
        assert retry.state == "submitted" and retry.response_id is None
        assert await coordinator.reset_operation_event_spool(operation_id="operation", **owner)
        await batcher.enqueue(operation_id="operation", **owner, event_text=new_created)
        batcher._terminal_append_timeout_seconds = 2
        result = await batcher.append_terminal_event(
            operation_id="operation",
            **owner,
            event_text=new_terminal,
            max_bytes=4096,
            state="completed",
            response_id="resp-new",
        )
        assert result.persisted
        await asyncio.gather(*batcher._terminal_finalize_tasks)
        operation = await coordinator.get_operation(operation_id="operation")
        assert operation is not None and operation.state == "completed" and operation.response_id == "resp-new"
        assert operation.event_spool_complete
        assert await coordinator.get_operation_events(operation_id="operation") == [new_created, new_terminal]
    finally:
        release.set()
        await batcher.close()
        await engine.dispose()
