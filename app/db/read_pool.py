from __future__ import annotations

import asyncio
import concurrent.futures
import os
import queue
import sqlite3
import threading
from time import monotonic
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar, cast

from app.db.connection import apply_pragmas


T = TypeVar("T")


class SQLiteReadPoolClosedError(RuntimeError):
    """Raised when work is submitted before start or after shutdown begins."""


class SQLiteReadPoolPausedError(RuntimeError):
    """Raised while maintenance has exclusive ownership of the database."""


class SQLiteReadPoolOverloadedError(RuntimeError):
    """Raised when both owned capacity and the bounded waiter queue are full."""


class SQLiteReadPoolTimeoutError(TimeoutError):
    """A read exceeded its actor execution deadline."""


class SQLiteReadPoolForkedError(RuntimeError):
    """A started pool was used from a different process."""


class _DatabaseReplacedError(sqlite3.OperationalError):
    """The database pathname changed while a read was executing."""


@dataclass(slots=True)
class _ConnectionSlot:
    connection: sqlite3.Connection
    identity: tuple[int, int]


@dataclass(slots=True)
class _ReadRequest(Generic[T]):
    operation: Callable[[sqlite3.Connection], T]
    result: concurrent.futures.Future[T]
    deadline: float
    cancelled: threading.Event


@dataclass(slots=True)
class _AdmissionWaiter:
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]
    state: str = "waiting"


@dataclass(slots=True)
class _Worker:
    index: int
    queue: queue.Queue[object]
    thread: threading.Thread | None = None
    connection_open: bool = False
    stopped: bool = False


_PAUSE = object()
_STOP = object()


class SQLiteReadPool:
    """Bounded, multi-loop SQLite read actor pool.

    Each fixed worker owns its SQLite connection from creation through close.
    Admission is centrally bounded: cancelling an awaiter after admission does
    not release capacity, because the worker still owns and finishes the read.
    """

    DEFAULT_QUEUE_CAPACITY = 256
    DEFAULT_MAX_WAITERS = 1024

    def __init__(
        self,
        database_path: Path,
        *,
        max_connections: int,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        max_waiters: int = DEFAULT_MAX_WAITERS,
        timeout_seconds: float = 5.0,
    ) -> None:
        for name, value in (
            ("max_connections", max_connections),
            ("queue_capacity", queue_capacity),
            ("max_waiters", max_waiters),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 1:
                raise ValueError(f"{name} must be at least one")
        if queue_capacity < max_connections:
            raise ValueError("queue_capacity must be at least max_connections")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._database_path = Path(database_path).absolute()
        self._max_connections = max_connections
        self._queue_capacity = queue_capacity
        self._max_waiters = max_waiters
        self._timeout_seconds = float(timeout_seconds)
        self._workers = [
            _Worker(index=index, queue=queue.Queue())
            for index in range(max_connections)
        ]
        self._state_lock = threading.Lock()
        self._state = "new"
        self._owner_pid: int | None = None
        self._outstanding = 0
        self._next_worker = 0
        self._admission_waiters: deque[_AdmissionWaiter] = deque()
        self._pause_controls_enqueued = False
        self._pause_ack_count = 0
        self._pause_complete: concurrent.futures.Future[None] | None = None
        self._stop_enqueued = False
        self._stopped_worker_count = 0
        self._fatal_error: BaseException | None = None
        self._closed: concurrent.futures.Future[None] = concurrent.futures.Future()

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @property
    def active_connection_count(self) -> int:
        with self._state_lock:
            return sum(worker.connection_open for worker in self._workers)

    @property
    def outstanding_count(self) -> int:
        with self._state_lock:
            return self._outstanding

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._state == "closed"

    def operational_state(self) -> dict[str, Any]:
        with self._state_lock:
            running_workers = sum(
                worker.thread is not None
                and worker.thread.is_alive()
                and not worker.stopped
                for worker in self._workers
            )
            healthy = bool(
                self._state == "open"
                and self._fatal_error is None
                and running_workers == self._max_connections
            )
            return {
                "ok": healthy,
                "state": self._state,
                "workers_running": running_workers,
                "workers_configured": self._max_connections,
                "connections_open": sum(worker.connection_open for worker in self._workers),
                "outstanding": self._outstanding,
                "waiting": sum(
                    waiter.state == "waiting"
                    for waiter in self._admission_waiters
                ),
            }

    def start(self) -> None:
        """Start fixed worker actors without opening database connections."""

        with self._state_lock:
            if self._state == "open":
                self._assert_owner_process_locked()
                return
            if self._state != "new":
                raise SQLiteReadPoolClosedError("SQLite read pool cannot be started")
            self._owner_pid = os.getpid()
            self._state = "starting"

        started: list[_Worker] = []
        try:
            for worker in self._workers:
                thread = threading.Thread(
                    target=self._worker_main,
                    args=(worker,),
                    name=f"rapid-inbox-db-read-{worker.index}",
                    daemon=True,
                )
                worker.thread = thread
                thread.start()
                started.append(worker)
        except BaseException as exc:
            for worker in started:
                worker.queue.put_nowait(_STOP)
            for worker in started:
                if worker.thread is not None:
                    worker.thread.join()
            with self._state_lock:
                self._state = "closed"
                self._fatal_error = exc
                if not self._closed.done():
                    self._closed.set_exception(exc)
            raise

        with self._state_lock:
            self._state = "open"

    async def fetch_all(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        return await self._submit(
            lambda connection: [
                dict(row)
                for row in connection.execute(query, params).fetchall()
            ]
        )

    async def fetch_one(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection) -> dict[str, Any] | None:
            row = connection.execute(query, params).fetchone()
            return None if row is None else dict(row)

        return await self._submit(operation)

    async def pause_and_drain(self) -> None:
        """Atomically stop admission, drain owned reads, and owner-close handles."""

        rejected: list[_AdmissionWaiter] = []
        with self._state_lock:
            self._assert_owner_process_locked()
            if self._state == "paused":
                return
            if self._state == "open":
                self._state = "pausing"
                self._pause_complete = concurrent.futures.Future()
                while self._admission_waiters:
                    waiter = self._admission_waiters.popleft()
                    if waiter.state != "waiting":
                        continue
                    waiter.state = "rejected"
                    rejected.append(waiter)
                self._advance_lifecycle_locked()
            elif self._state != "pausing":
                raise SQLiteReadPoolClosedError("SQLite read pool is not available")
            pause_complete = self._pause_complete

        for waiter in rejected:
            self._schedule_admission_rejection(
                waiter,
                SQLiteReadPoolPausedError("SQLite read pool is paused for maintenance"),
            )
        if pause_complete is None:  # pragma: no cover - guarded by the state machine
            raise RuntimeError("missing SQLite read pool pause future")
        await self._await_process_future_uncancellable(pause_complete)

    def resume(self) -> None:
        """Resume admission after maintenance; workers reopen lazily."""

        with self._state_lock:
            self._assert_owner_process_locked()
            if self._state == "paused":
                self._pause_controls_enqueued = False
                self._pause_ack_count = 0
                self._pause_complete = None
                self._state = "open"
                return
            if self._state in {"closing", "closed"}:
                return
            if self._state == "open":
                return
            raise RuntimeError("SQLite read pool has not finished pausing")

    async def close(self) -> None:
        """Reject new reads, drain actors, owner-close handles, and join threads."""

        rejected: list[_AdmissionWaiter] = []
        with self._state_lock:
            if self._state == "new":
                self._state = "closed"
                self._closed.set_result(None)
            else:
                self._assert_owner_process_locked()
                if self._state not in {"closing", "closed"}:
                    self._state = "closing"
                    while self._admission_waiters:
                        waiter = self._admission_waiters.popleft()
                        if waiter.state != "waiting":
                            continue
                        waiter.state = "rejected"
                        rejected.append(waiter)
                    if self._pause_complete is not None and not self._pause_complete.done():
                        self._pause_complete.set_exception(
                            SQLiteReadPoolClosedError("SQLite read pool is closing")
                        )
                    self._advance_lifecycle_locked()

        for waiter in rejected:
            self._schedule_admission_rejection(
                waiter,
                SQLiteReadPoolClosedError("SQLite read pool is closing or closed"),
            )

        close_error: BaseException | None = None
        try:
            await self._await_process_future_uncancellable(self._closed)
        except BaseException as exc:  # preserve actor failure after all joins
            close_error = exc
        while any(
            worker.thread is not None and worker.thread.is_alive()
            for worker in self._workers
        ):
            await asyncio.sleep(0.001)
        for worker in self._workers:
            if worker.thread is not None:
                worker.thread.join(timeout=0)
        if close_error is not None:
            raise close_error

    async def _submit(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        deadline = monotonic() + self._timeout_seconds
        await self._acquire_capacity(deadline)
        result: concurrent.futures.Future[T] = concurrent.futures.Future()
        request = _ReadRequest(
            operation=operation,
            result=result,
            deadline=deadline,
            cancelled=threading.Event(),
        )
        capacity_released = False
        try:
            with self._state_lock:
                # Capacity was linearized before a pause/ordinary close and is
                # therefore still owned by this request. Those states cannot
                # enqueue controls until this request releases its slot. A
                # fatal actor shutdown is different: no worker is guaranteed
                # to consume a request, so fail before placing it after STOP.
                if self._fatal_error is not None or self._state == "closed":
                    self._outstanding -= 1
                    capacity_released = True
                    self._advance_lifecycle_locked()
                    raise SQLiteReadPoolClosedError(
                        "SQLite read pool actor failed before dispatch"
                    ) from self._fatal_error
                worker = self._workers[self._next_worker]
                self._next_worker = (self._next_worker + 1) % self._max_connections
                worker.queue.put_nowait(request)
        except BaseException:
            # The fatal/closed branch above already releases its admitted
            # slot while holding the state lock. All other dispatch failures
            # still own capacity here.
            if not capacity_released:
                self._release_capacity()
            raise

        # Shielding prevents asyncio cancellation from cancelling the process-
        # owned Future. Only the worker releases the admitted capacity.
        try:
            return await asyncio.shield(asyncio.wrap_future(result))
        except asyncio.CancelledError:
            request.cancelled.set()
            raise

    async def _acquire_capacity(self, deadline: float) -> None:
        loop = asyncio.get_running_loop()
        waiter: _AdmissionWaiter | None = None
        with self._state_lock:
            self._raise_if_not_open_locked()
            self._discard_inactive_waiters_locked()
            if self._outstanding < self._queue_capacity and not self._admission_waiters:
                self._outstanding += 1
                return
            waiting_count = sum(
                item.state == "waiting"
                for item in self._admission_waiters
            )
            if waiting_count >= self._max_waiters:
                raise SQLiteReadPoolOverloadedError("SQLite read admission queue is full")
            waiter = _AdmissionWaiter(loop=loop, future=loop.create_future())
            self._admission_waiters.append(waiter)
            grants = self._grant_waiters_locked()

        self._schedule_admission_grants(grants)
        try:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(asyncio.shield(waiter.future), timeout=remaining)
            with self._state_lock:
                if waiter.state != "granted":
                    raise SQLiteReadPoolClosedError("SQLite read pool is unavailable")
                waiter.state = "consumed"
        except asyncio.TimeoutError as exc:
            grants = self._cancel_admission(waiter)
            self._schedule_admission_grants(grants)
            if not waiter.future.done():
                waiter.future.cancel()
            raise SQLiteReadPoolTimeoutError(
                "SQLite read deadline exceeded during admission"
            ) from exc
        except BaseException:
            grants = self._cancel_admission(waiter)
            self._schedule_admission_grants(grants)
            if not waiter.future.done():
                waiter.future.cancel()
            raise

    def _raise_if_not_open_locked(self) -> None:
        self._assert_owner_process_locked()
        if self._state in {"pausing", "paused"}:
            raise SQLiteReadPoolPausedError("SQLite read pool is paused for maintenance")
        if self._state != "open":
            raise SQLiteReadPoolClosedError("SQLite read pool is not started or is closing")

    def _assert_owner_process_locked(self) -> None:
        if self._owner_pid is not None and self._owner_pid != os.getpid():
            raise SQLiteReadPoolForkedError(
                "started SQLite read pool cannot be used after fork"
            )

    def _discard_inactive_waiters_locked(self) -> None:
        while self._admission_waiters and self._admission_waiters[0].state != "waiting":
            self._admission_waiters.popleft()

    def _grant_waiters_locked(self) -> list[_AdmissionWaiter]:
        grants: list[_AdmissionWaiter] = []
        if self._state != "open":
            return grants
        while self._outstanding < self._queue_capacity and self._admission_waiters:
            waiter = self._admission_waiters.popleft()
            if waiter.state != "waiting":
                continue
            waiter.state = "granted"
            self._outstanding += 1
            grants.append(waiter)
        return grants

    def _cancel_admission(self, waiter: _AdmissionWaiter) -> list[_AdmissionWaiter]:
        with self._state_lock:
            if waiter.state == "waiting":
                waiter.state = "cancelled"
            elif waiter.state == "granted":
                waiter.state = "cancelled"
                self._outstanding -= 1
            grants = self._grant_waiters_locked()
            self._advance_lifecycle_locked()
        return grants

    def _schedule_admission_grants(self, waiters: list[_AdmissionWaiter]) -> None:
        for waiter in waiters:
            try:
                waiter.loop.call_soon_threadsafe(self._deliver_admission_grant, waiter)
            except RuntimeError:
                grants = self._cancel_admission(waiter)
                self._schedule_admission_grants(grants)

    def _deliver_admission_grant(self, waiter: _AdmissionWaiter) -> None:
        grants: list[_AdmissionWaiter] = []
        with self._state_lock:
            if waiter.state != "granted":
                return
            if waiter.future.done():
                waiter.state = "cancelled"
                self._outstanding -= 1
                grants = self._grant_waiters_locked()
                self._advance_lifecycle_locked()
            else:
                waiter.future.set_result(None)
        self._schedule_admission_grants(grants)

    @staticmethod
    def _schedule_admission_rejection(
        waiter: _AdmissionWaiter,
        error: BaseException,
    ) -> None:
        try:
            waiter.loop.call_soon_threadsafe(
                SQLiteReadPool._deliver_admission_rejection,
                waiter,
                error,
            )
        except RuntimeError:
            return

    @staticmethod
    def _deliver_admission_rejection(
        waiter: _AdmissionWaiter,
        error: BaseException,
    ) -> None:
        if not waiter.future.done():
            waiter.future.set_exception(error)

    def _release_capacity(self) -> None:
        with self._state_lock:
            if self._outstanding <= 0:
                raise RuntimeError("SQLite read capacity counter underflow")
            self._outstanding -= 1
            grants = self._grant_waiters_locked()
            self._advance_lifecycle_locked()
        self._schedule_admission_grants(grants)

    def _advance_lifecycle_locked(self) -> None:
        if self._outstanding != 0:
            return
        if self._state == "pausing" and not self._pause_controls_enqueued:
            self._pause_controls_enqueued = True
            for worker in self._workers:
                worker.queue.put_nowait(_PAUSE)
        elif self._state == "closing" and not self._stop_enqueued:
            self._stop_enqueued = True
            for worker in self._workers:
                worker.queue.put_nowait(_STOP)

    def _worker_main(self, worker: _Worker) -> None:
        slot: _ConnectionSlot | None = None
        fatal_error: BaseException | None = None
        active_request: _ReadRequest[Any] | None = None
        active_capacity_owned = False
        try:
            while True:
                item = worker.queue.get()
                try:
                    if item is _STOP:
                        return
                    if item is _PAUSE:
                        slot = self._close_connection(worker, slot)
                        self._acknowledge_pause()
                        continue
                    request = cast(_ReadRequest[Any], item)
                    active_request = request
                    active_capacity_owned = True
                    slot = self._run_request(worker, slot, request)
                    self._release_capacity()
                    active_capacity_owned = False
                    active_request = None
                finally:
                    worker.queue.task_done()
        except BaseException as exc:  # pragma: no cover - catastrophic actor failure
            fatal_error = exc
            self._begin_worker_failure(worker, exc)
            if active_request is not None:
                if not active_request.result.done():
                    active_request.result.set_exception(exc)
                if active_capacity_owned:
                    self._release_capacity()
            self._fail_worker_queue(worker, exc)
        finally:
            self._close_connection(worker, slot)
            self._worker_stopped(worker, fatal_error)

    def _run_request(
        self,
        worker: _Worker,
        slot: _ConnectionSlot | None,
        request: _ReadRequest[Any],
    ) -> _ConnectionSlot | None:
        for attempt in range(2):
            if request.cancelled.is_set():
                request.result.cancel()
                return slot
            if monotonic() >= request.deadline:
                request.result.set_exception(
                    SQLiteReadPoolTimeoutError("SQLite read deadline exceeded")
                )
                return slot
            interruption_reason: str | None = None

            def check_deadline() -> int:
                nonlocal interruption_reason
                if request.cancelled.is_set():
                    interruption_reason = "cancelled"
                    return 1
                if monotonic() >= request.deadline:
                    interruption_reason = "timeout"
                    return 1
                return 0

            try:
                slot = self._ensure_connection(worker, slot)
                slot.connection.set_progress_handler(check_deadline, 1000)
                try:
                    result = request.operation(slot.connection)
                finally:
                    slot.connection.set_progress_handler(None, 0)
                if slot.connection.in_transaction:
                    slot.connection.rollback()
                if self._database_identity() != slot.identity:
                    raise _DatabaseReplacedError("database was replaced during read")
            except sqlite3.Error as exc:
                if request.cancelled.is_set() or interruption_reason == "cancelled":
                    if slot is not None and slot.connection.in_transaction:
                        try:
                            slot.connection.rollback()
                        except sqlite3.Error:
                            slot = self._close_connection(worker, slot)
                    request.result.cancel()
                    return slot
                if interruption_reason == "timeout" or monotonic() >= request.deadline:
                    if slot is not None and slot.connection.in_transaction:
                        try:
                            slot.connection.rollback()
                        except sqlite3.Error:
                            slot = self._close_connection(worker, slot)
                    request.result.set_exception(
                        SQLiteReadPoolTimeoutError("SQLite read deadline exceeded")
                    )
                    return slot
                slot = self._close_connection(worker, slot)
                if attempt == 1:
                    request.result.set_exception(exc)
                    return slot
                continue
            except BaseException as exc:
                if slot is not None and slot.connection.in_transaction:
                    try:
                        slot.connection.rollback()
                    except sqlite3.Error:
                        slot = self._close_connection(worker, slot)
                request.result.set_exception(exc)
                return slot
            request.result.set_result(result)
            return slot
        raise AssertionError("unreachable SQLite retry state")

    def _ensure_connection(
        self,
        worker: _Worker,
        slot: _ConnectionSlot | None,
    ) -> _ConnectionSlot:
        current_identity = self._database_identity()
        if slot is not None and slot.identity == current_identity:
            return slot
        slot = self._close_connection(worker, slot)
        return self._open_current_database(worker, current_identity)

    def _open_current_database(
        self,
        worker: _Worker,
        expected_identity: tuple[int, int],
    ) -> _ConnectionSlot:
        for attempt in range(2):
            connection = sqlite3.connect(
                f"{self._database_path.as_uri()}?mode=ro",
                uri=True,
                isolation_level=None,
            )
            try:
                connection.row_factory = sqlite3.Row
                apply_pragmas(connection)
                connection.execute("PRAGMA query_only = ON;")
                actual_identity = self._database_identity()
                if actual_identity == expected_identity:
                    with self._state_lock:
                        worker.connection_open = True
                    return _ConnectionSlot(connection, actual_identity)
            except BaseException:
                connection.close()
                raise
            connection.close()
            if attempt == 0:
                expected_identity = self._database_identity()
        raise _DatabaseReplacedError("database changed while opening read connection")

    def _close_connection(
        self,
        worker: _Worker,
        slot: _ConnectionSlot | None,
    ) -> None:
        if slot is None:
            return None
        try:
            if slot.connection.in_transaction:
                slot.connection.rollback()
        except sqlite3.Error:
            pass
        try:
            slot.connection.close()
        except sqlite3.Error:
            pass
        finally:
            with self._state_lock:
                worker.connection_open = False
        return None

    def _database_identity(self) -> tuple[int, int]:
        try:
            metadata = self._database_path.stat()
        except OSError as exc:
            raise sqlite3.OperationalError("database file is unavailable") from exc
        return metadata.st_dev, metadata.st_ino

    def _acknowledge_pause(self) -> None:
        with self._state_lock:
            self._pause_ack_count += 1
            if self._pause_ack_count != self._max_connections:
                return
            if self._state == "pausing":
                self._state = "paused"
                if self._pause_complete is not None and not self._pause_complete.done():
                    self._pause_complete.set_result(None)

    def _fail_worker_queue(self, worker: _Worker, error: BaseException) -> None:
        while True:
            try:
                item = worker.queue.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(item, _ReadRequest):
                    item.result.set_exception(error)
                    self._release_capacity()
            finally:
                worker.queue.task_done()

    def _begin_worker_failure(self, worker: _Worker, error: BaseException) -> None:
        rejected: list[_AdmissionWaiter] = []
        with self._state_lock:
            if self._fatal_error is None:
                self._fatal_error = error
                self._state = "closing"
                while self._admission_waiters:
                    waiter = self._admission_waiters.popleft()
                    if waiter.state != "waiting":
                        continue
                    waiter.state = "rejected"
                    rejected.append(waiter)
                if self._pause_complete is not None and not self._pause_complete.done():
                    self._pause_complete.set_exception(error)
                if not self._stop_enqueued:
                    self._stop_enqueued = True
                    for other in self._workers:
                        if other is not worker:
                            other.queue.put_nowait(_STOP)

        for waiter in rejected:
            self._schedule_admission_rejection(waiter, error)

    def _worker_stopped(
        self,
        worker: _Worker,
        fatal_error: BaseException | None,
    ) -> None:
        rejected: list[_AdmissionWaiter] = []
        with self._state_lock:
            worker.connection_open = False
            if worker.stopped:
                return
            worker.stopped = True
            self._stopped_worker_count += 1
            if self._stopped_worker_count == self._max_connections:
                self._state = "closed"
                if not self._closed.done():
                    if self._fatal_error is None:
                        self._closed.set_result(None)
                    else:
                        self._closed.set_exception(self._fatal_error)

        for waiter in rejected:
            self._schedule_admission_rejection(waiter, fatal_error or RuntimeError())

    @staticmethod
    async def _await_process_future_uncancellable(
        future: concurrent.futures.Future[T],
    ) -> T:
        wrapped = asyncio.wrap_future(future)
        while True:
            try:
                return await asyncio.shield(wrapped)
            except asyncio.CancelledError:
                if wrapped.done():
                    return wrapped.result()
                continue
