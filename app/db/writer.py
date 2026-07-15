from __future__ import annotations

import asyncio
import concurrent.futures
import queue
import sqlite3
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar, cast

from app.db.connection import apply_pragmas


T = TypeVar("T")


class DatabaseWriterClosedError(RuntimeError):
    """Raised when work is submitted after writer shutdown has started."""


class DatabaseWriterOverloadedError(RuntimeError):
    """Raised when both writer capacity and the bounded waiter queue are full."""


@dataclass(slots=True)
class _WriteRequest(Generic[T]):
    operation: Callable[[sqlite3.Connection], T]
    result: concurrent.futures.Future[T]


@dataclass(slots=True)
class _AdmissionWaiter:
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]
    state: str = "waiting"


_STOP = object()


class DatabaseWriter:
    """Serialize SQLite mutations on one dedicated, bounded writer actor.

    Admission is asynchronous, but the queue and its ownership counters are
    protected with thread primitives so one writer can safely serve callers
    from different event loops.  Once a request is admitted it belongs to the
    writer: cancelling the awaiting coroutine does not cancel the transaction
    or release its queue capacity early.
    """

    DEFAULT_QUEUE_CAPACITY = 256
    DEFAULT_MAX_WAITERS = 1024

    def __init__(
        self,
        database_path: Path,
        *,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        max_waiters: int = DEFAULT_MAX_WAITERS,
    ) -> None:
        if isinstance(queue_capacity, bool) or not isinstance(queue_capacity, int):
            raise TypeError("queue_capacity must be an integer")
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be at least 1")
        if isinstance(max_waiters, bool) or not isinstance(max_waiters, int):
            raise TypeError("max_waiters must be an integer")
        if max_waiters < 1:
            raise ValueError("max_waiters must be at least 1")

        self._database_path = database_path
        self._queue_capacity = queue_capacity
        self._max_waiters = max_waiters
        self._queue: queue.Queue[_WriteRequest[Any] | object] = queue.Queue(
            maxsize=queue_capacity
        )
        self._state_lock = threading.Lock()
        self._state = "open"
        self._outstanding = 0
        self._admission_waiters: deque[_AdmissionWaiter] = deque()
        self._stop_enqueued = False
        self._closed: concurrent.futures.Future[None] = concurrent.futures.Future()
        # Creating an application must remain fork/preload safe.  The actor
        # thread is created lazily with the first successful admission.
        self._thread: threading.Thread | None = None

    async def execute(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        return await self._submit(operation)

    async def execute_maintenance(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        # Maintenance uses the same actor and connection so VACUUM/checkpoints
        # cannot overlap or overtake ordinary mutations.
        return await self._submit(operation)

    async def close(self) -> None:
        """Reject new work, drain owned requests, and stop the writer thread."""

        rejected: list[_AdmissionWaiter] = []
        enqueue_stop = False
        complete_without_worker = False
        with self._state_lock:
            if self._state == "open":
                self._state = "closing"
                while self._admission_waiters:
                    waiter = self._admission_waiters.popleft()
                    if waiter.state != "waiting":
                        continue
                    waiter.state = "rejected"
                    rejected.append(waiter)
                if self._outstanding == 0 and not self._stop_enqueued:
                    if self._thread is None:
                        self._state = "closed"
                        complete_without_worker = True
                    else:
                        self._stop_enqueued = True
                        enqueue_stop = True

        for waiter in rejected:
            self._schedule_admission_rejection(waiter)
        if complete_without_worker:
            self._closed.set_result(None)
        if enqueue_stop:
            self._queue.put_nowait(_STOP)

        # Shield the process-owned close future: cancellation of one caller
        # must not cancel shutdown for another caller or strand the worker.
        await asyncio.shield(asyncio.wrap_future(self._closed))

    async def _submit(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")

        await self._acquire_capacity()
        result: concurrent.futures.Future[T] = concurrent.futures.Future()
        request = _WriteRequest(operation=operation, result=result)
        enqueue_stop = False
        writer_closed = False
        try:
            # Admission and queue ownership are separate steps.  Serialize the
            # latter with shutdown so a worker failure cannot transition the
            # actor to closing between the final state check and the enqueue,
            # leaving an accepted request behind a worker that has already
            # exited.
            with self._state_lock:
                if self._state != "open":
                    if self._outstanding <= 0:
                        raise RuntimeError("database writer capacity counter underflow")
                    self._outstanding -= 1
                    enqueue_stop = self._prepare_stop_locked()
                    writer_closed = True
                else:
                    self._queue.put_nowait(request)
        except BaseException:
            # This is only a defensive path: admission keeps the number of
            # running plus queued requests within the queue capacity.
            self._release_capacity()
            raise
        if enqueue_stop:
            self._queue.put_nowait(_STOP)
        if writer_closed:
            raise DatabaseWriterClosedError("database writer is closing or closed")

        # asyncio.wrap_future normally propagates cancellation into the source
        # concurrent future.  Shielding preserves transaction ownership after
        # admission; capacity is released by the writer thread only.
        return await asyncio.shield(asyncio.wrap_future(result))

    async def _acquire_capacity(self) -> None:
        loop = asyncio.get_running_loop()
        waiter: _AdmissionWaiter | None = None
        grants: list[_AdmissionWaiter] = []

        with self._state_lock:
            self._raise_if_not_open_locked()
            self._ensure_worker_started_locked()
            self._discard_inactive_waiters_locked()
            if self._outstanding < self._queue_capacity and not self._admission_waiters:
                self._outstanding += 1
                return
            waiting_count = sum(
                waiter.state == "waiting"
                for waiter in self._admission_waiters
            )
            if waiting_count >= self._max_waiters:
                raise DatabaseWriterOverloadedError("database writer admission queue is full")

            future = loop.create_future()
            waiter = _AdmissionWaiter(loop=loop, future=future)
            self._admission_waiters.append(waiter)
            grants = self._grant_waiters_locked()

        self._schedule_admission_grants(grants)
        try:
            await waiter.future
            with self._state_lock:
                if waiter.state != "granted":
                    raise DatabaseWriterClosedError("database writer is closing or closed")
                waiter.state = "consumed"
        except BaseException:
            grants, enqueue_stop = self._cancel_admission(waiter)
            self._schedule_admission_grants(grants)
            if enqueue_stop:
                self._queue.put_nowait(_STOP)
            raise

    def _raise_if_not_open_locked(self) -> None:
        if self._state != "open":
            raise DatabaseWriterClosedError("database writer is closing or closed")

    def _ensure_worker_started_locked(self) -> None:
        if self._thread is not None:
            return
        thread = threading.Thread(
            target=self._worker_main,
            name="rapid-inbox-db-writer",
            daemon=True,
        )
        self._thread = thread
        try:
            thread.start()
        except BaseException as exc:
            self._thread = None
            self._state = "closed"
            self._closed.set_exception(exc)
            raise

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

    def _cancel_admission(
        self,
        waiter: _AdmissionWaiter,
    ) -> tuple[list[_AdmissionWaiter], bool]:
        with self._state_lock:
            if waiter.state == "waiting":
                waiter.state = "cancelled"
            elif waiter.state == "granted":
                waiter.state = "cancelled"
                self._outstanding -= 1
            grants = self._grant_waiters_locked()
            enqueue_stop = self._prepare_stop_locked()
        return grants, enqueue_stop

    def _schedule_admission_grants(self, waiters: list[_AdmissionWaiter]) -> None:
        for waiter in waiters:
            try:
                waiter.loop.call_soon_threadsafe(self._deliver_admission_grant, waiter)
            except RuntimeError:
                # The destination loop disappeared before consuming its slot.
                grants, enqueue_stop = self._cancel_admission(waiter)
                self._schedule_admission_grants(grants)
                if enqueue_stop:
                    self._queue.put_nowait(_STOP)

    def _deliver_admission_grant(self, waiter: _AdmissionWaiter) -> None:
        grants: list[_AdmissionWaiter] = []
        enqueue_stop = False
        with self._state_lock:
            if waiter.state != "granted":
                return
            if waiter.future.done():
                waiter.state = "cancelled"
                self._outstanding -= 1
                grants = self._grant_waiters_locked()
                enqueue_stop = self._prepare_stop_locked()
            else:
                waiter.future.set_result(None)
        self._schedule_admission_grants(grants)
        if enqueue_stop:
            self._queue.put_nowait(_STOP)

    def _schedule_admission_rejection(self, waiter: _AdmissionWaiter) -> None:
        try:
            waiter.loop.call_soon_threadsafe(self._deliver_admission_rejection, waiter)
        except RuntimeError:
            # A closed event loop has no coroutine left to wake.
            return

    @staticmethod
    def _deliver_admission_rejection(waiter: _AdmissionWaiter) -> None:
        if not waiter.future.done():
            waiter.future.set_exception(
                DatabaseWriterClosedError("database writer is closing or closed")
            )

    def _release_capacity(self) -> None:
        with self._state_lock:
            if self._outstanding <= 0:
                raise RuntimeError("database writer capacity counter underflow")
            self._outstanding -= 1
            grants = self._grant_waiters_locked()
            enqueue_stop = self._prepare_stop_locked()
        self._schedule_admission_grants(grants)
        if enqueue_stop:
            self._queue.put_nowait(_STOP)

    def _prepare_stop_locked(self) -> bool:
        if (
            self._state == "closing"
            and self._outstanding == 0
            and not self._stop_enqueued
        ):
            self._stop_enqueued = True
            return True
        return False

    def _worker_main(self) -> None:
        connection: sqlite3.Connection | None = None
        fatal_error: BaseException | None = None
        active_request: _WriteRequest[Any] | None = None
        try:
            while True:
                item = self._queue.get()
                try:
                    if item is _STOP:
                        return
                    request = cast(_WriteRequest[Any], item)
                    active_request = request
                    if connection is None:
                        try:
                            connection = self._open_connection()
                        except BaseException as exc:
                            request.result.set_exception(exc)
                            self._release_capacity()
                            active_request = None
                            continue
                    connection = self._run_request(connection, request)
                    active_request = None
                finally:
                    self._queue.task_done()
        except BaseException as exc:  # pragma: no cover - catastrophic actor failure
            fatal_error = exc
            self._begin_fatal_shutdown()
            if active_request is not None and not active_request.result.done():
                try:
                    active_request.result.set_exception(exc)
                finally:
                    self._release_capacity()
            self._fail_owned_requests(exc)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            with self._state_lock:
                self._state = "closed"
            if fatal_error is None:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(fatal_error)

    def _begin_fatal_shutdown(self) -> None:
        """Close admission before failing work owned by a dead worker.

        Releasing queued requests decrements the outstanding counter.  The
        ordinary release path grants waiters while the actor is open, so the
        state transition and waiter rejection must happen first or those
        callers can be sent to a worker that is already unwinding.
        """

        rejected: list[_AdmissionWaiter] = []
        with self._state_lock:
            if self._state != "closed":
                self._state = "closing"
            # The failing worker itself is the stop signal.  Suppress the
            # ordinary final-capacity path from enqueueing an unconsumed
            # sentinel after the worker has exited.
            self._stop_enqueued = True
            while self._admission_waiters:
                waiter = self._admission_waiters.popleft()
                if waiter.state != "waiting":
                    continue
                waiter.state = "rejected"
                rejected.append(waiter)

        for waiter in rejected:
            self._schedule_admission_rejection(waiter)

    def _open_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        try:
            apply_pragmas(connection, durable_writes=True)
        except BaseException:
            connection.close()
            raise
        return connection

    def _run_request(
        self,
        connection: sqlite3.Connection,
        request: _WriteRequest[Any],
    ) -> sqlite3.Connection | None:
        try:
            # A submitted operation may change connection-local PRAGMAs.  The
            # persistent actor connection must restore the same safety
            # contract that a fresh mutation connection had for every call.
            apply_pragmas(connection, durable_writes=True)
            result = request.operation(connection)
            connection.commit()
        except BaseException as exc:
            connection_valid = True
            try:
                connection.rollback()
            except sqlite3.Error:
                connection_valid = False
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            request.result.set_exception(exc)
            next_connection = connection if connection_valid else None
        else:
            request.result.set_result(result)
            next_connection = connection
        finally:
            self._release_capacity()
        return next_connection

    def _fail_owned_requests(self, error: BaseException) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            try:
                if item is not _STOP:
                    request = cast(_WriteRequest[Any], item)
                    request.result.set_exception(error)
                    self._release_capacity()
            finally:
                self._queue.task_done()
