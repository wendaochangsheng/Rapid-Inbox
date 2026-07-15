from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParseTask:
    message_id: str
    raw_size_bytes: int


class ParseQueue:
    def __init__(
        self,
        worker: Callable[[ParseTask], Awaitable[None]],
        *,
        worker_count: int = 1,
        max_messages: int = 10_000,
        max_bytes: int = 536_870_912,
    ) -> None:
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or max_messages < 1:
            raise ValueError("max_messages must be a positive integer")
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        self._worker = worker
        self._worker_count = max(1, int(worker_count))
        self._queue: asyncio.Queue[ParseTask | None] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self._active_message_ids: set[str] = set()
        self._reserved_message_ids: set[str] = set()
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._reserved_messages = 0
        self._reserved_bytes = 0
        self._active_changed = asyncio.Event()
        self._active_changed.set()
        self._capacity_changed = asyncio.Event()
        self._capacity_changed.set()

    async def start(self) -> None:
        if not self._tasks:
            self._tasks = [
                asyncio.create_task(self._run())
                for _ in range(self._worker_count)
            ]

    async def stop(self, *, discard_pending: bool = False, timeout: float | None = None) -> None:
        if not self._tasks:
            self._discard_queue_contents()
            return
        tasks = list(self._tasks)
        if discard_pending:
            self.clear_pending()
        for _ in tasks:
            await self._queue.put(None)
        try:
            waiter = asyncio.gather(*tasks)
            if timeout is None:
                await waiter
            else:
                await asyncio.wait_for(waiter, timeout=timeout)
        except TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            if all(task.done() for task in tasks):
                self._tasks = []
                # A timed-out/cancelled worker can leave both ordinary work and
                # stop sentinels behind.  Neither may leak into a later start.
                self._discard_queue_contents()

    @property
    def is_running(self) -> bool:
        return bool(self._tasks)

    @property
    def max_messages(self) -> int:
        return self._max_messages

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def reserved_messages(self) -> int:
        return self._reserved_messages

    @property
    def reserved_bytes(self) -> int:
        return self._reserved_bytes

    @property
    def active_messages(self) -> int:
        return len(self._active_message_ids)

    @property
    def queued_messages(self) -> int:
        return max(self._reserved_messages - self.active_messages, 0)

    def contains(self, message_id: str) -> bool:
        return message_id in self._reserved_message_ids

    def try_enqueue(self, task: ParseTask) -> bool:
        """Reserve and enqueue without waiting for capacity.

        Reservations include queued and active tasks.  Keeping the message ID
        in the same bounded state as the budgets also makes duplicate
        suppression exact without a second, potentially unbounded runtime set.
        """

        self._validate_task(task)
        if task.raw_size_bytes > self._max_bytes:
            return False
        if task.message_id in self._reserved_message_ids:
            return False
        if not self._has_capacity(task):
            return False
        self._reserved_message_ids.add(task.message_id)
        self._reserved_messages += 1
        self._reserved_bytes += task.raw_size_bytes
        self._queue.put_nowait(task)
        return True

    async def enqueue(self, task: ParseTask) -> bool:
        """Wait for capacity and enqueue, returning false for a duplicate."""

        self._validate_task(task)
        if task.raw_size_bytes > self._max_bytes:
            raise ValueError("parse task exceeds queue byte budget")
        while True:
            if task.message_id in self._reserved_message_ids:
                return False
            if self.try_enqueue(task):
                return True
            self._capacity_changed.clear()
            # Close the clear/wait race: a worker may have released capacity
            # just before the event was cleared.
            if task.message_id in self._reserved_message_ids:
                return False
            if self._has_capacity(task):
                continue
            await self._capacity_changed.wait()

    async def drain(self) -> None:
        await self._queue.join()

    def clear_pending(self) -> int:
        return self.remove_pending(lambda _task: True)

    def remove_pending(self, predicate: Callable[[ParseTask], bool]) -> int:
        cleared = 0
        retained: list[ParseTask | None] = []
        while True:
            try:
                task = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                for retained_task in retained:
                    self._queue.put_nowait(retained_task)
                return cleared

            if task is not None and predicate(task):
                cleared += 1
                self._release(task)
            else:
                retained.append(task)
            self._queue.task_done()

    async def wait_until_not_active(self, predicate: Callable[[str], bool]) -> None:
        while any(predicate(message_id) for message_id in self._active_message_ids):
            self._active_changed.clear()
            if not any(predicate(message_id) for message_id in self._active_message_ids):
                return
            await self._active_changed.wait()

    async def _run(self) -> None:
        while True:
            task = await self._queue.get()
            try:
                if task is None:
                    return
                self._active_message_ids.add(task.message_id)
                try:
                    await self._worker(task)
                except Exception:
                    continue
            finally:
                if task is not None:
                    self._active_message_ids.discard(task.message_id)
                    self._active_changed.set()
                    self._release(task)
                self._queue.task_done()

    def _validate_task(self, task: ParseTask) -> None:
        if not isinstance(task, ParseTask):
            raise TypeError("task must be a ParseTask")
        if not task.message_id:
            raise ValueError("parse task message_id is required")
        if (
            isinstance(task.raw_size_bytes, bool)
            or not isinstance(task.raw_size_bytes, int)
            or task.raw_size_bytes < 0
        ):
            raise ValueError("parse task raw_size_bytes must be a non-negative integer")

    def _has_capacity(self, task: ParseTask) -> bool:
        return (
            self._reserved_messages < self._max_messages
            and self._reserved_bytes + task.raw_size_bytes <= self._max_bytes
        )

    def _release(self, task: ParseTask) -> None:
        if task.message_id not in self._reserved_message_ids:
            raise RuntimeError("parse queue reservation is missing")
        self._reserved_message_ids.remove(task.message_id)
        self._reserved_messages -= 1
        self._reserved_bytes -= task.raw_size_bytes
        if self._reserved_messages < 0 or self._reserved_bytes < 0:
            raise RuntimeError("parse queue reservation underflow")
        self._capacity_changed.set()

    def _discard_queue_contents(self) -> None:
        while True:
            try:
                task = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if task is not None:
                self._release(task)
            self._queue.task_done()
