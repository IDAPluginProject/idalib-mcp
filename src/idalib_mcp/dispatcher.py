from __future__ import annotations

import json
import logging
import math
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable

logger = logging.getLogger(__name__)


class CancelledError(Exception):
    """Raised when a dispatcher run is cancelled."""


class QueueClosedError(RuntimeError):
    """Raised when a submission is rejected; str(exc) is the reason."""


@dataclass
class RequestRecord:
    id: str
    json_rpc_id: Any
    session_id: str
    tool: str
    enqueued_at: float
    started_at: float | None = None
    finished_at: float | None = None
    status: str = "queued"
    duration_ms: float | None = None
    args_preview: str = ""
    args_full_id: str | None = None
    result_preview: str | None = None
    result_full_id: str | None = None
    error: str | None = None
    kind: str = "tool"
    phase: str | None = None

    def to_dict(self, result_available: bool = False) -> dict[str, Any]:
        data = asdict(self)
        data["result_available"] = result_available
        return data


PREVIEW_CHARS = 500

_FINAL_STATUSES = frozenset({"ok", "error", "cancelled"})
_RUN_STEP = 0.1


def _serialize(value: Any) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), default=str)
    except Exception:
        return str(value)


def _make_preview(value: Any) -> tuple[str, str | None]:
    """Return (preview, full_text_if_truncated)."""
    text = _serialize(value)
    if len(text) <= PREVIEW_CHARS:
        return text, None
    return f"{text[:PREVIEW_CHARS]}…[{len(text)} chars total]", text


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def _exception_text(exc: BaseException) -> str:
    try:
        return str(exc)
    except Exception:
        return type(exc).__name__


def _seconds_between(start: float, end: float) -> float:
    return round(max(0.0, end - start), 1)


class Job:
    """One queued unit of work; all mutable state is guarded by the owning dispatcher's lock."""

    def __init__(
        self,
        dispatcher: WorkerDispatcher,
        record: RequestRecord,
        fn: Callable[[Job], Any] | None,
        tool_text: str,
    ) -> None:
        self.id = record.id
        self.session_id = record.session_id
        self.kind = record.kind
        self.tool = record.tool
        self.json_rpc_id = record.json_rpc_id
        self._dispatcher = dispatcher
        self._record = record
        self._fn = fn
        self._tool_text = tool_text
        self._done = threading.Event()
        self._cancel_requested = False
        self._hook: Callable[[], None] | None = None
        self._hook_ran = False
        self._result: Any = None
        self._has_result = False
        self._exception: BaseException | None = None
        self._evicted = False

    @property
    def status(self) -> str:
        with self._dispatcher._cond:
            return self._record.status

    @property
    def cancel_requested(self) -> bool:
        with self._dispatcher._cond:
            return self._cancel_requested

    def wait(self, timeout: float | None = None) -> bool:
        if timeout is None or timeout > threading.TIMEOUT_MAX:
            self._done.wait()
        elif not math.isnan(timeout) and timeout > 0:
            self._done.wait(timeout)
        return self.is_done()

    def is_done(self) -> bool:
        return self._done.is_set()

    def set_phase(self, phase: str | None) -> None:
        with self._dispatcher._cond:
            if self._record.status not in _FINAL_STATUSES:
                self._record.phase = phase

    def set_cancel_hook(self, hook: Callable[[], None] | None) -> None:
        with self._dispatcher._cond:
            if hook is None:
                self._hook = None
                return
            if self._record.status in _FINAL_STATUSES:
                return
            if self._cancel_requested and not self._hook_ran:
                self._hook = None
                self._hook_ran = True
            else:
                self._hook = hook
                return
        _invoke_hook(self, hook)

    def result_available(self) -> bool:
        dispatcher = self._dispatcher
        with dispatcher._cond:
            now = time.time()
            dispatcher._free_expired_locked(now)
            return dispatcher._result_available_locked(self, now)

    def result(self) -> Any:
        dispatcher = self._dispatcher
        with dispatcher._cond:
            now = time.time()
            dispatcher._free_expired_locked(now)
            if not dispatcher._result_available_locked(self, now):
                raise LookupError("Job result not available")
            return self._result

    def exception(self) -> BaseException | None:
        with self._dispatcher._cond:
            return self._exception if self._record.status == "error" else None

    def info(self, now: float | None = None) -> dict[str, Any]:
        info = self._dispatcher.job_info(self.id, now)
        return {} if info is None else info


def _invoke_hook(job: Job, hook: Callable[[], None]) -> None:
    try:
        hook()
    except Exception:
        logger.exception("Cancel hook failed for job %s (%s)", job.id, job.session_id)


class WorkerDispatcher:
    HISTORY_CAP = 100
    PAYLOAD_CAP = 200
    RESULT_TTL = 600.0

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self._cond = threading.Condition(threading.Lock())
        self._queue: deque[Job] = deque()
        self._current: Job | None = None
        self._history: deque[Job] = deque()
        self._jobs: dict[str, Job] = {}
        self._payloads: "OrderedDict[str, str]" = OrderedDict()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._close_reason: str | None = None

    # --- public snapshot -------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        with self._cond:
            now = time.time()
            self._free_expired_locked(now)
            current = self._current
            return {
                "session_id": self.session_id,
                "current": current._record.to_dict() if current else None,
                "queued": [job._record.to_dict() for job in self._queue],
                "history": [
                    job._record.to_dict(self._result_available_locked(job, now)) for job in self._history
                ],
            }

    def get_payload(self, payload_id: str) -> str | None:
        with self._cond:
            return self._payloads.get(payload_id)

    # --- submission ------------------------------------------------------

    def submit(
        self,
        tool: str,
        json_rpc_id: Any,
        args: Any,
        fn: Callable[[Job], Any],
        *,
        kind: str = "tool",
    ) -> Job:
        if not callable(fn):
            raise TypeError("fn must be callable")
        args_preview, args_full = _make_preview(args)
        tool_text = str(tool)
        with self._cond:
            self._free_expired_locked(time.time())
            if self._closed:
                raise QueueClosedError(self._close_reason)
            if self._draining_locked():
                raise QueueClosedError(f"Database {self.session_id} is closing")
            job = self._new_job_locked(tool, json_rpc_id, args_preview, args_full, fn, kind, tool_text)
            if self._thread is None:
                thread = threading.Thread(
                    target=self._executor_loop,
                    name=f"queue-{self.session_id}",
                    daemon=True,
                )
                thread.start()
                self._thread = thread
            self._queue.append(job)
            self._jobs[job.id] = job
            self._cond.notify_all()
            return job

    def run(
        self,
        tool: str,
        json_rpc_id: Any,
        args: Any,
        fn: Callable[[threading.Event], Any],
        cancel_event: threading.Event,
        *,
        kind: str = "tool",
    ) -> Any:
        if self.is_executor_thread():
            raise RuntimeError("WorkerDispatcher.run called from its own executor")
        if cancel_event.is_set():
            self._record_cancelled_run(tool, json_rpc_id, args, kind)
            raise CancelledError("Cancelled while queued")

        returned: list[Any] = []

        def call(_job: Job) -> Any:
            value = fn(cancel_event)
            returned.append(value)
            return value

        job = self.submit(tool, json_rpc_id, args, call, kind=kind)
        while not job.wait(_RUN_STEP):
            if cancel_event.is_set():
                self.cancel(job.id)
                raise CancelledError("Cancelled")

        with self._cond:
            status = job._record.status
            error = job._record.error
            exception = job._exception
        if status == "ok":
            return returned[0]
        if status == "error":
            if exception is not None:
                raise exception
            raise QueueClosedError(error)
        raise CancelledError(error or "Cancelled")

    # --- control ---------------------------------------------------------

    def cancel(self, job_id: str) -> str:
        if not isinstance(job_id, str):
            return "unknown"
        with self._cond:
            now = time.time()
            self._free_expired_locked(now)
            job = self._jobs.get(job_id)
            if job is None:
                return "unknown"
            status = job._record.status
            if status == "queued":
                self._queue.remove(job)
                self._finish_unstarted_locked(job, "cancelled", "Cancelled while queued", now)
                return "removed"
            if status == "cancelling":
                return "already_cancelling"
            if status != "running":
                return "finished"
            job._cancel_requested = True
            job._record.status = "cancelling"
            hook = None if job._hook_ran else job._hook
            if hook is not None:
                job._hook = None
                job._hook_ran = True
        if hook is not None:
            _invoke_hook(job, hook)
        return "cancelling"

    def close(self, reason: str) -> list[Job]:
        with self._cond:
            if self._closed:
                return []
            self._closed = True
            self._close_reason = reason
            now = time.time()
            failed = list(self._queue)
            self._queue.clear()
            for job in failed:
                self._finish_unstarted_locked(job, "error", reason, now)
            self._cond.notify_all()
            return failed

    # --- queries ---------------------------------------------------------

    def job(self, job_id: str) -> Job | None:
        if not isinstance(job_id, str):
            return None
        with self._cond:
            self._free_expired_locked(time.time())
            return self._jobs.get(job_id)

    def job_info(self, job_id: str, now: float | None = None) -> dict[str, Any] | None:
        if not isinstance(job_id, str):
            return None
        with self._cond:
            now = time.time() if now is None else now
            self._free_expired_locked(time.time())
            job = self._jobs.get(job_id)
            return None if job is None else self._job_info_locked(job, now)

    def jobs_info(self, *, recent: int = 20, now: float | None = None) -> dict[str, Any]:
        if not isinstance(recent, int):
            raise TypeError("recent must be an int")
        with self._cond:
            now = time.time() if now is None else now
            self._free_expired_locked(time.time())
            count = min(max(recent, 0), self.HISTORY_CAP)
            newest = list(self._history)[::-1][:count]
            current = self._current
            return {
                "running": self._job_info_locked(current, now) if current else None,
                "queued": [self._job_info_locked(job, now) for job in self._queue],
                "recent": [self._job_info_locked(job, now) for job in newest],
            }

    def queued_count(self) -> int:
        with self._cond:
            return len(self._queue)

    def current_job(self) -> Job | None:
        with self._cond:
            return self._current

    def is_executor_thread(self) -> bool:
        thread = self._thread
        return thread is not None and threading.current_thread() is thread

    def draining(self) -> bool:
        with self._cond:
            return self._draining_locked()

    @property
    def closed(self) -> bool:
        with self._cond:
            return self._closed

    @property
    def close_reason(self) -> str | None:
        with self._cond:
            return self._close_reason

    # --- executor --------------------------------------------------------

    def _executor_loop(self) -> None:
        while True:
            with self._cond:
                while not self._queue and not self._closed:
                    self._cond.wait()
                if not self._queue:
                    return
                job = self._queue.popleft()
                job._record.status = "running"
                job._record.started_at = time.time()
                self._current = job
                fn = job._fn
            self._execute(job, fn)
            # Drop references so an idle executor does not pin the last job's closure or result.
            del job, fn

    def _execute(self, job: Job, fn: Callable[[Job], Any] | None) -> None:
        value: Any = None
        raised: BaseException | None = None
        try:
            value = fn(job) if fn is not None else None
        except BaseException as exc:
            raised = exc
        error_text = None if raised is None else _exception_text(raised)

        result_preview: str | None = None
        result_full: str | None = None
        if raised is None:
            try:
                result_preview, result_full = _make_preview(value)
            except Exception:
                logger.exception("Could not preview the result of job %s (%s)", job.id, self.session_id)
                result_preview = "<unpreviewable result>"

        log_error = False
        with self._cond:
            record = job._record
            if job._cancel_requested:
                record.status = "cancelled"
                record.error = error_text if raised is not None else "Cancelled while running; result discarded"
            elif raised is None:
                record.status = "ok"
                record.result_preview = result_preview
                if result_full is not None:
                    record.result_full_id = f"{record.id}.result"
                    self._store_payload_locked(record.result_full_id, result_full)
                job._result = value
                job._has_result = True
            elif isinstance(raised, CancelledError):
                record.status = "cancelled"
                record.error = error_text or "Cancelled"
            else:
                record.status = "error"
                record.error = error_text
                job._exception = raised
                log_error = True
            self._complete_locked(job, time.time())
            self._current = None
        if log_error:
            logger.error(
                "Job %s (%s, %s) failed",
                job.id,
                job._tool_text,
                self.session_id,
                exc_info=raised,
            )
        job._done.set()

    # --- internals -------------------------------------------------------

    def _new_job_locked(
        self,
        tool: Any,
        json_rpc_id: Any,
        args_preview: str,
        args_full: str | None,
        fn: Callable[[Job], Any] | None,
        kind: str,
        tool_text: str,
    ) -> Job:
        job_id = uuid.uuid4().hex[:12]
        while job_id in self._jobs:
            job_id = uuid.uuid4().hex[:12]
        record = RequestRecord(
            id=job_id,
            json_rpc_id=json_rpc_id,
            session_id=self.session_id,
            tool=tool,
            enqueued_at=time.time(),
            args_preview=args_preview,
            kind=kind,
        )
        if args_full is not None:
            record.args_full_id = f"{job_id}.args"
            self._store_payload_locked(record.args_full_id, args_full)
        return Job(self, record, fn, tool_text)

    def _record_cancelled_run(self, tool: Any, json_rpc_id: Any, args: Any, kind: str) -> None:
        args_preview, args_full = _make_preview(args)
        tool_text = str(tool)
        with self._cond:
            job = self._new_job_locked(tool, json_rpc_id, args_preview, args_full, None, kind, tool_text)
            self._jobs[job.id] = job
            self._finish_unstarted_locked(job, "cancelled", "Cancelled while queued", time.time())

    def _finish_unstarted_locked(self, job: Job, status: str, error: str, now: float) -> None:
        job._record.status = status
        job._record.error = error
        self._complete_locked(job, now)
        job._done.set()

    def _complete_locked(self, job: Job, now: float) -> None:
        record = job._record
        record.finished_at = now
        base = record.started_at if record.started_at is not None else record.enqueued_at
        record.duration_ms = max(0.0, (now - base) * 1000.0)
        job._fn = None
        job._hook = None
        self._history.append(job)
        while len(self._history) > max(self.HISTORY_CAP, 0):
            evicted = self._history.popleft()
            evicted._evicted = True
            evicted._result = None
            evicted._has_result = False
            self._jobs.pop(evicted.id, None)

    def _job_info_locked(self, job: Job, now: float) -> dict[str, Any]:
        record = job._record
        end = record.finished_at if record.finished_at is not None else now
        if record.status == "queued":
            position = self._queue.index(job) + 1
            queue_position: int | None = position
            jobs_ahead: int | None = position - 1 + (1 if self._current is not None else 0)
        elif record.status in _FINAL_STATUSES:
            queue_position = jobs_ahead = None
        else:
            queue_position, jobs_ahead = None, 0
        return {
            "job_id": job.id,
            "database": self.session_id,
            "tool": job._tool_text,
            "kind": record.kind,
            "status": record.status,
            "phase": record.phase,
            "queue_position": queue_position,
            "jobs_ahead": jobs_ahead,
            "enqueued_at": _iso(record.enqueued_at),
            "started_at": _iso(record.started_at),
            "finished_at": _iso(record.finished_at),
            "elapsed_seconds": _seconds_between(record.enqueued_at, end),
            "run_seconds": None if record.started_at is None else _seconds_between(record.started_at, end),
            "error": record.error,
            "result_available": self._result_available_locked(job, time.time()),
            "args_preview": record.args_preview,
        }

    def _draining_locked(self) -> bool:
        if self._current is not None and self._current.kind == "close":
            return True
        return any(job.kind == "close" for job in self._queue)

    def _result_available_locked(self, job: Job, now: float) -> bool:
        record = job._record
        return (
            record.status == "ok"
            and job._has_result
            and not job._evicted
            and record.finished_at is not None
            and now - record.finished_at < self.RESULT_TTL
        )

    def _free_expired_locked(self, now: float) -> None:
        ttl = self.RESULT_TTL
        for job in self._history:
            if job._has_result and not (now - job._record.finished_at < ttl):
                job._result = None
                job._has_result = False

    def _store_payload_locked(self, key: str, text: str) -> None:
        self._payloads[key] = text
        while len(self._payloads) > self.PAYLOAD_CAP:
            self._payloads.popitem(last=False)
