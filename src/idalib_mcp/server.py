from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from .cancellation import CancellationRegistry
from .config import default_ida_home_from_env, validate_ida_home
from .dispatcher import CancelledError, Job, QueueClosedError, WorkerDispatcher
from .installer import list_available_clients, print_mcp_config, run_install_command
from .winjob import assign_process_to_job, create_kill_on_close_job


logger = logging.getLogger(__name__)


def _load_upstream_supervisor() -> Any:
    try:
        return importlib.import_module("ida_pro_mcp.idalib_supervisor")
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'ida-pro-mcp'. Install this project with "
            "`pip install -e .` before running the supervisor."
        ) from exc


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class OpenSubmission:
    """What submit_open did: attached to a registered or pending database, or created an open job."""

    session_id: str
    job: Job | None
    created: bool
    session: Any | None


def _build_managed_supervisor_class(upstream):
    seat_phases = ("spawning", "opening", "registering")

    def reason_text(exc: BaseException) -> str:
        try:
            return str(exc) or type(exc).__name__
        except Exception:
            return type(exc).__name__

    def reuse_message(existing: str, requested: Any) -> str:
        return (
            f"Binary already open as session '{existing}', cannot reuse "
            f"different session_id '{requested}'."
        )

    @dataclass(eq=False)
    class PendingOpen:
        """A database from submit_open until it registers or its failure path removes it."""

        session_id: str
        input_path: str
        filename: str
        candidates: set[str]
        context_id: str | None
        dispatcher: WorkerDispatcher
        created_at: datetime = field(default_factory=datetime.now)
        phase: str = "discovering"
        worker: Any = None
        claimed: bool = False
        hook_set: bool = False
        open_sent: bool = False
        cancelled: bool = False
        previous_bindings: dict[str, str | None] = field(default_factory=dict)
        job: Job | None = None

        def live(self) -> bool:
            return not self.cancelled and self.job is not None and not self.job.is_done()

    class ManagedIdalibSupervisor(upstream.IdalibSupervisor):  # type: ignore[misc]
        RETIRED_TTL = 600.0

        def __init__(
            self,
            *args,
            ida_home: Path | None = None,
            show_worker_io: bool = False,
            sync_wait: float = 15.0,
            **kwargs,
        ):
            super().__init__(*args, **kwargs)
            self.ida_home = ida_home
            self.show_worker_io = show_worker_io
            self.sync_wait = sync_wait
            self._dispatchers: dict[str, WorkerDispatcher] = {}
            self._retired: list[tuple[str, str, WorkerDispatcher, float]] = []
            self._cancels = CancellationRegistry()
            self._job_local = threading.local()
            self._pending: dict[str, PendingOpen] = {}
            self._reopens: dict[str, dict[str, Any]] = {}
            self._schema_spawning = False
            self._schema_spawn_lock = threading.Lock()
            self._shut_down = False
            # OS-enforced backstop: workers assigned to this job are killed when
            # the supervisor process exits, so a hard kill never orphans them.
            self._worker_job = create_kill_on_close_job()

        # --- job threads ---------------------------------------------------

        def _pinned_session(self) -> str | None:
            return getattr(self._job_local, "session_id", None)

        def _wrap_job_body(self, body: Callable[[Job], Any], ensure_ready: bool) -> Callable[[Job], Any]:
            """Return the executor-side job wrapper; call it on the submitting thread."""
            transport_session_id = getattr(self.mcp._transport_session_id, "data", None)
            extensions = getattr(self.mcp._enabled_extensions, "data", None)
            mcp = self.mcp
            local = self._job_local
            supervisor = self

            def run(job: Job) -> Any:
                try:
                    local.session_id = job.session_id
                    mcp._transport_session_id.data = transport_session_id
                    mcp._enabled_extensions.data = set() if extensions is None else set(extensions)
                    supervisor._guard_follower(job)
                    if ensure_ready:
                        supervisor._ensure_ready(job)
                    if job.cancel_requested:
                        raise CancelledError("Cancelled")
                    return body(job)
                finally:
                    local.session_id = None
                    mcp._transport_session_id.data = None
                    mcp._enabled_extensions.data = set()

            return run

        def _guard_follower(self, job: Job) -> None:
            with self._lock:
                record = self._pending.get(job.session_id)
                if record is None or record.job is job:
                    return
            raise QueueClosedError(f"Database '{job.session_id}' did not open: Cancelled")

        def _ensure_ready(self, job: Job) -> None:
            with self._lock:
                session = self.sessions.get(job.session_id)
            if session is None or session.backend != "gui" or session.is_alive():
                return
            job.set_phase("reopening")
            try:
                self._reopen_gui_session_headless(session)
            finally:
                job.set_phase(None)

        def submit_job(
            self,
            session_id: Any,
            tool: str,
            json_rpc_id: Any,
            args: Any,
            body: Callable[[Job], Any],
            *,
            kind: str = "tool",
            ensure_ready: bool | None = None,
        ) -> Job:
            with self._lock:
                dispatcher = self._dispatchers.get(session_id) if isinstance(session_id, str) else None
                if dispatcher is None:
                    raise LookupError(f"Database/session not found: {session_id}")
                if not callable(body):
                    raise TypeError("body must be callable")
                if ensure_ready is None:
                    ensure_ready = kind != "close"
                wrapped = self._wrap_job_body(body, ensure_ready)
                return dispatcher.submit(tool, json_rpc_id, args, wrapped, kind=kind)

        # --- opening and seats ----------------------------------------------

        def submit_open(
            self,
            input_path: str,
            *,
            run_auto_analysis: bool = True,
            session_id: str | None = None,
            context_id: str | None = None,
            json_rpc_id: Any = None,
        ) -> OpenSubmission:
            resolved = self._normalize_input_path(input_path)
            with self._lock:
                if self._shut_down:
                    raise QueueClosedError("Supervisor shutting down")
                existing = self.path_to_session.get(self._path_key(resolved))
                if existing is not None:
                    session = self.sessions.get(existing)
                    if session is not None and session.is_alive():
                        if session_id is not None and session_id != existing:
                            raise ValueError(reuse_message(existing, session_id))
                        session.last_accessed = datetime.now()
                        if context_id is not None:
                            self.bind_context(context_id, existing)
                        return OpenSubmission(existing, None, False, session)
                    self._unregister_session_locked(existing)

                candidates = self._candidate_idb_paths(resolved)
                for record in self._pending.values():
                    if record.live() and record.candidates & candidates:
                        if session_id is not None and session_id != record.session_id:
                            raise ValueError(reuse_message(record.session_id, session_id))
                        if context_id is not None:
                            self.bind_context(context_id, record.session_id)
                        return OpenSubmission(record.session_id, record.job, False, None)

                if session_id is None:
                    session_id = str(uuid.uuid4())[:8]
                    while session_id in self.sessions or session_id in self._pending:
                        session_id = str(uuid.uuid4())[:8]
                elif session_id in self.sessions or session_id in self._pending:
                    raise ValueError(f"Session already exists: {session_id}")
                dispatcher = WorkerDispatcher(session_id)
                record = PendingOpen(
                    session_id, resolved, Path(resolved).name, candidates, context_id, dispatcher
                )
                self._pending[session_id] = record
                self._dispatchers[session_id] = dispatcher
                args = {
                    "input_path": resolved,
                    "run_auto_analysis": run_auto_analysis,
                    "session_id": session_id,
                }
                fn = self._open_job_fn(record, run_auto_analysis)
                record.job = dispatcher.submit("idalib_open", json_rpc_id, args, fn, kind="open")
                if context_id is not None:
                    self.bind_context(context_id, session_id)
                return OpenSubmission(session_id, record.job, True, None)

        def open_session(self, input_path, *, run_auto_analysis=True, session_id=None, context_id=None):
            if self._pinned_session() is not None:
                raise RuntimeError("open_session called from a job body")
            submission = self.submit_open(
                input_path,
                run_auto_analysis=run_auto_analysis,
                session_id=session_id,
                context_id=context_id,
            )
            job = submission.job
            if job is None:
                return submission.session
            job.wait(None)
            status = job.status
            if status == "ok":
                try:
                    sid = job.result()["session_id"]
                except LookupError:
                    sid = submission.session_id
                with self._lock:
                    session = self.sessions.get(sid)
                if session is None:
                    raise RuntimeError(f"Session is stale or missing: {sid}")
                return session
            error = job.info().get("error")
            if status == "error":
                exception = job.exception()
                if exception is not None:
                    raise exception
                raise QueueClosedError(error)
            raise CancelledError(error or "Cancelled")

        def _open_job_fn(self, record: PendingOpen, run_auto_analysis: bool) -> Callable[[Job], Any]:
            """Return the open job's fn; its failure path covers the wrapper's own early exits too."""
            outcome: dict[str, Any] = {
                "registered": False,
                "reason": "Cancelled",
                "rebind_to": None,
                "discard": False,
            }
            phases = self._wrap_job_body(
                lambda job: self._run_open(record, job, run_auto_analysis, outcome), True
            )

            def run(job: Job) -> Any:
                try:
                    return phases(job)
                except BaseException as exc:
                    if isinstance(exc, CancelledError):
                        outcome["reason"] = "Cancelled"
                    elif isinstance(exc, Exception) and self._open_cancel_seen(record, job):
                        outcome["reason"] = "Cancelled"
                        raise CancelledError("Cancelled") from exc
                    else:
                        outcome["reason"] = reason_text(exc)
                    raise
                finally:
                    if not outcome["registered"]:
                        self._finish_failed_open(
                            record,
                            outcome["reason"],
                            rebind_to=outcome["rebind_to"],
                            discard=outcome["discard"],
                        )

            return run

        def _run_open(
            self, record: PendingOpen, job: Job, run_auto_analysis: bool, outcome: dict[str, Any]
        ) -> Any:
            sid = record.session_id
            resolved = record.input_path
            self._set_open_phase(record, job, "discovering")
            instance = self._find_gui_instance_for_path(resolved)
            if instance is not None:
                session = self._make_gui_session(resolved, sid, instance)
                self._set_open_phase(record, job, "registering")
                return self._register_open(record, job, session, outcome)

            with self._lock:
                self._check_open_locked(record, job)
                worker = self._take_schema_worker_for_session()
                if worker is None:
                    self._prune_dead_worker_sessions_locked()
                    self._require_seat_locked()
                else:
                    record.worker = worker
                    record.claimed = True
                record.phase = "spawning"
                job.set_phase("spawning")
            if worker is None:
                worker = self._start_worker_process()
                with self._lock:
                    record.worker = worker
                    if self._shut_down:
                        raise RuntimeError("Supervisor shutting down")
                self._set_open_hook(record, job, worker)
                self._wait_worker_ready(worker)

            self._set_open_phase(record, job, "opening")
            if record.claimed:
                self._set_open_hook(record, job, worker)
                with self._lock:
                    self._check_open_locked(record, job)
            record.open_sent = True
            opened = self.call_worker_tool(
                worker,
                "idalib_open",
                {"input_path": resolved, "run_auto_analysis": run_auto_analysis, "session_id": sid},
            )
            if isinstance(opened, dict) and opened.get("error"):
                raise RuntimeError(str(opened["error"]))
            worker_session = opened.get("session", {}) if isinstance(opened, dict) else {}
            session = upstream.WorkerSession(
                session_id=sid,
                input_path=str(worker_session.get("input_path") or resolved),
                filename=str(worker_session.get("filename") or Path(resolved).name),
                is_analyzing=bool(worker_session.get("is_analyzing", False)),
                metadata=dict(worker_session.get("metadata") or {}),
                host=worker.host,
                port=worker.port,
                process=worker.process,
                backend="worker",
                owned=True,
                pid=worker.process.pid if worker.process is not None else None,
            )
            self._set_open_phase(record, job, "registering")
            return self._register_open(record, job, session, outcome)

        def _register_open(self, record: PendingOpen, job: Job, session: Any, outcome: dict[str, Any]) -> Any:
            sid = record.session_id
            resolved = record.input_path
            with self._lock:
                self._check_open_locked(record, job)
                existing = self.path_to_session.get(self._path_key(resolved))
                current = None if existing is None else self.sessions.get(existing)
                if current is not None and current.is_alive():
                    current.last_accessed = datetime.now()
                    outcome.update(
                        rebind_to=existing, discard=True, reason=f"already open as session '{existing}'"
                    )
                    return self._open_result(current, record)
                if existing is not None:
                    self._unregister_session_locked(existing)
                if self._pending.get(sid) is not record:
                    raise RuntimeError(f"Pending open was removed: {sid}")
                del self._pending[sid]
                self._register_session_locked(session, resolved, None)
                outcome["registered"] = True
                return self._open_result(session, record)

        def _open_result(self, session: Any, record: PendingOpen) -> dict[str, Any]:
            return {
                "session_id": session.session_id,
                "session": session.to_dict(),
                "context_id": record.context_id,
            }

        def _set_open_phase(self, record: PendingOpen, job: Job, phase: str) -> None:
            with self._lock:
                self._check_open_locked(record, job)
                record.phase = phase
                job.set_phase(phase)

        def _set_open_hook(self, record: PendingOpen, job: Job, worker: Any) -> None:
            record.hook_set = True
            job.set_cancel_hook(lambda: self._terminate_worker(worker))

        def _check_open_locked(self, record: PendingOpen, job: Job) -> None:
            if self._shut_down:
                raise RuntimeError("Supervisor shutting down")
            if record.cancelled or job.cancel_requested:
                raise CancelledError("Cancelled")

        def _open_cancel_seen(self, record: PendingOpen, job: Job) -> bool:
            with self._lock:
                return record.cancelled or job.cancel_requested

        def _require_seat_locked(self) -> None:
            if self.max_workers > 0 and self.seats_in_use() >= self.max_workers:
                raise RuntimeError(
                    f"Maximum idalib worker count reached ({self.max_workers}). "
                    "Close a database with idalib_close or increase --max-workers."
                )

        def _finish_failed_open(
            self, record: PendingOpen, reason: str, *, rebind_to: str | None = None, discard: bool = False
        ) -> None:
            """Dispose of the open's worker, then remove its record, bindings and queue if still its own."""
            try:
                reusable = record.claimed and not record.open_sent and not record.hook_set
                discard_as = record.session_id if discard else None
                self._dispose_worker(record.worker, reusable=reusable, discard_as=discard_as)
            finally:
                sid = record.session_id
                with self._lock:
                    if self._pending.get(sid) is record:
                        del self._pending[sid]
                        for context, bound in list(self.context_bindings.items()):
                            if bound != sid:
                                continue
                            previous = rebind_to
                            if previous is None:
                                previous = record.previous_bindings.get(context)
                            if previous in self.sessions or self._is_live_pending(previous):
                                self.context_bindings[context] = previous
                            else:
                                del self.context_bindings[context]
                        dispatcher = record.dispatcher
                        if self._dispatchers.get(sid) is dispatcher:
                            del self._dispatchers[sid]
                        self._prune_retired_locked()
                        self._retired.append((sid, record.filename, dispatcher, time.monotonic()))
                        dispatcher.close(f"Database '{sid}' did not open: {reason}")

        def _is_live_pending(self, session_id: str) -> bool:
            record = self._pending.get(session_id)
            return record is not None and record.live()

        def _dispose_worker(
            self, worker: Any, *, reusable: bool = False, discard_as: str | None = None
        ) -> None:
            """Hand a reusable, alive worker back as the schema worker if that slot is free; else stop it."""
            if worker is None:
                return
            try:
                if reusable and worker.is_alive():
                    with self._lock:
                        slot = self._schema_worker
                        if not self._shut_down and (slot is None or not slot.is_alive()):
                            self._schema_worker = worker
                            return
                if discard_as is not None:
                    self._discard_opened_worker_session(worker, discard_as)
                else:
                    self._terminate_worker(worker)
            except Exception:
                logger.exception("Could not stop worker pid %s", getattr(worker, "pid", None))

        def seats_in_use(self) -> int:
            with self._lock:
                count = sum(
                    1
                    for session in self.sessions.values()
                    if session.backend == "worker" and session.owned and session.is_alive()
                )
                count += sum(1 for record in self._pending.values() if record.phase in seat_phases)
                count += len(self._reopens)
                schema = self._schema_worker
                if schema is not None and schema.is_alive():
                    count += 1
                if self._schema_spawning:
                    count += 1
                return count

        def _schema_or_idle_worker(self):
            if self._pinned_session() is not None:
                raise RuntimeError("_schema_or_idle_worker called from a job body")
            with self._lock:
                worker = self._select_schema_worker_locked()
            if worker is not None:
                return worker
            with self._schema_spawn_lock:
                with self._lock:
                    worker = self._select_schema_worker_locked()
                    if worker is not None:
                        return worker
                    if self.max_workers > 0 and self.seats_in_use() >= self.max_workers:
                        raise RuntimeError("No idle worker for tool discovery; all worker seats are in use")
                    self._schema_spawning = True
                try:
                    spawned = self._spawn_worker()
                    with self._lock:
                        current = self._schema_worker
                        if self._shut_down:
                            result = None
                        elif current is not None and current.is_alive():
                            result = current
                        else:
                            self._schema_worker = spawned
                            result = spawned
                    if result is not spawned:
                        self._dispose_worker(spawned)
                finally:
                    with self._lock:
                        self._schema_spawning = False
            if result is None:
                raise RuntimeError("Supervisor shutting down")
            return result

        def _select_schema_worker_locked(self) -> Any:
            usable = []
            for session in self.sessions.values():
                if session.backend != "worker" or not session.is_alive():
                    continue
                dispatcher = self._dispatchers.get(session.session_id)
                if dispatcher is None or dispatcher.draining():
                    continue
                if dispatcher.current_job() is None and dispatcher.queued_count() == 0:
                    return session
                usable.append(session)
            schema = self._schema_worker
            if schema is not None and schema.is_alive():
                return schema
            return usable[0] if usable else None

        def _reopen_gui_session_headless(self, session):
            sid = session.session_id
            if self._pinned_session() != sid:
                return session
            logger.info("GUI IDA backend for session %s is unavailable; reopening headless", sid)
            resolved = self._resolve_gui_fallback_path(session)
            mark: dict[str, Any] = {"worker": None}
            worker = None
            claimed = False
            open_sent = False
            settled = False
            try:
                with self._lock:
                    if self._shut_down:
                        raise RuntimeError("Supervisor shutting down")
                    worker = self._take_schema_worker_for_session()
                    if worker is None:
                        self._prune_dead_worker_sessions_locked()
                        self._require_seat_locked()
                    else:
                        claimed = True
                        mark["worker"] = worker
                    self._reopens[sid] = mark
                if worker is None:
                    worker = self._spawn_worker()
                    with self._lock:
                        mark["worker"] = worker
                        if self._shut_down:
                            raise RuntimeError("Supervisor shutting down")
                open_sent = True
                opened = self.call_worker_tool(
                    worker,
                    "idalib_open",
                    {"input_path": resolved, "run_auto_analysis": False, "session_id": sid},
                )
                if isinstance(opened, dict) and opened.get("error"):
                    raise RuntimeError(str(opened["error"]))
                worker_session = opened.get("session", {}) if isinstance(opened, dict) else {}
                replacement = upstream.WorkerSession(
                    session_id=sid,
                    input_path=str(worker_session.get("input_path") or resolved),
                    filename=str(worker_session.get("filename") or Path(resolved).name),
                    is_analyzing=bool(worker_session.get("is_analyzing", False)),
                    metadata={
                        **session.metadata,
                        **dict(worker_session.get("metadata") or {}),
                        "fallback_from_gui": True,
                    },
                    host=worker.host,
                    port=worker.port,
                    process=worker.process,
                    backend="worker",
                    owned=True,
                    pid=worker.process.pid if worker.process is not None else None,
                )
                with self._lock:
                    if self._reopens.get(sid) is mark:
                        del self._reopens[sid]
                    if self._shut_down:
                        raise RuntimeError("Supervisor shutting down")
                    current = self.sessions.get(sid)
                    if current is session:
                        self._register_session_locked(replacement, resolved, None)
                        settled = True
                        return replacement
                    if current is not None and current.is_alive():
                        current.last_accessed = datetime.now()
                        kept = current
                    else:
                        if current is not None:
                            self._unregister_session_locked(sid)
                        kept = None
                settled = True
                self._dispose_worker(worker, discard_as=sid)
                if kept is None:
                    raise RuntimeError(f"Session '{sid}' was closed or replaced while reopening headlessly")
                return kept
            except BaseException:
                if not settled:
                    self._dispose_worker(worker, reusable=claimed and not open_sent)
                raise
            finally:
                with self._lock:
                    if self._reopens.get(sid) is mark:
                        del self._reopens[sid]

        def resolve_target(self, database: Any) -> str:
            if self._pinned_session() is not None:
                raise RuntimeError("resolve_target called from a job body")
            with self._lock:
                if database:
                    records = self._pending_matches_locked(database)
                    if len(records) > 1:
                        raise RuntimeError(f"Database selector is ambiguous: {database}")
                    if records:
                        return records[0].session_id
                else:
                    bound = self._bound_session_locked()
                    if bound is not None and bound in self._pending:
                        return bound
            return self.resolve_session(database).session_id

        def _bound_session_locked(self) -> str | None:
            context_id = self.resolve_context_id()
            bound = self.context_bindings.get(context_id)
            if bound is None and not self.isolated_contexts:
                bound = self.context_bindings.get(upstream.SHARED_FALLBACK_CONTEXT_ID)
            return bound

        def _pending_matches_locked(self, database: Any) -> list[PendingOpen]:
            """Match pending opens by id (any record), then live ones by path key, filename, input path."""
            if not isinstance(database, str) or not self._pending:
                return []
            record = self._pending.get(database)
            if record is not None:
                return [record]
            live = [record for record in self._pending.values() if record.live()]
            try:
                key = self._path_key(database)
            except Exception:
                key = os.path.normcase(database)
            normalized = os.path.normcase(database)
            for matches in (
                [record for record in live if key in record.candidates],
                [record for record in live if record.filename == database],
                [record for record in live if os.path.normcase(record.input_path) == normalized],
            ):
                if matches:
                    return matches
            return []

        def _worker_rpc(self, worker, payload, *, timeout=None) -> dict[str, Any]:
            pinned = self._pinned_session()
            with self._lock:
                target = worker.session_id if worker.session_id in self.sessions else None
                dispatcher = None if target is None else self._dispatchers.get(target)
            direct = super()._worker_rpc
            if target is None or pinned == target:
                return direct(worker, payload, timeout=timeout)
            if pinned is not None:
                raise RuntimeError("Cross-database worker RPC from a job body")
            if dispatcher is None:
                raise QueueClosedError("Supervisor shutting down")
            params = payload.get("params")
            if payload.get("method") == "tools/call" and isinstance(params, dict):
                label = params.get("name")
            else:
                label = payload.get("method")

            def send(_job: Job) -> dict[str, Any]:
                with self._lock:
                    session = self.sessions.get(target)
                if session is None:
                    raise RuntimeError(f"Session is stale or missing: {target}")
                return direct(session, payload, timeout=timeout)

            wrapped = self._wrap_job_body(send, True)
            return dispatcher.run(
                label,
                payload.get("id"),
                params,
                lambda _event: wrapped(dispatcher.current_job()),
                threading.Event(),
                kind="rpc",
            )

        def resolve_session(self, database=None):
            pinned = self._pinned_session()
            if pinned is None:
                with self._lock:
                    if database:
                        records = [
                            record for record in self._pending_matches_locked(database) if record.live()
                        ]
                    else:
                        record = self._pending.get(self._bound_session_locked())
                        records = [record] if record is not None and record.live() else []
                    if len(records) > 1:
                        raise RuntimeError(f"Database selector is ambiguous: {database}")
                    if records:
                        record = records[0]
                        raise RuntimeError(
                            f"Database '{record.session_id}' is still opening (job {record.job.id}, "
                            f"{record.phase}); poll idalib_job_status(job_id=...)"
                        )
                return super().resolve_session(database)
            with self._lock:
                session = self.sessions.get(pinned)
                if session is None:
                    raise RuntimeError(f"Session is stale or missing: {pinned}")
                session.last_accessed = datetime.now()
            if session.backend == "worker" and session.owned and not session.is_alive():
                raise RuntimeError(f"Worker for session '{pinned}' is not running")
            return session

        def bind_context(self, context_id: str, session_id: str) -> None:
            if self._pinned_session() is not None:
                return
            with self._lock:
                record = self._pending.get(session_id) if isinstance(session_id, str) else None
                if record is not None and context_id not in record.previous_bindings:
                    record.previous_bindings[context_id] = self.context_bindings.get(context_id)
                super().bind_context(context_id, session_id)

        # --- registration, close, shutdown ----------------------------------

        def _register_session_locked(self, session, resolved_path, context_id) -> None:
            super()._register_session_locked(session, resolved_path, context_id)
            if session.session_id not in self._dispatchers:
                self._dispatchers[session.session_id] = WorkerDispatcher(session.session_id)

        def _unregister_session_locked(self, session_id: str):
            session = super()._unregister_session_locked(session_id)
            if session is not None:
                dispatcher = self._dispatchers.pop(session_id, None)
                if dispatcher is not None:
                    dispatcher.close(f"Session closed: {session_id}")
                    self._prune_retired_locked()
                    self._retired.append((session_id, session.filename, dispatcher, time.monotonic()))
            return session

        def _prune_retired_locked(self) -> None:
            cutoff = time.monotonic() - self.RETIRED_TTL
            self._retired = [entry for entry in self._retired if entry[3] > cutoff]

        def close_session(self, session_id: str) -> bool:
            pinned = self._pinned_session()
            if pinned is not None:
                if pinned == session_id:
                    return super().close_session(session_id)
                raise RuntimeError("Cross-database close from a job body")
            closed: list[bool] = []

            def body(_job: Job) -> bool:
                closed.append(self.close_session(session_id))
                return closed[0]

            try:
                job = self.submit_job(
                    session_id,
                    "idalib_close",
                    None,
                    {"session_id": session_id},
                    body,
                    kind="close",
                    ensure_ready=False,
                )
            except LookupError:
                return False
            job.wait(None)
            status = job.status
            if status == "ok":
                return closed[0]
            error = job.info().get("error")
            if status == "error":
                exception = job.exception()
                if exception is not None:
                    raise exception
                raise QueueClosedError(error)
            raise CancelledError(error or "Cancelled")

        def shutdown(self) -> None:
            with self._lock:
                self._shut_down = True
                dispatchers = list(self._dispatchers.values())
                self._dispatchers.clear()
                records = list(self._pending.values())
                self._pending.clear()
                workers = [record.worker for record in records if record.worker is not None]
                workers.extend(
                    mark["worker"] for mark in self._reopens.values() if mark["worker"] is not None
                )
            for dispatcher in dispatchers:
                dispatcher.close("Supervisor shutting down")
            for worker in workers:
                self._dispose_worker(worker)
            super().shutdown()

        # --- job lookup and database state ----------------------------------

        def _locate_job(self, job_id: Any) -> tuple[Job, WorkerDispatcher] | None:
            if not isinstance(job_id, str):
                return None
            with self._lock:
                self._prune_retired_locked()
                candidates = list(self._dispatchers.values())
                candidates.extend(entry[2] for entry in reversed(self._retired))
            for dispatcher in candidates:
                job = dispatcher.job(job_id)
                if job is not None:
                    return job, dispatcher
            return None

        def find_job(self, job_id: Any) -> Job | None:
            found = self._locate_job(job_id)
            return None if found is None else found[0]

        def cancel_job(self, job_id: Any) -> str:
            found = self._locate_job(job_id)
            if found is None:
                return "unknown"
            job, dispatcher = found
            if job.kind != "open":
                return dispatcher.cancel(job_id)
            with self._lock:
                record = next((record for record in self._pending.values() if record.job is job), None)
                if record is None:
                    return "finished"
                flagged = not record.cancelled
                record.cancelled = True
            outcome = dispatcher.cancel(job_id)
            if outcome == "removed":
                self._finish_failed_open(record, "Cancelled")
            elif outcome == "finished" and flagged and job.status == "cancelled":
                return "cancelling"
            return outcome

        def database_state(self, session_id: Any) -> str:
            with self._lock:
                record = self._pending.get(session_id) if isinstance(session_id, str) else None
                if record is not None:
                    if record.dispatcher.draining():
                        return "closing"
                    return "spawning" if record.phase in ("discovering", "spawning") else "opening"
                if isinstance(session_id, str) and session_id in self.sessions:
                    dispatcher = self._dispatchers.get(session_id)
                    if dispatcher is not None and dispatcher.draining():
                        return "closing"
                    if session_id in self._reopens:
                        return "reopening"
                    return "ready"
                for retired_id, _filename, dispatcher, _retired_at in self._retired:
                    if retired_id == session_id and dispatcher.current_job() is not None:
                        return "closing"
                return "closed"

        def _registered_matches_locked(self, database: str) -> list[str]:
            """Apply upstream resolve_session's selector rules without liveness checks or reopen."""
            if database in self.sessions:
                return [database]
            try:
                mapped = self.path_to_session.get(self._path_key(database))
            except Exception:
                mapped = self.path_to_session.get(os.path.normcase(database))
            if mapped is not None:
                matches = [mapped]
            else:
                matches = [
                    s.session_id
                    for s in self.sessions.values()
                    if database in {s.session_id, s.filename, s.input_path}
                    or os.path.normcase(database) == os.path.normcase(s.input_path)
                ]
            if not matches:
                try:
                    normalized = os.path.normcase(str(Path(database).resolve()))
                except Exception:
                    normalized = os.path.normcase(database)
                matches = [
                    s.session_id
                    for s in self.sessions.values()
                    if os.path.normcase(s.input_path) == normalized
                ]
            return [sid for sid in matches if sid in self.sessions and sid in self._dispatchers]

        def job_databases(self, database: str | None, recent: int) -> list[dict[str, Any]]:
            if not isinstance(recent, int):
                raise TypeError("recent must be an int")
            with self._lock:
                self._prune_retired_locked()
                if database is None or database == "":
                    items = [
                        self._database_item_locked(sid, recent)
                        for sid in self.sessions
                        if sid in self._dispatchers
                    ]
                    items.extend(
                        self._pending_item_locked(record, recent) for record in self._pending.values()
                    )
                    return items + self._retired_items_locked(None, recent)
                records = self._pending_matches_locked(database)
                if len(records) > 1:
                    raise RuntimeError(f"Database selector is ambiguous: {database}")
                if records:
                    return [self._pending_item_locked(records[0], recent)]
                matches = self._registered_matches_locked(database)
                if len(matches) > 1:
                    raise RuntimeError(f"Database selector is ambiguous: {database}")
                if matches:
                    return [self._database_item_locked(matches[0], recent)]
                items = self._retired_items_locked(database, recent)
                if not items:
                    raise LookupError(f"Database/session not found: {database}")
                return items

        def _database_item_locked(self, sid: str, recent: int) -> dict[str, Any]:
            return {
                "database": sid,
                "filename": self.sessions[sid].filename,
                "state": self.database_state(sid),
                **self._dispatchers[sid].jobs_info(recent=recent),
            }

        def _pending_item_locked(self, record: PendingOpen, recent: int) -> dict[str, Any]:
            return {
                "database": record.session_id,
                "filename": record.filename,
                "state": self.database_state(record.session_id),
                **record.dispatcher.jobs_info(recent=recent),
            }

        def _retired_items_locked(self, database: str | None, recent: int) -> list[dict[str, Any]]:
            return [
                {
                    "database": sid,
                    "filename": filename,
                    "state": "closing" if dispatcher.current_job() is not None else "closed",
                    **dispatcher.jobs_info(recent=recent),
                }
                for sid, filename, dispatcher, _retired_at in self._retired
                if database is None or sid == database
            ]

        def list_sessions(self, context_id: str | None) -> list[dict[str, Any]]:
            with self._lock:
                entries = super().list_sessions(context_id)
                for entry in entries:
                    sid = entry["session_id"]
                    dispatcher = self._dispatchers.get(sid)
                    running = None if dispatcher is None else dispatcher.current_job()
                    entry["metadata"] = {
                        **entry["metadata"],
                        "state": self.database_state(sid),
                        "queued_jobs": 0 if dispatcher is None else dispatcher.queued_count(),
                        "running_job": None if running is None else running.info(),
                    }
                current = None if context_id is None else self.context_bindings.get(context_id)
                counts: dict[str, int] = {}
                for bound in self.context_bindings.values():
                    counts[bound] = counts.get(bound, 0) + 1
                entries.extend(
                    self._pending_entry_locked(record, current, counts) for record in self._pending.values()
                )
                return entries

        def _pending_entry_locked(
            self, record: PendingOpen, current: str | None, counts: dict[str, int]
        ) -> dict[str, Any]:
            sid = record.session_id
            worker = record.worker
            process = None if worker is None else worker.process
            pid = None if process is None else process.pid
            created = record.created_at.isoformat()
            running = record.dispatcher.current_job()
            return {
                "session_id": sid,
                "input_path": record.input_path,
                "filename": record.filename,
                "created_at": created,
                "last_accessed": created,
                "is_analyzing": record.phase in ("opening", "registering"),
                "metadata": {
                    "state": self.database_state(sid),
                    "queued_jobs": record.dispatcher.queued_count(),
                    "running_job": None if running is None else running.info(),
                },
                "is_active": process is not None and process.poll() is None,
                "is_current_context": current == sid,
                "bound_contexts": counts.get(sid, 0),
                "backend": "worker",
                "owned": True,
                "pid": pid,
                "worker_pid": pid,
            }

        # --- errors and worker processes ------------------------------------

        def _decorate_error(self, tool_name: str, response: dict[str, Any]) -> dict[str, Any]:
            result = response.get("result")
            if not isinstance(result, dict) or not result.get("isError"):
                return response
            schema = self._find_tool_schema(tool_name)
            if schema is None:
                return response
            content = list(result.get("content") or [])
            content.append({
                "type": "text",
                "text": "Tool schema:\n" + json.dumps(schema, indent=2),
            })
            result["content"] = content
            meta = dict(result.get("_meta") or {})
            meta["tool_schema"] = schema
            result["_meta"] = meta
            return response

        def _find_tool_schema(self, tool_name: str) -> dict[str, Any] | None:
            with self._lock:
                caches = list(self._tools_cache.values())
            for tools in caches:
                for tool in tools:
                    if tool.get("name") == tool_name:
                        return tool.get("inputSchema")
            return None

        def _spawn_worker(self):
            worker = self._start_worker_process()
            try:
                self._wait_worker_ready(worker)
            except Exception:
                self._terminate_worker(worker)
                raise
            return worker

        def _start_worker_process(self) -> Any:
            """Start a worker process and return its session without waiting for it to be ready."""
            port = self._pick_port()
            cmd = [
                sys.executable,
                "-m",
                "idalib_mcp.worker",
            ]
            if self.ida_home is not None:
                cmd.extend(["--ida-home", str(self.ida_home)])
            # Let the worker watch us and shut down cleanly if we disappear.
            cmd.extend(["--supervisor-pid", str(os.getpid())])
            cmd.extend(
                [
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    *self.worker_args,
                ]
            )

            logger.info("Spawning idalib worker on 127.0.0.1:%d", port)
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=None if self.show_worker_io else subprocess.DEVNULL,
                stderr=None if self.show_worker_io else subprocess.DEVNULL,
                env=os.environ.copy(),
            )
            if self._worker_job is not None and not assign_process_to_job(
                self._worker_job, process
            ):
                logger.warning(
                    "Could not assign worker pid %s to the kill-on-close job; "
                    "relying on the parent-death watchdog instead.",
                    process.pid,
                )
            return upstream.WorkerSession(
                session_id=f"__worker_schema_{uuid.uuid4().hex[:8]}",
                input_path="",
                filename="",
                host="127.0.0.1",
                port=port,
                process=process,
                backend="worker",
                owned=True,
                pid=process.pid,
            )

    return ManagedIdalibSupervisor


def _snapshot_instances(supervisor: Any) -> dict[str, Any]:
    with supervisor._lock:
        binding_counts: dict[str, int] = {}
        for session_id in supervisor.context_bindings.values():
            binding_counts[session_id] = binding_counts.get(session_id, 0) + 1

        sessions = [
            session.to_list_dict(current=False, bound_contexts=binding_counts.get(session.session_id, 0))
            for session in supervisor.sessions.values()
        ]

        owned_workers = sum(
            1
            for session in supervisor.sessions.values()
            if session.backend == "worker" and session.owned and session.is_alive()
        )

        return {
            "sessions": sessions,
            "count": len(sessions),
            "owned_workers": owned_workers,
            "max_workers": supervisor.max_workers,
            "isolated_contexts": supervisor.isolated_contexts,
            "ida_home": str(supervisor.ida_home) if getattr(supervisor, "ida_home", None) else None,
        }


def _save_session_before_close(supervisor: Any, session_id: str) -> dict[str, Any]:
    session = supervisor.resolve_session(session_id)
    tool_name = "idb_save" if session.backend == "gui" else "idalib_save"
    result = supervisor.call_worker_tool(session, tool_name, {"path": ""})
    if not isinstance(result, dict):
        raise RuntimeError("Unexpected save result")
    if not result.get("ok"):
        error = result.get("error") or "Save failed"
        raise RuntimeError(str(error))
    return result


def _build_request_handler(upstream):
    mcp_http_handler = upstream.McpServer.serve.__globals__["McpHttpRequestHandler"]

    class ManagementHttpRequestHandler(mcp_http_handler):  # type: ignore[misc, valid-type]
        @staticmethod
        def _history_snapshot(supervisor, session_id: str):
            dispatcher = supervisor._dispatchers.get(session_id)
            if dispatcher is None:
                return None
            return dispatcher.snapshot()

        @staticmethod
        def _history_payload(supervisor, session_id: str, payload_id: str):
            dispatcher = supervisor._dispatchers.get(session_id)
            if dispatcher is None:
                return None
            return dispatcher.get_payload(payload_id)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/instances"}:
                if not self._check_api_request():
                    return
                self._send_html(200, self._instances_html())
                return
            if parsed.path == "/api/instances":
                if not self._check_api_request():
                    return
                self._send_json(200, _snapshot_instances(self._supervisor()))
                return

            history_match = re.match(r"^/api/instances/([^/]+)/history$", parsed.path)
            if history_match:
                if not self._check_api_request():
                    return
                session_id = unquote(history_match.group(1))
                snapshot = self._history_snapshot(self._supervisor(), session_id)
                if snapshot is None:
                    self._send_json(404, {"error": f"No dispatcher for session: {session_id}"})
                    return
                self._send_json(200, snapshot)
                return

            payload_match = re.match(r"^/api/instances/([^/]+)/history/([^/]+)$", parsed.path)
            if payload_match:
                if not self._check_api_request():
                    return
                session_id = unquote(payload_match.group(1))
                payload_id = unquote(payload_match.group(2))
                payload = self._history_payload(self._supervisor(), session_id, payload_id)
                if payload is None:
                    self._send_json(404, {"error": "Payload not found or evicted"})
                    return
                body = payload.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(body)
                return

            super().do_GET()

        def do_POST(self):
            from .cancellation import set_active_socket
            set_active_socket(self.connection)
            try:
                self._do_POST_inner()
            finally:
                set_active_socket(None)

        def _do_POST_inner(self):
            parsed = urlparse(self.path)
            prefix = "/api/instances/"
            suffix = "/close"
            if parsed.path.startswith(prefix) and parsed.path.endswith(suffix):
                if not self._check_api_request():
                    return
                session_id = unquote(parsed.path[len(prefix) : -len(suffix)])
                if not session_id:
                    self._send_json(400, {"success": False, "error": "Missing session id"})
                    return
                supervisor = self._supervisor()
                options = self._read_json_body()
                save_requested = bool(options.get("save"))
                save_result = None
                if save_requested:
                    try:
                        save_result = _save_session_before_close(supervisor, session_id)
                    except KeyError:
                        self._send_json(404, {"success": False, "error": f"Session not found: {session_id}"})
                        return
                    except Exception as exc:
                        self._send_json(
                            500,
                            {
                                "success": False,
                                "saved": False,
                                "error": f"Save failed: {exc}",
                            },
                        )
                        return
                try:
                    closed = supervisor.close_session(session_id)
                except Exception as exc:
                    self._send_json(500, {"success": False, "error": str(exc)})
                    return
                status = 200 if closed else 404
                self._send_json(
                    status,
                    {
                        "success": bool(closed),
                        "saved": bool(save_result),
                        "save_result": save_result,
                        "message": f"Session closed: {session_id}" if closed else None,
                        "error": None if closed else f"Session not found: {session_id}",
                    },
                )
                return
            super().do_POST()

        def _supervisor(self):
            if upstream.supervisor is None:
                raise RuntimeError("idalib supervisor is not initialized")
            return upstream.supervisor

        def _send_json(self, status: int, payload: Any) -> None:
            body = _json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            body = self.rfile.read(length)
            if not body:
                return {}
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {}
            return payload if isinstance(payload, dict) else {}

        def _send_html(self, status: int, text: str) -> None:
            body = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Frame-Options", "DENY")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                "style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; form-action 'self'",
            )
            self.end_headers()
            self.wfile.write(body)

        def _instances_html(self) -> str:
            return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>idalib MCP Instances</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1d2433;
      --muted: #637083;
      --line: #d8dde6;
      --accent: #0f766e;
      --danger: #b42318;
      --danger-bg: #fff0ee;
      --ok-bg: #ecfdf3;
      --warn-bg: #fff8e6;
      --shadow: rgba(15, 23, 42, 0.08);
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --bg: #111318;
        --panel: #181c23;
        --text: #edf1f7;
        --muted: #9aa6b5;
        --line: #303744;
        --accent: #2dd4bf;
        --danger: #ff8a80;
        --danger-bg: #351c1a;
        --ok-bg: #123428;
        --warn-bg: #352b16;
        --shadow: rgba(0, 0, 0, 0.25);
      }
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", system-ui, sans-serif;
      font-size: 14px;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 16px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    h1 { margin: 0; font-size: 18px; font-weight: 650; }
    main { padding: 20px 24px; }
    .summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      box-shadow: 0 1px 2px var(--shadow);
      min-height: 72px;
    }
    .metric span { display: block; color: var(--muted); font-size: 12px; }
    .metric strong { display: block; margin-top: 6px; font-size: 20px; }
    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 10px;
    }
    .status { color: var(--muted); font-size: 13px; }
    button {
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--text);
      min-height: 32px;
      padding: 0 12px;
      cursor: pointer;
      font: inherit;
    }
    button:hover { border-color: var(--accent); }
    button.danger { color: var(--danger); background: var(--danger-bg); }
    button:disabled { cursor: not-allowed; opacity: 0.55; }
    .table-wrap {
      overflow-x: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 2px var(--shadow);
    }
    table { width: 100%; border-collapse: collapse; min-width: 900px; }
    th, td { padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: middle; }
    th { color: var(--muted); font-size: 12px; font-weight: 650; background: color-mix(in srgb, var(--panel), var(--bg) 45%); }
    tr:last-child td { border-bottom: 0; }
    code { font-family: Consolas, "SFMono-Regular", monospace; font-size: 12px; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      font-size: 12px;
      background: var(--warn-bg);
      color: var(--text);
      white-space: nowrap;
    }
    .pill.ok { background: var(--ok-bg); }
    .empty { padding: 28px; color: var(--muted); text-align: center; }
    .path { max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .modal-backdrop {
            position: fixed;
            inset: 0;
            display: grid;
            place-items: center;
            padding: 20px;
            background: rgba(0, 0, 0, 0.36);
            z-index: 10;
        }
        .modal-backdrop[hidden] { display: none; }
        .modal {
            width: min(420px, 100%);
            border: 1px solid var(--line);
            border-radius: 8px;
            background: var(--panel);
            box-shadow: 0 16px 40px var(--shadow);
            padding: 18px;
        }
        .modal h2 { margin: 0 0 8px; font-size: 16px; }
        .modal p { margin: 0 0 16px; color: var(--muted); line-height: 1.4; }
        .modal-actions { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; }
  </style>
</head>
<body>
  <header>
    <h1>idalib MCP Instances</h1>
    <button id="refresh" title="Refresh instance list">Refresh</button>
  </header>
  <main>
    <section class="summary" aria-label="Summary">
      <div class="metric"><span>Instances</span><strong id="count">0</strong></div>
      <div class="metric"><span>Workers</span><strong id="workers">0</strong></div>
      <div class="metric"><span>Limit</span><strong id="limit">0</strong></div>
      <div class="metric"><span>IDA Home</span><strong id="idaHome" style="font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">auto</strong></div>
    </section>
    <div class="toolbar">
      <div class="status" id="status">Loading</div>
    </div>
    <div class="table-wrap">
      <table aria-label="Instances">
        <thead>
          <tr>
            <th>Session</th>
            <th>File</th>
            <th>Backend</th>
            <th>PID</th>
            <th>State</th>
            <th>Bound</th>
            <th>Last Accessed</th>
            <th>Path</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
      <div class="empty" id="empty" hidden>No instances</div>
    </div>
  </main>
    <div class="modal-backdrop" id="closeDialog" hidden>
        <div class="modal" role="dialog" aria-modal="true" aria-labelledby="closeTitle">
            <h2 id="closeTitle">Close Instance</h2>
            <p>Save database changes before closing <code id="closeDialogSession">-</code>?</p>
            <div class="modal-actions">
                <button id="cancelClose">Cancel</button>
                <button class="danger" id="closeWithoutSave">Close without saving</button>
                <button id="saveAndClose">Save and close</button>
            </div>
        </div>
    </div>
    <div class="modal-backdrop" id="inspectDialog" hidden>
        <div class="modal" role="dialog" aria-modal="true" aria-labelledby="inspectTitle" style="width:min(720px,100%);max-height:80vh;display:flex;flex-direction:column;">
            <h2 id="inspectTitle">Inspect <code id="inspectSession">-</code></h2>
            <div style="overflow:auto;flex:1;">
                <h3 style="margin:8px 0;font-size:14px;">Current</h3>
                <div id="inspectCurrent" class="empty">Idle</div>
                <h3 style="margin:8px 0;font-size:14px;">Queued (<span id="inspectQueuedCount">0</span>)</h3>
                <div id="inspectQueued" class="empty">No queued requests</div>
                <h3 style="margin:8px 0;font-size:14px;">History (last 100)</h3>
                <div id="inspectHistory" class="empty">No history</div>
            </div>
            <div class="modal-actions">
                <button id="closeInspect">Close</button>
            </div>
        </div>
    </div>
  <script>
    const rows = document.getElementById('rows');
    const empty = document.getElementById('empty');
    const statusEl = document.getElementById('status');
    const refreshButton = document.getElementById('refresh');
        const closeDialog = document.getElementById('closeDialog');
        const closeDialogSession = document.getElementById('closeDialogSession');
        const cancelClose = document.getElementById('cancelClose');
        const closeWithoutSave = document.getElementById('closeWithoutSave');
        const saveAndClose = document.getElementById('saveAndClose');
        let pendingClose = null;

    function text(value) {
      return value === null || value === undefined || value === '' ? '-' : String(value);
    }

    function escapeHtml(value) {
      return text(value).replace(/[&<>"']/g, char => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      }[char]));
    }

    function fmtTime(value) {
      if (!value) return '-';
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
    }

    function sessionRow(session) {
      const tr = document.createElement('tr');
      const activeClass = session.is_active ? 'pill ok' : 'pill';
      const sessionId = escapeHtml(session.session_id);
      const inputPath = escapeHtml(session.input_path);
      tr.innerHTML = `
        <td><code>${sessionId}</code></td>
        <td>${escapeHtml(session.filename)}</td>
        <td>${escapeHtml(session.backend)}</td>
        <td>${escapeHtml(session.pid ?? session.worker_pid)}</td>
        <td><span class="${activeClass}">${session.is_active ? 'running' : 'stopped'}</span></td>
        <td>${escapeHtml(session.bound_contexts)}</td>
        <td>${escapeHtml(fmtTime(session.last_accessed))}</td>
        <td class="path" title="${inputPath}">${inputPath}</td>
        <td>
          <button data-inspect="${sessionId}">Inspect</button>
          <button class="danger" data-close="${sessionId}" ${session.owned === false ? 'disabled' : ''}>Close</button>
        </td>
      `;
      return tr;
    }

    async function refresh() {
      statusEl.textContent = 'Refreshing';
      const response = await fetch('/api/instances', {cache: 'no-store'});
      if (!response.ok) throw new Error(await response.text());
      const data = await response.json();
      document.getElementById('count').textContent = data.count ?? 0;
      document.getElementById('workers').textContent = data.owned_workers ?? 0;
      document.getElementById('limit').textContent = data.max_workers === 0 ? 'unlimited' : text(data.max_workers);
      document.getElementById('idaHome').textContent = data.ida_home || 'auto';
      rows.replaceChildren(...(data.sessions || []).map(sessionRow));
      empty.hidden = Boolean((data.sessions || []).length);
      statusEl.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    }

        function showCloseDialog(sessionId, button) {
            pendingClose = {sessionId, button};
            closeDialogSession.textContent = sessionId;
            closeDialog.hidden = false;
            saveAndClose.focus();
        }

        function hideCloseDialog() {
            closeDialog.hidden = true;
            pendingClose = null;
        }

        async function closeSession(sessionId, button, save) {
      button.disabled = true;
            statusEl.textContent = save ? `Saving ${sessionId}` : `Closing ${sessionId}`;
            const response = await fetch(`/api/instances/${encodeURIComponent(sessionId)}/close`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({save})
            });
      if (!response.ok) {
        const body = await response.text();
        throw new Error(body || response.statusText);
      }
      await refresh();
    }

    rows.addEventListener('click', event => {
      const button = event.target.closest('button[data-close]');
      if (!button) return;
            showCloseDialog(button.dataset.close, button);
        });
        cancelClose.addEventListener('click', hideCloseDialog);
        closeDialog.addEventListener('click', event => {
            if (event.target === closeDialog) hideCloseDialog();
        });
        document.addEventListener('keydown', event => {
            if (event.key === 'Escape' && !closeDialog.hidden) hideCloseDialog();
        });
        closeWithoutSave.addEventListener('click', () => {
            const request = pendingClose;
            if (!request) return;
            hideCloseDialog();
            closeSession(request.sessionId, request.button, false).catch(error => {
        statusEl.textContent = error.message;
                request.button.disabled = false;
      });
    });
        saveAndClose.addEventListener('click', () => {
            const request = pendingClose;
            if (!request) return;
            hideCloseDialog();
            closeSession(request.sessionId, request.button, true).catch(error => {
                statusEl.textContent = error.message;
                request.button.disabled = false;
            });
        });
    refreshButton.addEventListener('click', () => refresh().catch(error => { statusEl.textContent = error.message; }));
    refresh().catch(error => { statusEl.textContent = error.message; });
    setInterval(() => refresh().catch(error => { statusEl.textContent = error.message; }), 5000);

        const inspectDialog = document.getElementById('inspectDialog');
        const inspectSession = document.getElementById('inspectSession');
        const inspectCurrent = document.getElementById('inspectCurrent');
        const inspectQueued = document.getElementById('inspectQueued');
        const inspectQueuedCount = document.getElementById('inspectQueuedCount');
        const inspectHistory = document.getElementById('inspectHistory');
        const closeInspect = document.getElementById('closeInspect');
        let inspectInterval = null;
        let inspectingSession = null;

        function renderRecord(record) {
            const status = escapeHtml(record.status);
            const tool = escapeHtml(record.tool);
            const duration = record.duration_ms == null ? '-' : `${record.duration_ms.toFixed(1)} ms`;
            const argsPreview = escapeHtml(record.args_preview ?? '');
            const resultPreview = escapeHtml(record.result_preview ?? '');
            const argsLink = record.args_full_id
                ? `<a href="/api/instances/${encodeURIComponent(inspectingSession)}/history/${encodeURIComponent(record.args_full_id)}" target="_blank">view full</a>`
                : '';
            const resultLink = record.result_full_id
                ? `<a href="/api/instances/${encodeURIComponent(inspectingSession)}/history/${encodeURIComponent(record.result_full_id)}" target="_blank">view full</a>`
                : '';
            const errorText = record.error ? `<div style="color:var(--danger);">${escapeHtml(record.error)}</div>` : '';
            return `<div style="padding:8px;border:1px solid var(--line);border-radius:6px;margin-bottom:6px;">
              <div><strong>${tool}</strong> <span class="pill">${status}</span> <span style="color:var(--muted);">${duration}</span></div>
              <div style="margin-top:4px;font-size:12px;">args: <code>${argsPreview}</code> ${argsLink}</div>
              <div style="margin-top:4px;font-size:12px;">result: <code>${resultPreview}</code> ${resultLink}</div>
              ${errorText}
            </div>`;
        }

        async function refreshInspect() {
            if (!inspectingSession) return;
            try {
                const response = await fetch(`/api/instances/${encodeURIComponent(inspectingSession)}/history`, {cache: 'no-store'});
                if (!response.ok) {
                    inspectCurrent.innerHTML = '<div class="empty">No dispatcher for this session yet</div>';
                    inspectQueued.innerHTML = '';
                    inspectHistory.innerHTML = '';
                    inspectQueuedCount.textContent = '0';
                    return;
                }
                const data = await response.json();
                inspectCurrent.innerHTML = data.current ? renderRecord(data.current) : '<div class="empty">Idle</div>';
                inspectQueued.innerHTML = (data.queued || []).map(renderRecord).join('') || '<div class="empty">No queued requests</div>';
                inspectQueuedCount.textContent = (data.queued || []).length;
                inspectHistory.innerHTML = (data.history || []).slice().reverse().map(renderRecord).join('') || '<div class="empty">No history</div>';
            } catch (error) {
                inspectCurrent.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
            }
        }

        function openInspect(sessionId) {
            inspectingSession = sessionId;
            inspectSession.textContent = sessionId;
            inspectDialog.hidden = false;
            refreshInspect();
            inspectInterval = setInterval(refreshInspect, 5000);
        }

        function closeInspectDialog() {
            inspectDialog.hidden = true;
            inspectingSession = null;
            if (inspectInterval) {
                clearInterval(inspectInterval);
                inspectInterval = null;
            }
        }

        rows.addEventListener('click', event => {
            const button = event.target.closest('button[data-inspect]');
            if (!button) return;
            openInspect(button.dataset.inspect);
        });
        closeInspect.addEventListener('click', closeInspectDialog);
        inspectDialog.addEventListener('click', event => {
            if (event.target === inspectDialog) closeInspectDialog();
        });
        document.addEventListener('keydown', event => {
            if (event.key === 'Escape' && !inspectDialog.hidden) closeInspectDialog();
        });
  </script>
</body>
</html>"""

    return ManagementHttpRequestHandler


def _format_url_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _instance_ui_url(host: str, port: int) -> str:
    return f"http://{_format_url_host(host)}:{port}/instances"


def _bound_instance_ui_url(mcp: Any, fallback_host: str, fallback_port: int) -> str:
    server = getattr(mcp, "_http_server", None)
    if server is None:
        return _instance_ui_url(fallback_host, fallback_port)

    host, port = server.server_address[:2]
    display_host = fallback_host if host in ("", "0.0.0.0", "::") else str(host)
    return _instance_ui_url(display_host, int(port))


def _serve_stdio_instance_ui(upstream: Any, host: str, port: int, request_handler: type) -> str:
    try:
        upstream.mcp.serve(host=host, port=port, background=True, request_handler=request_handler)
    except OSError as exc:
        if port == 0:
            raise
        logger.warning(
            "Instance UI port %s is unavailable (%s); retrying with an ephemeral port",
            port,
            exc,
        )
        upstream.mcp.serve(host=host, port=0, background=True, request_handler=request_handler)
    return _bound_instance_ui_url(upstream.mcp, host, port)


def _prefer_management_tools_in_tool_list(upstream) -> None:
    def _handle_tools_list_management_first(request_obj: dict[str, Any]) -> dict[str, Any]:
        supervisor = upstream._require_supervisor()
        local_tools = upstream.mcp._mcp_tools_list().get("tools", [])
        local_names = {tool.get("name") for tool in local_tools}
        worker_tools = [
            tool
            for tool in supervisor.worker_tools()
            if tool.get("name") not in local_names
        ]
        return upstream._jsonrpc_result(
            request_obj.get("id"),
            {"tools": local_tools + worker_tools},
        )

    upstream._handle_tools_list = _handle_tools_list_management_first


def _enable_path_database_auto_open(upstream) -> None:
    def _resolve_or_open_path_database(supervisor, database: str | None):
        try:
            return supervisor.resolve_session(database)
        except Exception:
            if not database:
                raise
            candidate = Path(database).expanduser()
            if not candidate.exists():
                raise
            context_id = supervisor.resolve_context_id()
            return supervisor.open_session(str(candidate), context_id=context_id)

    def _handle_tools_call_with_path_open(request_obj: dict[str, Any]) -> dict[str, Any] | None:
        from .cancellation import get_active_socket, spawn_socket_watcher

        supervisor = upstream._require_supervisor()
        params = request_obj.get("params") or {}
        tool_name = params.get("name", "")
        request_id = request_obj.get("id")

        if tool_name in upstream.IDALIB_MANAGEMENT_TOOLS:
            return upstream._original_dispatch(request_obj)
        if tool_name in upstream.IDALIB_HIDDEN_PLUGIN_TOOLS:
            return upstream._jsonrpc_result(
                request_id,
                upstream._call_tool_result(
                    {
                        "error": (
                            f"{tool_name} is a GUI-plugin routing tool and is not "
                            "available through idalib-mcp. Use idalib_list or "
                            "idalib_switch instead."
                        )
                    },
                    is_error=True,
                ),
            )

        arguments = dict(params.get("arguments") or {})
        database = arguments.pop("database", None)
        try:
            session = _resolve_or_open_path_database(supervisor, database)
        except Exception as exc:
            return upstream._jsonrpc_result(
                request_id,
                upstream._call_tool_result({"error": str(exc)}, is_error=True),
            )

        forwarded = dict(request_obj)
        forwarded["params"] = dict(params)
        forwarded["params"]["arguments"] = arguments

        handle = supervisor._cancels.register(request_id) if request_id is not None else None
        sock = get_active_socket()
        stop = threading.Event()
        watcher = spawn_socket_watcher(handle, sock, stop) if (handle and sock) else None
        try:
            return supervisor.forward_raw(session, forwarded)
        except CancelledError:
            return upstream._jsonrpc_result(
                request_id,
                upstream._call_tool_result({"error": "Request cancelled"}, is_error=True),
            )
        except Exception as exc:
            return upstream._jsonrpc_result(
                request_id,
                upstream._call_tool_result({"error": str(exc)}, is_error=True),
            )
        finally:
            stop.set()
            if watcher is not None:
                watcher.join(timeout=1.0)
            if handle is not None and request_id is not None:
                supervisor._cancels.unregister(request_id)

    upstream._handle_tools_call = _handle_tools_call_with_path_open


def _register_open_ui_tool(upstream, *, host: str, port: int) -> None:
    def idalib_open_ui() -> dict:
        """Return the bound management UI URL."""
        return {"url": _bound_instance_ui_url(upstream.mcp, host, port)}

    idalib_open_ui.__doc__ = "Return the bound management UI URL."
    upstream.mcp.tool(idalib_open_ui)
    upstream.IDALIB_MANAGEMENT_TOOLS.add("idalib_open_ui")


def _wrap_dispatch_supervisor(upstream) -> None:
    original = upstream.mcp.registry.dispatch

    def wrapped(request):
        request_obj = request
        if not isinstance(request, dict):
            try:
                request_obj = json.loads(request)
            except Exception:
                request_obj = None
        if isinstance(request_obj, dict) and request_obj.get("method") == "notifications/cancelled":
            params = request_obj.get("params") or {}
            target = params.get("requestId")
            if target is not None and upstream.supervisor is not None:
                upstream.supervisor._cancels.cancel(target)
            return None
        return original(request)

    upstream.mcp.registry.dispatch = wrapped


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless MCP supervisor for IDA Pro via idalib")
    parser.add_argument(
        "--install",
        nargs="?",
        const="",
        default=None,
        metavar="TARGETS",
        help="Install MCP client config entries. Optionally pass comma-separated targets.",
    )
    parser.add_argument(
        "--uninstall",
        nargs="?",
        const="",
        default=None,
        metavar="TARGETS",
        help="Remove MCP client config entries. Optionally pass comma-separated targets.",
    )
    parser.add_argument("--config", action="store_true", help="Print example MCP config JSON")
    parser.add_argument("--list-clients", action="store_true", help="List supported MCP client config targets")
    parser.add_argument(
        "--transport",
        type=str,
        default=None,
        help="For install/config: stdio, streamable-http, sse, or URL. For running: stdio or URL.",
    )
    parser.add_argument(
        "--scope",
        type=str,
        choices=["global", "project"],
        default=None,
        help="Installation scope: project (default for explicit targets) or global.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show debug messages")
    parser.add_argument("--stdio", action="store_true", help="Serve MCP over stdio instead of HTTP")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="HTTP host, default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8745, help="HTTP port, default: 8745")
    parser.add_argument(
        "--ida-home",
        type=Path,
        default=default_ida_home_from_env(),
        help="IDA installation directory. Overrides idapro config for worker processes.",
    )
    parser.add_argument(
        "--isolated-contexts",
        action="store_true",
        help="Enable strict per-transport database binding isolation.",
    )
    parser.add_argument("--unsafe", action="store_true", help="Enable unsafe worker tools, including debugger tools")
    parser.add_argument("--profile", type=Path, default=None, metavar="PATH", help="Restrict worker tools to a profile file")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("IDA_MCP_MAX_WORKERS", "4")),
        help="Maximum simultaneous worker databases (0 = unlimited, default: 4)",
    )
    parser.add_argument(
        "--show-worker-io",
        action="store_true",
        help="Inherit worker stdout/stderr instead of suppressing it",
    )
    parser.add_argument("input_path", type=Path, nargs="?", help="Optional binary to open on startup")
    args = parser.parse_args()

    if args.list_clients:
        list_available_clients()
        return

    is_install = args.install is not None
    is_uninstall = args.uninstall is not None
    if args.scope and not (is_install or is_uninstall):
        raise SystemExit("--scope requires --install or --uninstall")
    if is_install and is_uninstall:
        raise SystemExit("Cannot install and uninstall at the same time")
    if is_install or is_uninstall:
        run_install_command(
            uninstall=is_uninstall,
            targets_str=args.install if is_install else args.uninstall,
            args=args,
        )
        return
    if args.config:
        print_mcp_config(args)
        return

    if args.transport is not None:
        if args.transport == "stdio":
            args.stdio = True
        else:
            transport_url = urlparse(args.transport)
            if transport_url.hostname is None or transport_url.port is None:
                raise SystemExit(f"Invalid transport URL: {args.transport}")
            args.host = transport_url.hostname
            args.port = transport_url.port

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    ida_home = validate_ida_home(args.ida_home) if args.ida_home is not None else None
    upstream = _load_upstream_supervisor()
    managed_supervisor_class = _build_managed_supervisor_class(upstream)

    worker_args: list[str] = []
    if args.verbose:
        worker_args.append("--verbose")
    if args.unsafe:
        worker_args.append("--unsafe")
    if args.profile is not None:
        worker_args.extend(["--profile", str(args.profile)])

    upstream.supervisor = managed_supervisor_class(
        upstream.mcp,
        isolated_contexts=args.isolated_contexts,
        max_workers=args.max_workers,
        worker_args=worker_args,
        ida_home=ida_home,
        show_worker_io=args.show_worker_io,
    )
    _prefer_management_tools_in_tool_list(upstream)
    _enable_path_database_auto_open(upstream)
    _register_open_ui_tool(upstream, host=args.host, port=args.port)
    upstream.mcp.registry.dispatch = upstream.dispatch_supervisor
    _wrap_dispatch_supervisor(upstream)
    upstream.mcp.require_streamable_http_session = args.isolated_contexts

    if args.input_path is not None:
        startup_context_id = (
            upstream.STDIO_DEFAULT_CONTEXT_ID if args.isolated_contexts else upstream.SHARED_FALLBACK_CONTEXT_ID
        )
        try:
            upstream.supervisor.open_session(str(args.input_path), context_id=startup_context_id)
        except Exception as exc:
            raise SystemExit(f"Failed to open initial binary: {exc}") from exc

    def cleanup_and_exit(signum, frame):
        logger.info("Shutting down idalib supervisor")
        if upstream.supervisor is not None:
            upstream.supervisor.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, cleanup_and_exit)
    signal.signal(signal.SIGTERM, cleanup_and_exit)

    try:
        handler = _build_request_handler(upstream)
        if args.stdio:
            ui_url = _serve_stdio_instance_ui(upstream, args.host, args.port, handler)
            logger.info("Instance UI: %s", ui_url)
            print(f"Instance UI: {ui_url}", file=sys.stderr, flush=True)
            upstream.mcp.stdio()
        else:
            logger.info("Instance UI: %s", _instance_ui_url(args.host, args.port))
            stop = threading.Event()

            def threaded_cleanup(signum, frame):
                stop.set()

            signal.signal(signal.SIGINT, threaded_cleanup)
            signal.signal(signal.SIGTERM, threaded_cleanup)

            upstream.mcp.serve(
                host=args.host, port=args.port, background=True, request_handler=handler,
            )
            try:
                stop.wait()
            finally:
                upstream.mcp.stop()
    finally:
        if upstream.supervisor is not None:
            upstream.supervisor.shutdown()


if __name__ == "__main__":
    main()
