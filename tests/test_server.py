from __future__ import annotations

import http.client
import importlib
import itertools
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import mock

from fakeworker import FakeWorker, FakeWorkerFactory, attach_fakes, make_supervisor, preserved_upstream
from idalib_mcp.dispatcher import QueueClosedError, WorkerDispatcher
from idalib_mcp.server import (
    _bound_instance_ui_url,
    _install_supervisor,
    _instance_ui_url,
    _prefer_management_tools_in_tool_list,
    _save_session_before_close,
    _serve_stdio_instance_ui,
    main,
)


class FakeMcp:
    class _Registry:
        def __init__(self):
            self.dispatch = lambda req: None

    def __init__(self) -> None:
        self._http_server: FakeHttpServer | None = None
        self.fail_ports: set[int] = set()
        self.serve_calls: list[tuple[str, int, bool, type]] = []
        self.registry = self._Registry()

    def _mcp_tools_list(self) -> dict[str, Any]:
        return {
            "tools": [
                {"name": "idalib_open"},
                {"name": "idalib_list"},
            ]
        }

    def serve(self, *, host: str, port: int, background: bool, request_handler: type) -> None:
        self.serve_calls.append((host, port, background, request_handler))
        if port in self.fail_ports:
            raise OSError("address already in use")
        bound_port = 18745 if port == 0 else port
        self._http_server = FakeHttpServer(host, bound_port)


class FakeHttpServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 18745) -> None:
        self.server_address = (host, port)


class FakeSession:
    def __init__(self, backend: str = "") -> None:
        self.backend = backend
        self.session_id = ""


class FakeSupervisor:
    def __init__(self) -> None:
        from idalib_mcp.cancellation import CancellationRegistry
        self.sessions: dict[str, FakeSession] = {}
        self.saved: tuple[FakeSession, str, dict[str, Any]] | None = None
        self.save_result: dict[str, Any] = {"ok": True, "path": "sample.i64"}
        self._cancels = CancellationRegistry()

    def worker_tools(self) -> list[dict[str, Any]]:
        return [
            {"name": "decompile"},
            {"name": "idalib_open"},
        ]

    def resolve_session(self, database: str | None) -> Any:
        if database in self.sessions:
            return self.sessions[database]
        if database == "existing-session":
            return "existing-session"
        raise KeyError(database)

    def call_worker_tool(self, session: FakeSession, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.saved = (session, tool_name, arguments)
        return self.save_result


class FakeIdalibSupervisor:
    """Minimal base class that mimics upstream.IdalibSupervisor for tests."""

    def __init__(self, *args, isolated_contexts: bool = False, max_workers: int = 4, **kwargs):
        import threading
        self._lock = threading.RLock()
        self.isolated_contexts = isolated_contexts
        self.max_workers = max_workers
        self.sessions: dict = {}
        self.worker_args = list(kwargs.get("worker_args") or [])
        self._tools_cache: dict = {}

    def close_session(self, session_id):
        return False

    def _worker_request_path(self):
        return "/mcp"

    def forward_raw(self, worker, request_obj):
        return self._worker_rpc(worker, request_obj)

    def _worker_rpc(self, worker, request_obj, *, timeout=None):
        raise NotImplementedError


class FakeUpstream:
    IDALIB_MANAGEMENT_TOOLS = {"idalib_open", "idalib_list"}
    IDALIB_HIDDEN_PLUGIN_TOOLS = {"jump_to_address"}

    IdalibSupervisor = FakeIdalibSupervisor

    def __init__(self) -> None:
        self.mcp = FakeMcp()
        self.supervisor = FakeSupervisor()
        self._original_dispatch_called = False

    def _require_supervisor(self) -> FakeSupervisor:
        return self.supervisor

    def _jsonrpc_result(self, request_id: int, result: Any) -> dict[str, Any]:
        return {"id": request_id, "result": result}

    def _call_tool_result(self, payload: Any, *, is_error: bool = False) -> dict[str, Any]:
        return {"payload": payload, "is_error": is_error}

    def _original_dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        self._original_dispatch_called = True
        return {"dispatched": request}


class ServerIntegrationPatchTests(unittest.TestCase):
    def test_instance_ui_url_uses_bound_http_address(self) -> None:
        mcp = FakeMcp()
        mcp._http_server = FakeHttpServer()

        self.assertEqual(_instance_ui_url("127.0.0.1", 8745), "http://127.0.0.1:8745/instances")
        self.assertEqual(_instance_ui_url("::1", 8745), "http://[::1]:8745/instances")
        self.assertEqual(_bound_instance_ui_url(mcp, "localhost", 0), "http://127.0.0.1:18745/instances")

    def test_stdio_instance_ui_retries_with_ephemeral_port(self) -> None:
        upstream = FakeUpstream()
        upstream.mcp.fail_ports.add(8745)

        with self.assertLogs("idalib_mcp.server", level="WARNING"):
            url = _serve_stdio_instance_ui(upstream, "127.0.0.1", 8745, FakeHttpServer)

        self.assertEqual(url, "http://127.0.0.1:18745/instances")
        self.assertEqual([call[1] for call in upstream.mcp.serve_calls], [8745, 0])
        self.assertTrue(all(call[2] for call in upstream.mcp.serve_calls))

    def test_management_tools_are_listed_before_worker_tools(self) -> None:
        upstream = FakeUpstream()

        _prefer_management_tools_in_tool_list(upstream)
        result = upstream._handle_tools_list({"id": 1})

        self.assertEqual(
            [tool["name"] for tool in result["result"]["tools"]],
            ["idalib_open", "idalib_list", "decompile"],
        )

    def test_save_session_before_close_uses_worker_save_tool(self) -> None:
        supervisor = FakeSupervisor()
        session = FakeSession("worker")
        supervisor.sessions["sample"] = session

        result = _save_session_before_close(supervisor, "sample")

        self.assertEqual(result, {"ok": True, "path": "sample.i64"})
        self.assertEqual(supervisor.saved, (session, "idalib_save", {"path": ""}))

    def test_save_session_before_close_uses_gui_save_tool(self) -> None:
        supervisor = FakeSupervisor()
        session = FakeSession("gui")
        supervisor.sessions["sample"] = session

        _save_session_before_close(supervisor, "sample")

        self.assertEqual(supervisor.saved, (session, "idb_save", {"path": ""}))


class SupervisorDispatcherIntegrationTests(unittest.TestCase):
    def _build_supervisor(self):
        from idalib_mcp.server import _build_managed_supervisor_class

        upstream = FakeUpstream()
        cls = _build_managed_supervisor_class(upstream)
        supervisor = cls(FakeMcp(), worker_args=[])
        return supervisor

    def test_error_response_is_decorated_with_tool_schema(self) -> None:
        supervisor = self._build_supervisor()

        # Seed the cache the supervisor reads schemas from.
        supervisor._tools_cache[()] = [{
            "name": "decompile",
            "inputSchema": {
                "type": "object",
                "properties": {"addr": {"type": "string"}},
                "required": ["addr"],
            },
        }]

        response = {
            "jsonrpc": "2.0", "id": 1,
            "result": {"content": [{"type": "text", "text": "bad args"}],
                       "isError": True, "structuredContent": {}},
        }

        decorated = supervisor._decorate_error("decompile", response)
        meta = decorated["result"].get("_meta") or {}
        self.assertEqual(meta.get("tool_schema", {}).get("required"), ["addr"])
        texts = [c.get("text", "") for c in decorated["result"]["content"]]
        self.assertTrue(any("Tool schema" in t for t in texts))


_WAIT = 5.0


def _wait_until(predicate: Any, timeout: float = _WAIT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


class SupervisorQueueCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.temp_path = Path(temp_dir.name)
        self.upstream = importlib.import_module("ida_pro_mcp.idalib_supervisor")
        self.factory = FakeWorkerFactory(self.upstream)
        self.supervisor = make_supervisor(self, self.upstream, self.factory, sync_wait=0.1)
        wait_ready = self.supervisor._wait_worker_ready
        # Upstream retries for 120 s; a misrouted ready ping must fail the test, not stall the suite.
        self.supervisor._wait_worker_ready = lambda worker, timeout=_WAIT: wait_ready(worker, timeout=_WAIT)

    def _open(self, name: str) -> Any:
        path = self.temp_path / name
        path.write_bytes(b"MZ")
        return self.supervisor.open_session(str(path), context_id=self.upstream.SHARED_FALLBACK_CONTEXT_ID)

    def _worker_of(self, session: Any) -> FakeWorker:
        return next(worker for worker in self.factory.workers if worker.port == session.port)

    def _tool_body(self, name: str, arguments: dict[str, Any]) -> Any:
        supervisor = self.supervisor

        def body(_job: Any) -> Any:
            return supervisor.call_worker_tool(supervisor.resolve_session(), name, arguments)

        return body

    def _submit_slow(self, session: Any, seconds: float) -> Any:
        arguments = {"seconds": seconds}
        job = self.supervisor.submit_job(
            session.session_id, "slow_tool", 1, arguments, self._tool_body("slow_tool", arguments)
        )
        self.assertTrue(_wait_until(lambda: "slow_tool" in self._worker_of(session).call_names()))
        return job

    def _queued(self, dispatcher: WorkerDispatcher) -> list[tuple[str, str]]:
        return [(job["tool"], job["kind"]) for job in dispatcher.jobs_info()["queued"]]

    def _call(self, worker: FakeWorker, name: str) -> Any:
        return next(call for call in worker.calls() if (call.name or call.method) == name)

    def test_resources_read_waits_behind_slow_tool(self) -> None:
        session = self._open("a.exe")
        worker = self._worker_of(session)
        dispatcher = self.supervisor._dispatchers[session.session_id]
        request = {"jsonrpc": "2.0", "id": 5, "method": "resources/read", "params": {"uri": "ida://fake"}}
        responses: list[Any] = []
        with preserved_upstream(self.upstream):
            self.upstream.supervisor = self.supervisor
            slow = self._submit_slow(session, 0.8)
            queued = self.supervisor.submit_job(session.session_id, "echo", 2, {}, self._tool_body("echo", {}))
            reader = threading.Thread(
                target=lambda: responses.append(self.upstream._handle_resources_read(request)), daemon=True
            )
            reader.start()
            expected_queue = [("echo", "tool"), ("resources/read", "rpc")]
            self.assertTrue(_wait_until(lambda: self._queued(dispatcher) == expected_queue))
            self.assertFalse(slow.is_done())
            self.assertNotIn("resources/read", worker.call_names())
            reader.join(_WAIT)
            self.assertFalse(reader.is_alive())
            self.assertTrue(queued.wait(_WAIT))

            contents = [{"uri": "ida://fake", "mimeType": "application/json", "text": "{}"}]
            self.assertEqual(responses, [{"jsonrpc": "2.0", "result": {"contents": contents}, "id": 5}])
            self.assertEqual((slow.status, queued.status), ("ok", "ok"))
            # A read that bypassed the queue would connect during slow_tool and be accepted before echo.
            self.assertEqual(worker.call_names()[-3:], ["slow_tool", "echo", "resources/read"])

    def test_cancelled_queued_job_never_reaches_worker(self) -> None:
        session = self._open("a.exe")
        worker = self._worker_of(session)
        sid = session.session_id
        slow = self._submit_slow(session, 0.5)
        doomed_args = {"which": "doomed"}
        after_args = {"which": "after"}
        doomed = self.supervisor.submit_job(sid, "echo", 2, doomed_args, self._tool_body("echo", doomed_args))
        after = self.supervisor.submit_job(sid, "echo", 3, after_args, self._tool_body("echo", after_args))

        self.assertEqual(self.supervisor.cancel_job(doomed.id), "removed")
        self.assertTrue(after.wait(_WAIT))

        self.assertEqual((slow.status, doomed.status, after.status), ("ok", "cancelled", "ok"))
        self.assertEqual([call.arguments for call in worker.calls() if call.name == "echo"], [after_args])
        self.assertEqual(self.supervisor.cancel_job(doomed.id), "finished")
        self.assertEqual(self.supervisor.cancel_job("no-such-job"), "unknown")
        self.assertEqual(self.supervisor.cancel_job(7), "unknown")

    def test_cross_database_worker_rpc_raises(self) -> None:
        a = self._open("a.exe")
        b = self._open("b.exe")
        b_worker = self._worker_of(b)
        supervisor = self.supervisor
        payload = {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"from": "a"}},
        }

        def body(_job: Any) -> Any:
            return supervisor._worker_rpc(supervisor.sessions[b.session_id], payload)

        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            job = supervisor.submit_job(a.session_id, "cross", None, {}, body)
            self.assertTrue(job.wait(_WAIT))

        self.assertEqual(job.status, "error")
        self.assertIsInstance(job.exception(), RuntimeError)
        self.assertEqual(str(job.exception()), "Cross-database worker RPC from a job body")
        self.assertNotIn("echo", b_worker.call_names())
        b_jobs = supervisor._dispatchers[b.session_id].jobs_info()["recent"]
        self.assertEqual([(j["tool"], j["kind"]) for j in b_jobs], [("idalib_open", "open")])

    def test_unregister_unknown_id_keeps_pending_dispatcher(self) -> None:
        pending = WorkerDispatcher("pending-id")
        self.supervisor._dispatchers["pending-id"] = pending

        with self.supervisor._lock:
            result = self.supervisor._unregister_session_locked("pending-id")

        self.assertIsNone(result)
        self.assertIs(self.supervisor._dispatchers["pending-id"], pending)
        self.assertFalse(pending.closed)
        self.assertEqual(self.supervisor.database_state("pending-id"), "closed")

    def test_worker_rpc_from_transport_thread_queues_as_rpc_job(self) -> None:
        session = self._open("a.exe")
        worker = self._worker_of(session)
        dispatcher = self.supervisor._dispatchers[session.session_id]
        slow = self._submit_slow(session, 0.8)
        request = {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"addr": "0x1000"}},
        }
        responses: list[Any] = []
        caller = threading.Thread(
            target=lambda: responses.append(self.supervisor.forward_raw(session, request)), daemon=True
        )
        caller.start()

        self.assertTrue(_wait_until(lambda: self._queued(dispatcher) == [("echo", "rpc")]))
        self.assertEqual(list(self.supervisor._dispatchers), [session.session_id])
        self.assertIs(self.supervisor._dispatchers[session.session_id], dispatcher)
        caller.join(_WAIT)
        self.assertFalse(caller.is_alive())

        self.assertEqual(responses[0]["id"], 7)
        echoed = {"tool": "echo", "arguments": {"addr": "0x1000"}}
        self.assertEqual(responses[0]["result"]["structuredContent"], echoed)
        self.assertEqual(slow.status, "ok")
        self.assertEqual(
            [(j["tool"], j["kind"], j["status"]) for j in dispatcher.jobs_info(recent=2)["recent"]],
            [("echo", "rpc", "ok"), ("slow_tool", "tool", "ok")],
        )
        self.assertGreaterEqual(self._call(worker, "echo").start, self._call(worker, "slow_tool").end)

    def test_worker_rpc_to_unregistered_worker_goes_direct(self) -> None:
        session = self._open("a.exe")
        dispatcher = self.supervisor._dispatchers[session.session_id]
        slow = self._submit_slow(session, 1.0)
        spare = self.supervisor._spawn_worker()
        spare_worker = self._worker_of(spare)
        supervisor = self.supervisor

        response = supervisor._worker_rpc(spare, {"jsonrpc": "2.0", "id": 3, "method": "ping"}, timeout=2.0)

        self.assertFalse(slow.is_done())
        self.assertEqual(response, {"jsonrpc": "2.0", "result": {}, "id": 3})
        self.assertEqual(list(supervisor._dispatchers), [session.session_id])
        self.assertEqual(dispatcher.jobs_info()["queued"], [])

        job = supervisor.submit_job(
            session.session_id,
            "spare_ping",
            None,
            {},
            lambda _job: supervisor._worker_rpc(spare, {"jsonrpc": "2.0", "id": 4, "method": "ping"}),
        )
        self.assertTrue(job.wait(_WAIT))
        self.assertEqual(job.status, "ok")
        self.assertEqual(spare_worker.call_names(), ["ping", "ping", "ping"])
        self.assertEqual(list(supervisor._dispatchers), [session.session_id])

    def test_close_session_retires_dispatcher(self) -> None:
        session = self._open("a.exe")
        sid = session.session_id
        worker = self._worker_of(session)
        dispatcher = self.supervisor._dispatchers[sid]
        earlier = self.supervisor.submit_job(sid, "echo", 1, {}, self._tool_body("echo", {}))
        self.assertTrue(earlier.wait(_WAIT))

        self.assertTrue(self.supervisor.close_session(sid))

        self.assertNotIn(sid, self.supervisor._dispatchers)
        self.assertNotIn(sid, self.supervisor.sessions)
        self.assertTrue(dispatcher.closed)
        self.assertEqual(dispatcher.close_reason, f"Session closed: {sid}")
        self.assertIs(self.supervisor.find_job(earlier.id), earlier)
        self.assertEqual(self.supervisor.database_state(sid), "closed")
        databases = self.supervisor.job_databases(sid, 5)
        self.assertEqual(
            [(d["database"], d["filename"], d["state"]) for d in databases], [(sid, "a.exe", "closed")]
        )
        self.assertEqual(
            [(j["tool"], j["kind"], j["status"]) for j in databases[0]["recent"]],
            [("idalib_close", "close", "ok"), ("echo", "tool", "ok"), ("idalib_open", "open", "ok")],
        )
        self.assertEqual(worker.call_names()[-1], "idalib_close")
        self.assertEqual(worker.process.poll(), -9)
        with self.assertRaises(LookupError):
            self.supervisor.submit_job(sid, "echo", 2, {}, self._tool_body("echo", {}))
        self.assertFalse(self.supervisor.close_session(sid))
        self.assertEqual(self.supervisor.list_sessions(None), [])


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


_LIST_KEYS = [
    "session_id",
    "input_path",
    "filename",
    "created_at",
    "last_accessed",
    "is_analyzing",
    "metadata",
    "is_active",
    "is_current_context",
    "bound_contexts",
    "backend",
    "owned",
    "pid",
    "worker_pid",
]
_MAX_TEXT = (
    "Maximum idalib worker count reached (1). "
    "Close a database with idalib_close or increase --max-workers."
)


class SupervisorOpenPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.temp_path = Path(temp_dir.name)
        self.upstream = importlib.import_module("ida_pro_mcp.idalib_supervisor")
        self.ctx = self.upstream.SHARED_FALLBACK_CONTEXT_ID

    def _make(self, max_workers: int = 4, **factory_kwargs: Any) -> tuple[Any, FakeWorkerFactory]:
        factory = FakeWorkerFactory(self.upstream, **factory_kwargs)
        supervisor = make_supervisor(self, self.upstream, factory, max_workers=max_workers, sync_wait=0.1)
        wait_ready = supervisor._wait_worker_ready
        supervisor._wait_worker_ready = lambda worker, timeout=_WAIT: wait_ready(worker, timeout=_WAIT)
        return supervisor, factory

    def _path(self, name: str) -> str:
        path = self.temp_path / name
        path.write_bytes(b"MZ")
        return str(path.resolve())

    def _worker_of(self, factory: FakeWorkerFactory, session: Any) -> FakeWorker:
        return next(worker for worker in factory.workers if worker.port == session.port)

    def _opening(self, factory: FakeWorkerFactory) -> bool:
        return bool(factory.workers) and "idalib_open" in factory.workers[-1].call_names()

    def _held_gate(self) -> threading.Event:
        gate = threading.Event()
        self.addCleanup(gate.set)
        return gate

    def test_opening_database_listed_as_opening(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(open_gate=gate)
        self.addCleanup(gate.set)
        path = self._path("a.exe")
        submission = supervisor.submit_open(path, context_id=self.ctx)
        sid = submission.session_id
        self.assertTrue(_wait_until(lambda: self._opening(factory)))
        request = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "idalib_list", "arguments": {}},
        }
        with preserved_upstream(self.upstream):
            self.upstream.supervisor = supervisor
            started = time.monotonic()
            listing = self.upstream._original_dispatch(request)["result"]["structuredContent"]
            self.assertLess(time.monotonic() - started, 1.0)

            self.assertEqual((listing["count"], listing["current_context_session_id"]), (1, sid))
            entry = listing["sessions"][0]
            self.assertEqual(list(entry), _LIST_KEYS)
            identity = (entry["session_id"], entry["input_path"], entry["filename"])
            self.assertEqual(identity, (sid, path, "a.exe"))
            self.assertEqual(entry["created_at"], entry["last_accessed"])
            self.assertEqual(entry["metadata"]["state"], "opening")
            self.assertEqual(entry["metadata"]["queued_jobs"], 0)
            self.assertEqual(entry["metadata"]["running_job"]["job_id"], submission.job.id)
            self.assertEqual(entry["metadata"]["running_job"]["phase"], "opening")
            pid = factory.workers[0].process.pid
            flag_keys = ("is_analyzing", "is_active", "is_current_context", "bound_contexts")
            flags = [entry[key] for key in flag_keys]
            self.assertEqual(flags, [True, True, True, 1])
            owner = [entry[key] for key in ("backend", "owned", "pid", "worker_pid")]
            self.assertEqual(owner, ["worker", True, pid, pid])

            gate.set()
            self.assertTrue(submission.job.wait(_WAIT))
            listing = self.upstream._original_dispatch(request)["result"]["structuredContent"]
            entry = listing["sessions"][0]
            self.assertEqual(list(entry), _LIST_KEYS)
            self.assertEqual((entry["session_id"], entry["metadata"]["state"]), (sid, "ready"))

    def test_concurrent_opens_of_one_path_spawn_one_worker(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(spawn_gate=gate)
        self.addCleanup(gate.set)
        path = self._path("a.exe")
        idb = self._path("a.exe.i64")
        barrier = threading.Barrier(2)
        results: list[Any] = []

        def submit() -> None:
            barrier.wait(_WAIT)
            results.append(supervisor.submit_open(path, context_id=self.ctx))

        threads = [threading.Thread(target=submit, daemon=True) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
            self.assertFalse(thread.is_alive())
        by_idb = supervisor.submit_open(idb, context_id="other")
        self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))
        gate.set()

        job = results[0].job
        self.assertTrue(job.wait(_WAIT))
        self.assertEqual(job.status, "ok")
        self.assertEqual(sorted(result.created for result in results), [False, True])
        self.assertFalse(by_idb.created)
        self.assertEqual({result.session_id for result in [*results, by_idb]}, {job.session_id})
        self.assertTrue(all(result.job is job for result in [*results, by_idb]))
        self.assertEqual((len(factory.spawn_threads), len(factory.workers)), (1, 1))
        again = supervisor.submit_open(idb)
        self.assertEqual((again.session_id, again.job, again.created), (job.session_id, None, False))
        self.assertIs(again.session, supervisor.sessions[job.session_id])

    def test_spawning_open_counts_against_max_workers(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(max_workers=1, spawn_gate=gate)
        self.addCleanup(gate.set)
        a = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))
        self.assertEqual(supervisor.database_state(a.session_id), "spawning")
        self.assertEqual(supervisor.seats_in_use(), 1)

        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            b = supervisor.submit_open(self._path("b.exe"), context_id="ctx-b")
            self.assertTrue(b.job.wait(_WAIT))
        self.assertEqual((b.job.status, b.job.info()["error"]), ("error", _MAX_TEXT))
        self.assertEqual(len(factory.spawn_threads), 1)
        self.assertEqual(supervisor.database_state(b.session_id), "closed")
        self.assertNotIn(b.session_id, supervisor._dispatchers)
        self.assertNotIn("ctx-b", supervisor.context_bindings)

        gate.set()
        self.assertTrue(a.job.wait(_WAIT))
        self.assertEqual(a.job.status, "ok")
        self.assertEqual(supervisor.database_state(a.session_id), "ready")
        self.assertEqual(supervisor.seats_in_use(), 1)

    def test_cancel_open_terminates_worker_and_frees_seat(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(max_workers=1, open_gate=gate)
        self.addCleanup(gate.set)
        a = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        sid = a.session_id
        follower = supervisor.submit_job(sid, "echo", 2, {}, lambda _job: "never")
        self.assertTrue(_wait_until(lambda: self._opening(factory)))
        worker = factory.workers[0]
        self.assertEqual(supervisor.database_state(sid), "opening")
        self.assertEqual(supervisor.seats_in_use(), 1)

        self.assertEqual(supervisor.cancel_job(a.job.id), "cancelling")
        self.assertTrue(a.job.wait(_WAIT))
        self.assertTrue(follower.wait(_WAIT))

        self.assertEqual((a.job.status, a.job.info()["error"]), ("cancelled", "Cancelled"))
        self.assertEqual(worker.process.poll(), -9)
        expected = ("error", f"Database '{sid}' did not open: Cancelled")
        self.assertEqual((follower.status, follower.info()["error"]), expected)
        self.assertEqual(supervisor.seats_in_use(), 0)
        self.assertNotIn(sid, supervisor._dispatchers)
        self.assertNotIn(self.ctx, supervisor.context_bindings)
        self.assertEqual(supervisor.database_state(sid), "closed")
        self.assertEqual(supervisor.cancel_job(a.job.id), "finished")

        gate.set()
        b = supervisor.open_session(self._path("b.exe"), context_id=self.ctx)
        self.assertTrue(b.is_alive())
        self.assertEqual(len(factory.workers), 2)

    def test_dead_gui_session_reopens_on_executor_only(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(spawn_gate=gate)
        self.addCleanup(gate.set)
        path = self._path("gui.exe")
        instance = {"host": "127.0.0.1", "port": _closed_port(), "pid": 4242, "idb_path": path}
        supervisor._find_gui_instance_for_path = lambda resolved_path: instance
        gui = supervisor.open_session(path, context_id=self.ctx)
        sid = gui.session_id
        self.assertEqual((gui.backend, gui.pid), ("gui", 4242))
        self.assertIs(supervisor.resolve_session(sid), gui)
        self.assertEqual(supervisor.resolve_target(sid), sid)
        self.assertEqual(factory.spawn_threads, [])

        def body(_job: Any) -> Any:
            return supervisor.call_worker_tool(supervisor.resolve_session(), "echo", {"x": 1})

        job = supervisor.submit_job(sid, "echo", 1, {"x": 1}, body)
        self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))
        self.assertEqual(factory.spawn_threads, [f"queue-{sid}"])
        self.assertEqual(supervisor.database_state(sid), "reopening")
        self.assertEqual(job.info()["phase"], "reopening")
        self.assertEqual(supervisor.seats_in_use(), 1)
        listed: list[Any] = []
        lister = threading.Thread(target=lambda: listed.append(supervisor.list_sessions(None)), daemon=True)
        lister.start()
        lister.join(1.0)
        self.assertFalse(lister.is_alive())
        self.assertEqual(listed[0][0]["metadata"]["state"], "reopening")

        gate.set()
        self.assertTrue(job.wait(_WAIT))
        self.assertEqual(job.status, "ok")
        self.assertEqual(job.result(), {"tool": "echo", "arguments": {"x": 1}})
        replacement = supervisor.sessions[sid]
        self.assertEqual((replacement.backend, replacement.metadata.get("fallback_from_gui")), ("worker", True))
        worker = self._worker_of(factory, replacement)
        self.assertEqual(worker.call_names(), ["ping", "idalib_open", "echo"])
        self.assertIs(worker.calls()[1].arguments["run_auto_analysis"], False)
        self.assertEqual(factory.spawn_threads, [f"queue-{sid}"])
        self.assertEqual(supervisor.database_state(sid), "ready")
        self.assertEqual(supervisor.seats_in_use(), 1)

    def test_gui_open_succeeds_at_seat_limit(self) -> None:
        supervisor, factory = self._make(max_workers=1)
        supervisor.open_session(self._path("a.exe"), context_id=self.ctx)
        gui_path = self._path("gui.exe")
        gui_worker = FakeWorker()
        gui_worker.start()
        self.addCleanup(gui_worker.kill)
        instance = {"host": gui_worker.host, "port": gui_worker.port, "pid": 4242, "idb_path": gui_path}
        supervisor._find_gui_instance_for_path = lambda resolved_path: (
            instance if resolved_path == gui_path else None
        )
        self.assertEqual(supervisor.seats_in_use(), 1)

        gui = supervisor.open_session(gui_path, context_id=self.ctx)

        self.assertEqual((gui.backend, gui.port, gui.pid), ("gui", gui_worker.port, 4242))
        self.assertIs(supervisor.sessions[gui.session_id], gui)
        self.assertEqual(supervisor.context_bindings[self.ctx], gui.session_id)
        self.assertEqual((len(factory.spawn_threads), supervisor.seats_in_use()), (1, 1))
        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            with self.assertRaises(RuntimeError) as caught:
                supervisor.open_session(self._path("c.exe"), context_id=self.ctx)
        self.assertEqual(str(caught.exception), _MAX_TEXT)

    def test_cancel_racing_open_registration(self) -> None:
        supervisor, factory = self._make()
        register = supervisor._register_session_locked
        outcomes: list[str] = []
        cancellers: list[threading.Thread] = []

        def racing_register(session: Any, resolved_path: str, context_id: Any) -> None:
            job = supervisor._dispatchers[session.session_id].current_job()
            canceller = threading.Thread(
                target=lambda: outcomes.append(supervisor.cancel_job(job.id)), daemon=True
            )
            cancellers.append(canceller)
            canceller.start()
            canceller.join(0.2)
            self.assertTrue(canceller.is_alive())
            register(session, resolved_path, context_id)

        supervisor._register_session_locked = racing_register
        submission = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        self.assertTrue(submission.job.wait(_WAIT))
        cancellers[0].join(_WAIT)
        self.assertFalse(cancellers[0].is_alive())

        sid = submission.session_id
        self.assertEqual(outcomes, ["finished"])
        self.assertEqual(submission.job.status, "ok")
        self.assertTrue(supervisor.sessions[sid].is_alive())
        self.assertIsNone(factory.workers[0].process.poll())
        self.assertEqual(supervisor.database_state(sid), "ready")
        self.assertEqual(supervisor.context_bindings[self.ctx], sid)

    def test_open_after_failed_spawn_spawns_again(self) -> None:
        supervisor, factory = self._make()
        attempts: list[int] = []

        def flaky_start() -> Any:
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError("spawn failed")
            return factory()

        supervisor._start_worker_process = flaky_start
        path = self._path("a.exe")
        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            with self.assertRaises(OSError) as caught:
                supervisor.open_session(path, context_id=self.ctx)
        self.assertEqual(str(caught.exception), "spawn failed")
        self.assertEqual((supervisor._pending, supervisor._dispatchers), ({}, {}))
        self.assertEqual(supervisor.seats_in_use(), 0)
        self.assertNotIn(self.ctx, supervisor.context_bindings)

        session = supervisor.open_session(path, context_id=self.ctx)

        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(factory.workers), 1)
        self.assertTrue(session.is_alive())
        self.assertEqual(supervisor._pending, {})
        self.assertEqual(list(supervisor._dispatchers), [session.session_id])
        self.assertEqual(supervisor.context_bindings[self.ctx], session.session_id)

    def test_failed_open_restores_previous_binding(self) -> None:
        supervisor, factory = self._make(max_workers=1)
        a = supervisor.open_session(self._path("a.exe"), context_id=self.ctx)
        path_b = self._path("b.exe")
        hold = self._held_gate()

        def discover(resolved_path: str) -> None:
            if resolved_path == path_b:
                hold.wait(_WAIT)
            return None

        supervisor._find_gui_instance_for_path = discover
        b = supervisor.submit_open(path_b, context_id=self.ctx)
        other = supervisor.submit_open(path_b, context_id="other")
        self.assertEqual((other.session_id, other.created), (b.session_id, False))
        self.assertEqual(supervisor.context_bindings[self.ctx], b.session_id)
        self.assertEqual(supervisor.context_bindings["other"], b.session_id)

        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            hold.set()
            self.assertTrue(b.job.wait(_WAIT))

        self.assertEqual((b.job.status, b.job.info()["error"]), ("error", _MAX_TEXT))
        self.assertEqual(supervisor.context_bindings[self.ctx], a.session_id)
        self.assertNotIn("other", supervisor.context_bindings)
        self.assertEqual(len(factory.workers), 1)

    def test_cancel_flag_then_registering_reports_cancelling(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(open_gate=gate)
        self.addCleanup(gate.set)
        submission = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        sid = submission.session_id
        self.assertTrue(_wait_until(lambda: self._opening(factory)))
        worker = factory.workers[0]
        dispatcher = supervisor._dispatchers[sid]
        real_cancel = dispatcher.cancel

        def late_cancel(job_id: str) -> str:
            gate.set()
            submission.job.wait(_WAIT)
            return real_cancel(job_id)

        dispatcher.cancel = late_cancel

        self.assertEqual(supervisor.cancel_job(submission.job.id), "cancelling")
        self.assertEqual(submission.job.status, "cancelled")
        self.assertEqual(worker.process.poll(), -9)
        self.assertNotIn(sid, supervisor.sessions)
        self.assertNotIn(sid, supervisor._dispatchers)
        self.assertEqual(supervisor.seats_in_use(), 0)

    def test_shutdown_during_spawn_leaves_no_registered_session(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(spawn_gate=gate)
        self.addCleanup(gate.set)
        submission = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))

        supervisor.shutdown()
        self.assertEqual((supervisor._pending, supervisor._dispatchers), ({}, {}))
        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            gate.set()
            self.assertTrue(submission.job.wait(_WAIT))

        expected = ("error", "Supervisor shutting down")
        self.assertEqual((submission.job.status, submission.job.info()["error"]), expected)
        self.assertEqual(supervisor.sessions, {})
        self.assertEqual(len(factory.workers), 1)
        self.assertEqual(factory.workers[0].process.poll(), -9)
        self.assertEqual(supervisor.seats_in_use(), 0)
        with self.assertRaises(QueueClosedError):
            supervisor.submit_open(self._path("b.exe"))
        self.assertFalse(supervisor.close_session(submission.session_id))

    def test_cold_tools_list_spawns_one_schema_worker(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(spawn_gate=gate)
        self.addCleanup(gate.set)
        request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        responses: list[Any] = []
        with preserved_upstream(self.upstream):
            self.upstream.supervisor = supervisor
            callers = [
                threading.Thread(
                    target=lambda: responses.append(self.upstream._handle_tools_list(request)), daemon=True
                )
                for _ in range(2)
            ]
            for caller in callers:
                caller.start()
            self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))
            seats: list[int] = []
            helper = threading.Thread(target=lambda: seats.append(supervisor.seats_in_use()), daemon=True)
            helper.start()
            helper.join(1.0)
            self.assertFalse(helper.is_alive())
            self.assertEqual(seats, [1])

            gate.set()
            for caller in callers:
                caller.join(_WAIT)
                self.assertFalse(caller.is_alive())
            self.assertEqual(len(factory.workers), 1)
            self.assertEqual(supervisor._schema_worker.port, factory.workers[0].port)
            self.assertEqual(len(responses), 2)
            for response in responses:
                names = [tool["name"] for tool in response["result"]["tools"]]
                self.assertIn("slow_tool", names)
                self.assertIn("echo", names)

    def test_schema_spawn_keeps_handed_back_worker(self) -> None:
        supervisor, factory = self._make()
        handed_back = factory()
        gate = threading.Event()
        factory.spawn_gate = gate
        self.addCleanup(gate.set)
        request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        responses: list[Any] = []
        with preserved_upstream(self.upstream):
            self.upstream.supervisor = supervisor
            caller = threading.Thread(
                target=lambda: responses.append(self.upstream._handle_tools_list(request)), daemon=True
            )
            caller.start()
            self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 2))
            with supervisor._lock:
                supervisor._schema_worker = handed_back

            gate.set()
            caller.join(_WAIT)
            self.assertFalse(caller.is_alive())
            self.assertIs(supervisor._schema_worker, handed_back)
            spawned = factory.workers[1]
            self.assertTrue(_wait_until(lambda: spawned.process.poll() == -9))
            registered = {session.port for session in supervisor.sessions.values()}
            alive = [worker for worker in factory.workers if worker.process.poll() is None]
            tracked = {handed_back.port, *registered}
            self.assertTrue(all(worker.port in tracked for worker in alive))
            self.assertIn("echo", [tool["name"] for tool in responses[0]["result"]["tools"]])

    def test_gui_match_registering_takes_no_seat(self) -> None:
        supervisor, factory = self._make(max_workers=1)
        gui_path = self._path("gui.exe")
        instance = {"host": "127.0.0.1", "port": _closed_port(), "pid": 4242, "idb_path": gui_path}
        supervisor._find_gui_instance_for_path = lambda resolved_path: (
            instance if resolved_path == gui_path else None
        )
        register = supervisor._register_open
        entered = threading.Event()
        hold = self._held_gate()

        def paused_register(record: Any, job: Any, session: Any, outcome: Any) -> Any:
            if session.backend == "gui":
                entered.set()
                hold.wait(_WAIT)
            return register(record, job, session, outcome)

        supervisor._register_open = paused_register
        gui = supervisor.submit_open(gui_path, context_id=self.ctx)
        self.assertTrue(entered.wait(_WAIT))
        self.assertEqual(gui.job.info()["phase"], "registering")
        self.assertEqual(supervisor.seats_in_use(), 0)

        other = supervisor.submit_open(self._path("b.exe"), context_id="ctx-b")
        self.assertTrue(other.job.wait(_WAIT))
        self.assertEqual(other.job.status, "ok")

        hold.set()
        self.assertTrue(gui.job.wait(_WAIT))
        self.assertEqual(gui.job.status, "ok")
        self.assertEqual(supervisor.sessions[gui.session_id].backend, "gui")
        self.assertEqual((len(factory.workers), supervisor.seats_in_use()), (1, 1))

    def test_reopen_collision_keeps_seat_until_discarded(self) -> None:
        gate = threading.Event()
        supervisor, factory = self._make(spawn_gate=gate)
        self.addCleanup(gate.set)
        path = self._path("gui.exe")
        instance = {"host": "127.0.0.1", "port": _closed_port(), "pid": 4242, "idb_path": path}
        supervisor._find_gui_instance_for_path = lambda resolved_path: instance
        opened = supervisor.submit_open(path, context_id=self.ctx)
        self.assertTrue(opened.job.wait(_WAIT))
        sid = opened.session_id
        discard = supervisor._discard_opened_worker_session
        discarding = threading.Event()
        hold = self._held_gate()

        def blocked_discard(worker: Any, session_id: str) -> None:
            discarding.set()
            hold.wait(_WAIT)
            discard(worker, session_id)

        supervisor._discard_opened_worker_session = blocked_discard
        job = supervisor.submit_job(sid, "echo", 1, {}, lambda _job: "never")
        self.assertTrue(_wait_until(lambda: len(factory.spawn_threads) == 1))
        with supervisor._lock:
            supervisor._unregister_session_locked(sid)
        gate.set()
        self.assertTrue(discarding.wait(_WAIT))
        worker = factory.workers[0]
        self.assertIsNone(worker.process.poll())
        self.assertEqual(supervisor.seats_in_use(), 1)

        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            hold.set()
            self.assertTrue(job.wait(_WAIT))
        expected = f"Session '{sid}' was closed or replaced while reopening headlessly"
        self.assertEqual((job.status, job.info()["error"]), ("error", expected))
        self.assertEqual(worker.process.poll(), -9)
        self.assertEqual(supervisor.seats_in_use(), 0)

    def test_follower_after_removed_open_never_runs_body(self) -> None:
        supervisor, factory = self._make()
        start = self._held_gate()

        class HeldDispatcher(WorkerDispatcher):
            def _executor_loop(self) -> None:
                start.wait(_WAIT)
                super()._executor_loop()

        with mock.patch("idalib_mcp.server.WorkerDispatcher", HeldDispatcher):
            opening = supervisor.submit_open(self._path("a.exe"), context_id=self.ctx)
        sid = opening.session_id
        dispatcher = supervisor._dispatchers[sid]
        ran: list[int] = []
        follower = supervisor.submit_job(sid, "echo", 2, {}, lambda _job: ran.append(1))
        finish = supervisor._finish_failed_open
        guard = supervisor._guard_follower

        def late_finish(record: Any, reason: str, **kwargs: Any) -> None:
            _wait_until(lambda: dispatcher.current_job() is follower)
            finish(record, reason, **kwargs)

        def late_guard(job: Any, *args: Any) -> None:
            if job is follower:
                _wait_until(lambda: sid not in supervisor._pending)
            guard(job, *args)

        supervisor._finish_failed_open = late_finish
        supervisor._guard_follower = late_guard
        outcomes: list[str] = []
        canceller = threading.Thread(
            target=lambda: outcomes.append(supervisor.cancel_job(opening.job.id)), daemon=True
        )
        canceller.start()
        self.assertTrue(opening.job.wait(_WAIT))
        with self.assertLogs("idalib_mcp.dispatcher", level="ERROR"):
            start.set()
            self.assertTrue(follower.wait(_WAIT))
        canceller.join(_WAIT)
        self.assertFalse(canceller.is_alive())

        self.assertEqual(outcomes, ["removed"])
        self.assertEqual(ran, [])
        expected = ("error", f"Database '{sid}' did not open: Cancelled")
        self.assertEqual((follower.status, follower.info()["error"]), expected)
        self.assertNotIn(sid, supervisor._dispatchers)
        self.assertEqual(factory.workers, [])


_JOB_TOOL_SCHEMAS = [
    {
        "name": "idalib_job_status",
        "description": "Wait up to wait_seconds for a job; return its original result when finished, else its status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job id from a job ticket"},
                "wait_seconds": {"type": "number", "description": "Seconds to wait for the job to finish (0-30)"},
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "idalib_jobs",
        "description": "List running, queued and recently finished jobs per database.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "description": "Database to inspect (session_id, filename or path); all if omitted",
                },
                "recent": {"type": "integer", "description": "Finished jobs to include per database (0-100)"},
            },
            "required": [],
        },
    },
    {
        "name": "idalib_job_cancel",
        "description": (
            "Cancel a job: queued jobs are dropped, an opening database's worker is terminated, "
            "other running jobs finish and are discarded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"job_id": {"type": "string", "description": "Job id to cancel"}},
            "required": ["job_id"],
        },
    },
]
_JOB_TOOL_NAMES = [schema["name"] for schema in _JOB_TOOL_SCHEMAS]
_SYNC_WAIT_ERROR = "--sync-wait must be a finite number >= 0"


def _post_ping(worker: FakeWorker) -> Any:
    conn = http.client.HTTPConnection(worker.host, worker.port, timeout=_WAIT)
    try:
        body = json.dumps({"jsonrpc": "2.0", "id": 99, "method": "ping"})
        conn.request("POST", "/mcp", body, {"Content-Type": "application/json"})
        return json.loads(conn.getresponse().read())
    finally:
        conn.close()


class RouterTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.temp_path = Path(temp_dir.name)
        self.upstream = importlib.import_module("ida_pro_mcp.idalib_supervisor")
        self.ctx = self.upstream.SHARED_FALLBACK_CONTEXT_ID
        self.ids = itertools.count(100)

    def _install(self, *, sync_wait: float = 0.1, **factory_kwargs: Any) -> tuple[Any, FakeWorkerFactory]:
        """Wire a supervisor over fake workers; the caller is inside preserved_upstream."""
        factory = FakeWorkerFactory(self.upstream, **factory_kwargs)
        self.addCleanup(factory.stop_all)
        supervisor = _install_supervisor(self.upstream, sync_wait=sync_wait)
        attach_fakes(supervisor, factory)
        wait_ready = supervisor._wait_worker_ready
        supervisor._wait_worker_ready = lambda worker, timeout=_WAIT: wait_ready(worker, timeout=_WAIT)
        return supervisor, factory

    def _installed(self, **kwargs: Any) -> tuple[Any, FakeWorkerFactory]:
        self.enterContext(preserved_upstream(self.upstream))
        return self._install(**kwargs)

    def _path(self, name: str) -> str:
        path = self.temp_path / name
        path.write_bytes(b"MZ")
        return str(path.resolve())

    def _open(self, supervisor: Any, factory: FakeWorkerFactory, name: str) -> tuple[Any, FakeWorker]:
        session = supervisor.open_session(self._path(name), context_id=self.ctx)
        return session, next(worker for worker in factory.workers if worker.port == session.port)

    def _request(self, name: str, arguments: Any) -> dict[str, Any]:
        params = {"name": name, "arguments": arguments}
        return {"jsonrpc": "2.0", "id": next(self.ids), "method": "tools/call", "params": params}

    def _dispatch(self, request: dict[str, Any]) -> Any:
        return self.upstream.mcp.registry.dispatch(request)

    def _call(self, name: str, arguments: Any) -> Any:
        return self._dispatch(self._request(name, arguments))

    def _structured(self, response: Any) -> Any:
        result = response["result"]
        self.assertIs(result["isError"], False, result)
        return result["structuredContent"]

    def _ticket_job_id(self, response: Any) -> str:
        result = response["result"]
        self.assertIs(result["isError"], True, result)
        self.assertNotIn("structuredContent", result)
        ticket = result["_meta"]["idalib_job"]
        self.assertIs(ticket["pending"], True)
        self.assertEqual(json.loads(result["content"][0]["text"]), ticket)
        return ticket["job"]["job_id"]

    def _finished(self, supervisor: Any, job_id: str) -> Any:
        job = supervisor.find_job(job_id)
        self.assertIsNotNone(job)
        self.assertTrue(job.wait(_WAIT))
        return job

    def test_hybrid_fast_result_and_slow_ticket_then_status(self) -> None:
        supervisor, factory = self._installed(sync_wait=0.3)
        _session, worker = self._open(supervisor, factory, "a.exe")

        fast = self._call("echo", {"x": 1})
        self.assertEqual(self._structured(fast), {"tool": "echo", "arguments": {"x": 1}})
        self.assertNotIn("_meta", fast["result"])

        started = time.monotonic()
        slow = self._call("slow_tool", {"seconds": 1.0})
        self.assertLess(time.monotonic() - started, 0.9)
        job_id = self._ticket_job_id(slow)
        ticket = slow["result"]["_meta"]["idalib_job"]
        self.assertEqual(
            (ticket["job"]["tool"], ticket["job"]["status"], ticket["database_state"]),
            ("slow_tool", "running", "ready"),
        )
        poll = {"tool": "idalib_job_status", "arguments": {"job_id": job_id, "wait_seconds": 30}}
        self.assertEqual(ticket["poll"], poll)

        status = self._call("idalib_job_status", {"job_id": job_id, "wait_seconds": 5})
        self.assertEqual(self._structured(status), {"slept": 1.0})
        info = status["result"]["_meta"]["idalib_job"]
        self.assertEqual((info["job_id"], info["status"]), (job_id, "ok"))
        self.assertEqual(worker.call_names()[-2:], ["echo", "slow_tool"])

    def test_routes_queue_behind_queued_tool(self) -> None:
        routes = {
            "worker tool by input path": lambda a: ("echo", {"which": "route", "database": a.input_path}),
            "idalib_save": lambda a: ("idalib_save", {"session_id": a.session_id}),
            "idalib_close": lambda a: ("idalib_close", {"session_id": a.session_id}),
        }
        for route, make_call in routes.items():
            with self.subTest(route=route), preserved_upstream(self.upstream):
                supervisor, factory = self._install(sync_wait=0.1)
                session, worker = self._open(supervisor, factory, "a.exe")
                slow_id = self._ticket_job_id(self._call("slow_tool", {"seconds": 1.0}))
                self.assertTrue(_wait_until(lambda: "slow_tool" in worker.call_names()))
                t2_id = self._ticket_job_id(self._call("echo", {"which": "T2"}))
                name, arguments = make_call(session)
                route_id = self._ticket_job_id(self._call(name, arguments))
                self.assertFalse(supervisor.find_job(slow_id).is_done())

                for job_id in (slow_id, t2_id, route_id):
                    self.assertEqual(self._finished(supervisor, job_id).status, "ok")
                # A route that bypassed the queue would connect during slow_tool and be accepted before T2.
                names = worker.call_names()
                self.assertEqual(names[names.index("slow_tool") :], ["slow_tool", "echo", name])
                echoed = [call.arguments for call in worker.calls() if call.name == "echo"]
                self.assertEqual(echoed[0], {"which": "T2"})
                if name == "echo":
                    self.assertEqual(echoed[1:], [{"which": "route"}])

        with self.subTest(route="auto-open"), preserved_upstream(self.upstream):
            gate = threading.Event()
            self.addCleanup(gate.set)
            supervisor, factory = self._install(sync_wait=_WAIT, open_gate=gate)
            path = self._path("b.exe")
            responses: dict[str, Any] = {}

            def send(which: str) -> None:
                responses[which] = self._call("echo", {"which": which, "database": path})

            first = threading.Thread(target=send, args=("first",), daemon=True)
            first.start()
            self.assertTrue(
                _wait_until(lambda: bool(factory.workers) and "idalib_open" in factory.workers[0].call_names())
            )
            sid = supervisor.resolve_target(path)
            self.assertEqual(supervisor.database_state(sid), "opening")
            second = threading.Thread(target=send, args=("second",), daemon=True)
            second.start()
            queued = lambda: [job["tool"] for job in supervisor.job_databases(sid, 0)[0]["queued"]]
            self.assertTrue(_wait_until(lambda: queued() == ["echo", "echo"]))
            gate.set()
            for sender in (first, second):
                sender.join(_WAIT)
                self.assertFalse(sender.is_alive())

            for which in ("first", "second"):
                self.assertEqual(self._structured(responses[which]), {"tool": "echo", "arguments": {"which": which}})
            self.assertEqual(factory.workers[0].call_names(), ["ping", "idalib_open", "echo", "echo"])
            self.assertEqual(supervisor.context_bindings[self.ctx], sid)

    def test_cancel_running_job_holds_queue_until_worker_returns(self) -> None:
        supervisor, factory = self._installed(sync_wait=0.1)
        _session, worker = self._open(supervisor, factory, "a.exe")
        t1_id = self._ticket_job_id(self._call("slow_tool", {"seconds": 1.0}))
        self.assertTrue(_wait_until(lambda: "slow_tool" in worker.call_names()))
        t2_id = self._ticket_job_id(self._call("echo", {"which": "T2"}))

        cancel = self._call("idalib_job_cancel", {"job_id": t1_id})
        pings: list[Any] = []
        pinger = threading.Thread(target=lambda: pings.append(_post_ping(worker)), daemon=True)
        pinger.start()

        outcome = self._structured(cancel)
        self.assertEqual((outcome["job_id"], outcome["outcome"]), (t1_id, "cancelling"))
        self.assertEqual(outcome["job"]["status"], "cancelling")
        t1 = self._finished(supervisor, t1_id)
        t2 = self._finished(supervisor, t2_id)
        pinger.join(_WAIT)
        self.assertFalse(pinger.is_alive())
        self.assertEqual(pings, [{"jsonrpc": "2.0", "result": {}, "id": 99}])
        self.assertEqual((t1.status, t2.status), ("cancelled", "ok"))
        # An executor that popped T2 at the cancel would connect T2 before the ping.
        names = worker.call_names()
        self.assertEqual(names[names.index("slow_tool") :], ["slow_tool", "ping", "echo"])
        status = self._call("idalib_job_status", {"job_id": t1_id, "wait_seconds": 0})["result"]
        self.assertIs(status["isError"], True)
        self.assertEqual(json.loads(status["content"][0]["text"])["error"], "Job was cancelled")

    def test_queued_save_does_not_rebind_after_switch(self) -> None:
        supervisor, factory = self._installed(sync_wait=0.1)
        a, a_worker = self._open(supervisor, factory, "a.exe")
        b, _b_worker = self._open(supervisor, factory, "b.exe")
        self.assertEqual(supervisor.context_bindings[self.ctx], b.session_id)
        slow_id = self._ticket_job_id(self._call("slow_tool", {"seconds": 0.8, "database": a.session_id}))
        self.assertTrue(_wait_until(lambda: "slow_tool" in a_worker.call_names()))

        save_id = self._ticket_job_id(self._call("idalib_save", {"session_id": a.session_id}))
        self.assertEqual(supervisor.context_bindings[self.ctx], a.session_id)
        switched = self._structured(self._call("idalib_switch", {"session_id": b.session_id}))
        self.assertIs(switched["success"], True)

        self.assertEqual(self._finished(supervisor, save_id).status, "ok")
        self.assertEqual(self._finished(supervisor, slow_id).status, "ok")
        self.assertEqual(supervisor.context_bindings[self.ctx], b.session_id)
        self.assertEqual(a_worker.call_names()[-2:], ["slow_tool", "idalib_save"])

    def test_idalib_open_string_encoded_arguments_returns_ticket(self) -> None:
        gate = threading.Event()
        self.addCleanup(gate.set)
        # Bounds a router that runs the open inline: it would return only once the gate opens.
        release = threading.Timer(3.0, gate.set)
        release.daemon = True
        self.addCleanup(release.cancel)
        supervisor, _factory = self._installed(sync_wait=0.3, open_gate=gate)
        path = self._path("a.exe")
        release.start()

        started = time.monotonic()
        response = self._call("idalib_open", json.dumps({"input_path": path}))
        elapsed = time.monotonic() - started
        job_id = self._ticket_job_id(response)
        self.assertLess(elapsed, 1.5)
        ticket = response["result"]["_meta"]["idalib_job"]
        self.assertEqual((ticket["job"]["tool"], ticket["job"]["kind"]), ("idalib_open", "open"))
        self.assertIn(ticket["database_state"], ("spawning", "opening"))

        gate.set()
        opened = self._structured(self._call("idalib_job_status", {"job_id": job_id, "wait_seconds": 5}))
        self.assertIs(opened["success"], True)
        self.assertEqual((opened["session"]["input_path"], opened["context_id"]), (path, self.ctx))
        self.assertIn(opened["session"]["session_id"], supervisor.sessions)

    def test_management_param_errors_match_original_dispatch(self) -> None:
        supervisor, factory = self._installed()
        path = self._path("a.exe")
        cases = [
            ("idalib_open", [path]),
            ("idalib_open", "[1, 2]"),
            ("idalib_open", {"input_path": path, "session_id": "123"}),
            ("idalib_open", {"input_path": path, "unexpected": 1}),
            ("idalib_open", {}),
            ("idalib_close", {"session_id": 5}),
            ("idalib_save", {"session_id": "123"}),
            ("idalib_warmup", {"init_hexrays": "yes"}),
        ]
        for name, arguments in cases:
            with self.subTest(name=name, arguments=arguments):
                request = self._request(name, arguments)
                response = self._dispatch(request)
                self.assertEqual(response, self.upstream._original_dispatch(request))
                self.assertTrue("error" in response or response["result"]["isError"], response)
        self.assertEqual(factory.workers, [])
        self.assertEqual(supervisor.list_sessions(None), [])

    def test_sync_wait_rejects_non_finite_and_negative(self) -> None:
        reached = mock.Mock(side_effect=AssertionError("reached _install_supervisor"))
        with (
            mock.patch("idalib_mcp.server._install_supervisor", reached),
            mock.patch("idalib_mcp.server.validate_ida_home", side_effect=lambda path: path),
            mock.patch("logging.basicConfig"),
        ):
            for value in ("nan", "inf", "-inf", "-1", "-0.5"):
                with self.subTest(value=value), mock.patch.object(sys, "argv", ["idalib-mcp", f"--sync-wait={value}"]):
                    with self.assertRaises(SystemExit) as caught:
                        main()
                    self.assertEqual(caught.exception.code, _SYNC_WAIT_ERROR)
            with (
                mock.patch.dict(os.environ, {"IDA_MCP_SYNC_WAIT": "inf"}),
                mock.patch.object(sys, "argv", ["idalib-mcp"]),
            ):
                with self.assertRaises(SystemExit) as caught:
                    main()
                self.assertEqual(caught.exception.code, _SYNC_WAIT_ERROR)
            reached.assert_not_called()

            with mock.patch.object(sys, "argv", ["idalib-mcp", "--sync-wait", "0"]):
                with self.assertRaises(AssertionError):
                    main()
        self.assertEqual(reached.call_args.kwargs["sync_wait"], 0.0)

    def test_sync_wait_zero_returns_ticket_at_once(self) -> None:
        supervisor, factory = self._installed(sync_wait=0)
        self.assertEqual(supervisor.sync_wait, 0)
        self._open(supervisor, factory, "a.exe")

        started = time.monotonic()
        response = self._call("slow_tool", {"seconds": 1.0})
        self.assertLess(time.monotonic() - started, 0.5)
        job_id = self._ticket_job_id(response)
        self.assertEqual(self._finished(supervisor, job_id).status, "ok")

    def test_tools_call_list_params_matches_original_dispatch(self) -> None:
        original_handler = self.upstream._handle_tools_call
        supervisor, factory = self._installed()
        path = self._path("a.exe")
        _session, worker = self._open(supervisor, factory, "a.exe")
        fixed = datetime(2026, 1, 2, 3, 4, 5)

        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz: Any = None) -> datetime:
                return fixed

        with mock.patch("idalib_mcp.server.datetime", FrozenDatetime):
            for params in ([], ["idalib_list", {}, None, "extra"], ["idalib_list"], ["idalib_open", {"input_path": path}]):
                with self.subTest(params=params):
                    request = {"jsonrpc": "2.0", "id": next(self.ids), "method": "tools/call", "params": params}
                    response = self._dispatch(request)
                    self.assertEqual(response, self.upstream._original_dispatch(request))
                    if params and params[0] == "idalib_open":
                        self.assertIs(self._structured(response)["success"], True)

        echoed = self._dispatch({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": ["echo", {"which": "u"}]})
        self.assertEqual(self._structured(echoed), {"tool": "echo", "arguments": {"which": "u"}})
        self.assertEqual((worker.calls()[-1].name, worker.calls()[-1].arguments), ("echo", {"which": "u"}))

        hidden = {"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {"name": "list_instances"}}
        expected = original_handler(hidden)
        self.assertIs(expected["result"]["isError"], True)
        self.assertEqual(self._dispatch({**hidden, "params": ["list_instances"]}), expected)
        self.assertEqual(self._dispatch(hidden), expected)

    def test_tools_list_adds_only_job_tools(self) -> None:
        self._installed()
        request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        with_jobs = self._dispatch(request)["result"]["tools"]
        self.assertTrue(set(_JOB_TOOL_NAMES) <= self.upstream.IDALIB_MANAGEMENT_TOOLS)
        for name in _JOB_TOOL_NAMES:
            del self.upstream.mcp.tools.methods[name]
            self.upstream.IDALIB_MANAGEMENT_TOOLS.discard(name)
        without = self._dispatch(request)["result"]["tools"]

        self.assertEqual([tool for tool in with_jobs if tool["name"] not in _JOB_TOOL_NAMES], without)
        self.assertEqual([tool for tool in with_jobs if tool["name"] in _JOB_TOOL_NAMES], _JOB_TOOL_SCHEMAS)
        self.assertEqual([tool["name"] for tool in without][-2:], ["slow_tool", "echo"])

    def test_tools_call_notification_returns_none(self) -> None:
        supervisor, factory = self._installed(sync_wait=_WAIT)
        session, worker = self._open(supervisor, factory, "a.exe")
        params = {"name": "slow_tool", "arguments": {"seconds": 1.0}}

        started = time.monotonic()
        self.assertIsNone(self._dispatch({"jsonrpc": "2.0", "method": "tools/call", "params": params}))
        self.assertLess(time.monotonic() - started, 0.5)

        self.assertTrue(_wait_until(lambda: any(c.name == "slow_tool" and c.end for c in worker.calls())))
        self.assertEqual(worker.calls()[-1].arguments, {"seconds": 1.0})
        recent = lambda: supervisor.job_databases(session.session_id, 5)[0]["recent"]
        self.assertTrue(_wait_until(lambda: [(j["tool"], j["status"]) for j in recent()][:1] == [("slow_tool", "ok")]))

    def test_analysis_call_auto_opens_existing_database_path(self) -> None:
        supervisor, factory = self._installed(sync_wait=_WAIT)
        path = self._path("sample.exe")

        response = self._call("decompile", {"database": path, "addr": "0x401000"})

        self.assertEqual(self._structured(response), {"tool": "decompile", "arguments": {"addr": "0x401000"}})
        (sid,) = supervisor.sessions
        self.assertEqual(supervisor.sessions[sid].input_path, path)
        self.assertEqual(supervisor.context_bindings[self.ctx], sid)
        worker = factory.workers[0]
        self.assertEqual(worker.call_names(), ["ping", "idalib_open", "decompile"])
        self.assertEqual(worker.calls()[-1].arguments, {"addr": "0x401000"})


class OpenUiToolTests(unittest.TestCase):
    def test_register_open_ui_tool_adds_to_management_set(self) -> None:
        from idalib_mcp.server import _register_open_ui_tool

        upstream = FakeUpstream()
        upstream.IDALIB_MANAGEMENT_TOOLS = set(upstream.IDALIB_MANAGEMENT_TOOLS)

        captured = {}

        class FakeMcpWithTool(FakeMcp):
            def tool(self, func):
                captured["func"] = func
                return func

        upstream.mcp = FakeMcpWithTool()
        _register_open_ui_tool(upstream, host="127.0.0.1", port=8745)

        self.assertIn("idalib_open_ui", upstream.IDALIB_MANAGEMENT_TOOLS)
        result = captured["func"]()
        self.assertEqual(result["url"], "http://127.0.0.1:8745/instances")


class HistoryEndpointTests(unittest.TestCase):
    def _build_handler_and_dispatcher(self):
        from idalib_mcp.dispatcher import WorkerDispatcher
        from idalib_mcp.server import _build_request_handler

        dispatcher = WorkerDispatcher("session-a")
        dispatcher.run("ping", 1, {"x": 1}, lambda _c: {"ok": True}, threading.Event())
        big = "a" * 5000
        dispatcher.run("big", 2, {"blob": big}, lambda _c: {"blob": big}, threading.Event())

        class Supervisor(FakeSupervisor):
            def __init__(self):
                super().__init__()
                self._dispatchers = {"session-a": dispatcher}

        # Minimal base handler so _build_request_handler can subclass it.
        class _FakeMcpHttpRequestHandler:
            pass

        def _fake_serve():
            pass

        _fake_serve.__globals__["McpHttpRequestHandler"] = _FakeMcpHttpRequestHandler  # type: ignore[attr-defined]

        class _FakeMcpServer:
            serve = _fake_serve

        class UpstreamWithSupervisor(FakeUpstream):
            McpServer = _FakeMcpServer
            supervisor = None

        upstream = UpstreamWithSupervisor()
        upstream.supervisor = Supervisor()
        handler_cls = _build_request_handler(upstream)
        return handler_cls, dispatcher, upstream

    def test_history_snapshot_endpoint(self):
        handler_cls, dispatcher, upstream = self._build_handler_and_dispatcher()
        # Bypass network: directly call the routing helper.
        snapshot = handler_cls._history_snapshot(upstream.supervisor, "session-a")
        self.assertEqual(len(snapshot["history"]), 2)

    def test_history_payload_returns_full_text(self):
        handler_cls, dispatcher, upstream = self._build_handler_and_dispatcher()
        big_record = next(r for r in dispatcher.snapshot()["history"] if r["tool"] == "big")
        payload_id = big_record["args_full_id"]
        result = handler_cls._history_payload(
            upstream.supervisor, "session-a", payload_id,
        )
        self.assertIn("a" * 100, result)

    def test_history_payload_returns_none_when_unknown(self):
        handler_cls, dispatcher, upstream = self._build_handler_and_dispatcher()
        result = handler_cls._history_payload(
            upstream.supervisor, "session-a", "unknown.args",
        )
        self.assertIsNone(result)


class CancellationPatchTests(unittest.TestCase):
    def _build_supervisor(self):
        from idalib_mcp.server import _build_managed_supervisor_class
        upstream = FakeUpstream()
        cls = _build_managed_supervisor_class(upstream)
        return upstream, cls(FakeMcp(), worker_args=[])

    def test_notifications_cancelled_sets_event(self):
        from idalib_mcp.server import _wrap_dispatch_supervisor
        upstream, supervisor = self._build_supervisor()
        upstream.supervisor = supervisor
        original_dispatch_calls: list[dict] = []
        upstream._original_dispatch = lambda req: original_dispatch_calls.append(req) or None
        # Seed mcp.registry.dispatch as a callable; _wrap_dispatch_supervisor wraps it.
        upstream.mcp.registry.dispatch = lambda req: None
        _wrap_dispatch_supervisor(upstream)

        handle = supervisor._cancels.register(42)
        # Fire notifications/cancelled and confirm the event sets.
        response = upstream.mcp.registry.dispatch({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": 42},
        })
        self.assertIsNone(response)
        self.assertTrue(handle.event.is_set())


if __name__ == "__main__":
    unittest.main()
