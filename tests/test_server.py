from __future__ import annotations

import importlib
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from fakeworker import FakeWorker, FakeWorkerFactory, make_supervisor, preserved_upstream
from idalib_mcp.dispatcher import QueueClosedError, WorkerDispatcher
from idalib_mcp.server import (
    _bound_instance_ui_url,
    _enable_path_database_auto_open,
    _instance_ui_url,
    _prefer_management_tools_in_tool_list,
    _save_session_before_close,
    _serve_stdio_instance_ui,
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
        self.opened: tuple[str, str] | None = None
        self.forwarded: tuple[str, dict[str, Any]] | None = None
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

    def resolve_context_id(self) -> str:
        return "context-id"

    def open_session(self, input_path: str, *, context_id: str) -> str:
        self.opened = (input_path, context_id)
        return "opened-session"

    def forward_raw(self, session: str, request: dict[str, Any]) -> dict[str, Any]:
        self.forwarded = (session, request)
        return {"ok": True}

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

    def test_analysis_call_auto_opens_existing_database_path(self) -> None:
        upstream = FakeUpstream()
        _enable_path_database_auto_open(upstream)

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "sample.exe"
            input_path.write_bytes(b"MZ")

            result = upstream._handle_tools_call(
                {
                    "id": 7,
                    "params": {
                        "name": "decompile",
                        "arguments": {
                            "database": str(input_path),
                            "addr": "0x401000",
                        },
                    },
                }
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(upstream.supervisor.opened, (str(input_path), "context-id"))
        self.assertIsNotNone(upstream.supervisor.forwarded)
        session, forwarded = upstream.supervisor.forwarded
        self.assertEqual(session, "opened-session")
        self.assertEqual(forwarded["params"]["arguments"], {"addr": "0x401000"})


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
        self.assertEqual(supervisor.list_sessions(None)[0]["metadata"]["state"], "reopening")

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
