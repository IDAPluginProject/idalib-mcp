from __future__ import annotations

import importlib
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from fakeworker import FakeWorker, FakeWorkerFactory, make_supervisor, preserved_upstream
from idalib_mcp.dispatcher import WorkerDispatcher
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
            reader = threading.Thread(
                target=lambda: responses.append(self.upstream._handle_resources_read(request)), daemon=True
            )
            reader.start()
            self.assertTrue(_wait_until(lambda: self._queued(dispatcher) == [("resources/read", "rpc")]))
            self.assertFalse(slow.is_done())
            self.assertNotIn("resources/read", worker.call_names())
            reader.join(_WAIT)
            self.assertFalse(reader.is_alive())

            contents = [{"uri": "ida://fake", "mimeType": "application/json", "text": "{}"}]
            self.assertEqual(responses, [{"jsonrpc": "2.0", "result": {"contents": contents}, "id": 5}])
            self.assertEqual(slow.status, "ok")
            read_call = self._call(worker, "resources/read")
            self.assertGreaterEqual(read_call.start, self._call(worker, "slow_tool").end)

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
        self.assertEqual(supervisor._dispatchers[b.session_id].jobs_info()["recent"], [])

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
            [("idalib_close", "close", "ok"), ("echo", "tool", "ok")],
        )
        self.assertEqual(worker.call_names()[-1], "idalib_close")
        self.assertEqual(worker.process.poll(), -9)
        with self.assertRaises(LookupError):
            self.supervisor.submit_job(sid, "echo", 2, {}, self._tool_body("echo", {}))
        self.assertFalse(self.supervisor.close_session(sid))
        self.assertEqual(self.supervisor.list_sessions(None), [])


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
