from __future__ import annotations

import http.client
import importlib
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from fakeworker import FakeWorker, FakeWorkerFactory, attach_fakes, preserved_upstream
from idalib_mcp.server import _build_request_handler, _install_supervisor, _run_stdio

_WAIT = 5.0
_SYNC_WAIT = 10.0
_JSON = {"Content-Type": "application/json"}


def _wait_until(predicate: Any, timeout: float = _WAIT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


def _tool_call(request_id: int, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    params = {"name": name, "arguments": arguments}
    return {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": params}


class _StdioPeer:
    """The client end of `_run_stdio` over two os.pipe() pairs."""

    def __init__(self, test: unittest.TestCase, upstream: Any) -> None:
        self._test = test
        in_read, in_write = os.pipe()
        out_read, out_write = os.pipe()
        self._to_server = os.fdopen(in_write, "wb")
        self._server_in = os.fdopen(in_read, "rb")
        self._server_out = os.fdopen(out_write, "wb")
        self._from_server = os.fdopen(out_read, "rb")
        self._cond = threading.Condition()
        self._messages: list[tuple[float, dict[str, Any]]] = []
        self._server = threading.Thread(
            target=_run_stdio,
            args=(upstream,),
            kwargs={"stdin": self._server_in, "stdout": self._server_out},
            name="e2e-stdio-server",
            daemon=True,
        )
        self._reader = threading.Thread(target=self._read, name="e2e-stdio-reader", daemon=True)
        test.addCleanup(self.close)
        self._server.start()
        self._reader.start()

    def _read(self) -> None:
        for raw in iter(self._from_server.readline, b""):
            with self._cond:
                self._messages.append((time.monotonic(), json.loads(raw)))
                self._cond.notify_all()

    def send(self, message: dict[str, Any]) -> None:
        self._to_server.write(json.dumps(message).encode("utf-8") + b"\n")
        self._to_server.flush()

    def find(self, request_id: Any) -> tuple[float, dict[str, Any]] | None:
        with self._cond:
            return next((entry for entry in self._messages if entry[1].get("id") == request_id), None)

    def wait_for(self, request_id: Any, timeout: float = _WAIT) -> tuple[float, dict[str, Any]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                entry = next((e for e in self._messages if e[1].get("id") == request_id), None)
                if entry is not None:
                    return entry
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(remaining)
        self._test.fail(f"no stdio response for id {request_id!r} within {timeout}s")

    def close(self) -> None:
        self._to_server.close()
        self._server.join(_WAIT)
        self._server_out.close()
        self._reader.join(_WAIT)
        stuck = [thread.name for thread in (self._server, self._reader) if thread.is_alive()]
        if not stuck:
            self._server_in.close()
            self._from_server.close()
        self._test.assertEqual(stuck, [], "stdio threads outlived the test")


class EndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.temp_path = Path(temp_dir.name)
        self.upstream = importlib.import_module("ida_pro_mcp.idalib_supervisor")
        self.ctx = self.upstream.SHARED_FALLBACK_CONTEXT_ID
        self.enterContext(preserved_upstream(self.upstream))
        self.factory = FakeWorkerFactory(self.upstream)
        self.addCleanup(self.factory.stop_all)
        self.supervisor = _install_supervisor(self.upstream, sync_wait=_SYNC_WAIT)
        attach_fakes(self.supervisor, self.factory)
        wait_ready = self.supervisor._wait_worker_ready
        self.supervisor._wait_worker_ready = lambda worker, timeout=_WAIT: wait_ready(worker, timeout=_WAIT)

    def _open(self, name: str) -> tuple[Any, FakeWorker]:
        path = self.temp_path / name
        path.write_bytes(b"MZ")
        session = self.supervisor.open_session(str(path.resolve()), context_id=self.ctx)
        return session, next(worker for worker in self.factory.workers if worker.port == session.port)

    def _jobs(self, session_id: str) -> list[dict[str, Any]]:
        entry = self.supervisor.job_databases(session_id, 20)[0]
        return ([entry["running"]] if entry["running"] else []) + entry["queued"] + entry["recent"]

    def _serve(self) -> int:
        handler = _build_request_handler(self.upstream)
        self.upstream.mcp.serve(host="127.0.0.1", port=0, background=True, request_handler=handler)
        server = self.upstream.mcp._http_server
        server.handle_error = lambda request, client_address: None
        return server.server_address[1]

    def _post(self, port: int, path: str, body: dict[str, Any]) -> dict[str, Any]:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=_WAIT)
        try:
            conn.request("POST", path, json.dumps(body), _JSON)
            response = conn.getresponse()
            self.assertEqual(response.status, 200)
            return json.loads(response.read())
        finally:
            conn.close()

    def test_stdio_ping_not_blocked_and_cancel_processed(self) -> None:
        _session, worker = self._open("a.exe")
        stdio = _StdioPeer(self, self.upstream)
        stdio.send(_tool_call(1, "slow_tool", {"seconds": 3.0}))
        self.assertTrue(_wait_until(lambda: "slow_tool" in worker.call_names()))

        stdio.send({"jsonrpc": "2.0", "id": 2, "method": "ping"})
        _, pong = stdio.wait_for(2, 1.5)
        self.assertEqual(pong, {"jsonrpc": "2.0", "id": 2, "result": {}})
        slow_call = next(call for call in worker.calls() if call.name == "slow_tool")
        self.assertIsNone(slow_call.end)
        self.assertIsNone(stdio.find(1))

        started = time.monotonic()
        stdio.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}})
        arrived, cancelled = stdio.wait_for(1, 1.5)
        self.assertLess(arrived - started, 1.5)
        result = cancelled["result"]
        self.assertIs(result["isError"], True)
        self.assertEqual(json.loads(result["content"][0]["text"]), {"error": "Request cancelled"})
        sid = self.supervisor.resolve_target(None)
        status = lambda: next((j["status"] for j in self._jobs(sid) if j["tool"] == "slow_tool"), None)
        self.assertTrue(_wait_until(lambda: status() == "cancelled"))

    def test_stdio_tool_call_reaches_worker(self) -> None:
        # The open runs with an empty extension set, so only the stdio thread (no attribute at all) is under test.
        self.upstream.mcp._enabled_extensions.data = set()
        _session, worker = self._open("a.exe")
        stdio = _StdioPeer(self, self.upstream)

        stdio.send(_tool_call(7, "echo", {"x": 1}))

        _, response = stdio.wait_for(7)
        result = response["result"]
        self.assertIs(result["isError"], False, result)
        self.assertEqual(result["structuredContent"], {"tool": "echo", "arguments": {"x": 1}})
        self.assertEqual(worker.call_names()[-1], "echo")
        self.assertEqual(worker.calls()[-1].path, "/mcp")

    def test_http_fast_tool_call_not_delayed_by_watcher(self) -> None:
        _session, worker = self._open("a.exe")
        port = self._serve()

        for index in range(3):
            started = time.monotonic()
            response = self._post(port, "/mcp", _tool_call(10 + index, "echo", {"n": index}))
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 0.3)
            self.assertEqual(response["result"]["structuredContent"], {"tool": "echo", "arguments": {"n": index}})
        self.assertEqual(worker.call_names()[-3:], ["echo"] * 3)

    def test_http_ext_query_reaches_worker(self) -> None:
        _session, worker = self._open("a.exe")
        port = self._serve()

        called = self._post(port, "/mcp?ext=dbg", _tool_call(20, "echo", {"x": 1}))
        listed = self._post(port, "/mcp?ext=dbg", {"jsonrpc": "2.0", "id": 21, "method": "tools/list"})

        self.assertEqual(called["result"]["structuredContent"], {"tool": "echo", "arguments": {"x": 1}})
        names = {tool["name"] for tool in listed["result"]["tools"]}
        self.assertLessEqual({"slow_tool", "echo"}, names)
        paths = {call.method + ":" + str(call.name): call.path for call in worker.calls()}
        self.assertEqual(paths["tools/call:echo"], "/mcp?ext=dbg")
        self.assertEqual(paths["tools/list:None"], "/mcp?ext=dbg")

    def test_http_disconnect_cancels_owned_job(self) -> None:
        session, worker = self._open("a.exe")
        sid = session.session_id
        self.supervisor.sync_wait = 0.1
        ticket = self.upstream.mcp.registry.dispatch(_tool_call(50, "slow_tool", {"seconds": 1.0}))
        slow_id = ticket["result"]["_meta"]["idalib_job"]["job"]["job_id"]
        self.assertTrue(_wait_until(lambda: "slow_tool" in worker.call_names()))
        self.supervisor.sync_wait = _SYNC_WAIT
        port = self._serve()

        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=_WAIT)
        self.addCleanup(conn.close)
        conn.request("POST", "/mcp", json.dumps(_tool_call(51, "echo", {"which": "owned"})), _JSON)
        echo_status = lambda: [job["status"] for job in self._jobs(sid) if job["tool"] == "echo"]
        self.assertTrue(_wait_until(lambda: echo_status() == ["queued"]))
        conn.close()

        self.assertTrue(_wait_until(lambda: echo_status() == ["cancelled"]), echo_status())
        slow = self.supervisor.find_job(slow_id)
        self.assertTrue(slow.wait(_WAIT))
        self.assertEqual(slow.status, "ok")
        self.assertNotIn("echo", worker.call_names())


if __name__ == "__main__":
    unittest.main()
