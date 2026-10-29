"""Fake idalib workers for supervisor tests: real sockets, no IDA, no subprocess."""

from __future__ import annotations

import contextlib
import itertools
import json
import logging
import os
import socket
import subprocess
import threading
import time
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Iterator

logger = logging.getLogger(__name__)

_HOST = "127.0.0.1"
_STEP = 0.02
_MAX_WAIT = 60.0
_JOIN_TIMEOUT = 5.0
_CONN_TIMEOUT = 5.0
_SPAWN_GATE_TIMEOUT = 30.0
_PIDS = itertools.count(900000)
_PID_LOCK = threading.Lock()


@dataclass
class FakeCall:
    method: str
    name: str | None
    arguments: Any
    path: str
    start: float
    end: float | None


class _Killed(Exception):
    """Raised inside a handler when the worker is killed mid-call."""


class FakeProcess:
    """Duck-types subprocess.Popen for the supervisor's worker handling."""

    def __init__(self, worker: FakeWorker) -> None:
        with _PID_LOCK:
            self.pid: int = next(_PIDS)
        self.returncode: int | None = None
        self._worker = worker
        self._dead = threading.Event()

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.kill()

    def kill(self) -> None:
        self._worker.kill()

    def wait(self, timeout: float | None = None) -> int:
        bound = _MAX_WAIT if timeout is None else timeout
        if not self._dead.wait(bound):
            raise subprocess.TimeoutExpired("fakeworker", bound)
        assert self.returncode is not None
        return self.returncode

    def _exit(self) -> None:
        self.returncode = -9
        self._dead.set()


class _FakeHttpServer(HTTPServer):
    allow_reuse_address = False
    request_queue_size = 64
    worker: FakeWorker

    def handle_error(self, request: Any, client_address: Any) -> None:
        logger.debug("fake worker request failed", exc_info=True)


class _Handler(BaseHTTPRequestHandler):
    timeout = _CONN_TIMEOUT

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_POST(self) -> None:
        worker: FakeWorker = self.server.worker  # type: ignore[attr-defined]
        start = time.monotonic()
        try:
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            try:
                request = json.loads(raw)
            except ValueError:
                request = None
            if not isinstance(request, dict):
                self._reply({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None})
                return
            method = request.get("method")
            params = request.get("params")
            name: str | None = None
            arguments: Any = params
            if method == "tools/call":
                params = params if isinstance(params, dict) else {}
                name = params.get("name")
                arguments = params.get("arguments", {})
            call = FakeCall(str(method), name, arguments, self.path, start, None)
            worker._record(call)
            response = worker._respond(request, name, arguments)
            if worker._killed.is_set():
                raise _Killed
            self._reply(response)
            call.end = time.monotonic()
        except (_Killed, TimeoutError):
            self.close_connection = True
            with contextlib.suppress(OSError):
                self.connection.shutdown(socket.SHUT_RDWR)

    def _reply(self, response: dict[str, Any]) -> None:
        # No Content-Length: the client's read ends at close, after `end` is stamped.
        body = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)


def _tool_result(obj: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(obj)}], "structuredContent": obj, "isError": False}


class FakeWorker:
    def __init__(self, *, open_delay: float = 0.0, open_gate: threading.Event | None = None) -> None:
        self.host = _HOST
        self._open_delay = open_delay
        self._open_gate = open_gate
        self._lock = threading.Lock()
        self._calls: list[FakeCall] = []
        self._killed = threading.Event()
        self._closed = threading.Event()
        self._thread: threading.Thread | None = None
        self._server = _FakeHttpServer((_HOST, 0), _Handler)
        self._server.worker = self
        self.port: int = self._server.server_address[1]
        self.process = FakeProcess(self)

    def start(self) -> None:
        with self._lock:
            if self._killed.is_set():
                raise RuntimeError("FakeWorker was killed")
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._serve, name=f"fakeworker-{self.port}", daemon=True)
            self._thread.start()

    def kill(self) -> None:
        with self._lock:
            self._killed.set()
            self.process._exit()
            thread = self._thread
            if thread is None and not self._closed.is_set():
                self._server.server_close()
                self._closed.set()
        if thread is not None and thread is not threading.current_thread():
            self._closed.wait(_JOIN_TIMEOUT)

    def calls(self) -> list[FakeCall]:
        with self._lock:
            return list(self._calls)

    def call_names(self) -> list[str]:
        return [call.name if call.method == "tools/call" and call.name else call.method for call in self.calls()]

    def _serve(self) -> None:
        self._server.timeout = _STEP
        try:
            while not self._killed.is_set():
                self._server.handle_request()
        finally:
            self._server.server_close()
            self._closed.set()

    def _record(self, call: FakeCall) -> None:
        with self._lock:
            self._calls.append(call)

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while True:
            if self._killed.is_set():
                raise _Killed
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(_STEP, remaining))

    def _wait_gate(self, gate: threading.Event) -> None:
        deadline = time.monotonic() + _MAX_WAIT
        while not gate.wait(_STEP):
            if self._killed.is_set():
                raise _Killed
            if time.monotonic() >= deadline:
                raise TimeoutError("open_gate was not released")

    def _respond(self, request: dict[str, Any], name: str | None, arguments: Any) -> dict[str, Any]:
        rid = request.get("id")
        method = request.get("method")
        if method == "ping":
            result: Any = {}
        elif method == "tools/list":
            result = {"tools": [_tool_schema("slow_tool", {"seconds": {"type": "number"}}), _tool_schema("echo", {})]}
        elif method == "tools/call":
            args = arguments if isinstance(arguments, dict) else {}
            try:
                result = self._call_tool(name, arguments, args)
            except (TypeError, ValueError):
                return {"jsonrpc": "2.0", "error": {"code": -32602, "message": "Invalid params"}, "id": rid}
        elif method == "resources/read":
            params = request.get("params")
            uri = params.get("uri") if isinstance(params, dict) else None
            result = {"contents": [{"uri": uri, "mimeType": "application/json", "text": "{}"}]}
        else:
            return {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Method not found: {method}"}, "id": rid}
        return {"jsonrpc": "2.0", "result": result, "id": rid}

    def _call_tool(self, name: str | None, arguments: Any, args: dict[str, Any]) -> dict[str, Any]:
        if name == "idalib_open":
            self._sleep(self._open_delay)
            if self._open_gate is not None:
                self._wait_gate(self._open_gate)
            input_path = args.get("input_path")
            now = datetime.now().isoformat()
            return _tool_result(
                {
                    "success": True,
                    "session": {
                        "session_id": args.get("session_id"),
                        "input_path": input_path,
                        "filename": os.path.basename(str(input_path or "")),
                        "created_at": now,
                        "last_accessed": now,
                        "is_analyzing": False,
                        "metadata": {},
                    },
                }
            )
        if name == "idalib_close":
            return _tool_result({"success": True})
        if name == "idalib_save":
            return _tool_result({"ok": True, "path": args.get("path") or "fake.i64"})
        if name == "idalib_health":
            return _tool_result({"ready": True, "session": None, "health": {"ok": True}, "error": None})
        if name == "idalib_warmup":
            return _tool_result({"ready": True, "session": None, "warmup": {"ok": True}, "error": None})
        if name == "slow_tool":
            seconds = args.get("seconds", 0)
            self._sleep(float(seconds))
            return _tool_result({"slept": seconds})
        return _tool_result({"tool": name, "arguments": arguments})


def _tool_schema(name: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "description": f"Fake {name}",
        "inputSchema": {"type": "object", "properties": properties},
    }


class FakeWorkerFactory:
    """Instance replacement for supervisor._start_worker_process."""

    def __init__(
        self,
        upstream: Any,
        *,
        open_delay: float = 0.0,
        open_gate: threading.Event | None = None,
        spawn_gate: threading.Event | None = None,
    ) -> None:
        self._upstream = upstream
        self.workers: list[FakeWorker] = []
        self.spawn_threads: list[str] = []
        self.open_delay = open_delay
        self.open_gate = open_gate
        self.spawn_gate = spawn_gate
        self._lock = threading.Lock()
        self._generation = 0

    def __call__(self) -> Any:
        with self._lock:
            self.spawn_threads.append(threading.current_thread().name)
            generation = self._generation
        gate = self.spawn_gate
        if gate is not None:
            deadline = time.monotonic() + _SPAWN_GATE_TIMEOUT
            while not gate.wait(_STEP):
                if self._generation != generation:
                    raise RuntimeError("FakeWorkerFactory stopped while waiting for spawn_gate")
                if time.monotonic() >= deadline:
                    raise TimeoutError("spawn_gate was not released")
        worker = FakeWorker(open_delay=self.open_delay, open_gate=self.open_gate)
        with self._lock:
            self.workers.append(worker)
        worker.start()
        return self._upstream.WorkerSession(
            session_id=f"__worker_schema_{uuid.uuid4().hex[:8]}",
            input_path="",
            filename="",
            host=worker.host,
            port=worker.port,
            process=worker.process,
            backend="worker",
            owned=True,
            pid=worker.process.pid,
        )

    def stop_all(self) -> None:
        with self._lock:
            self._generation += 1
            workers = list(self.workers)
        for worker in workers:
            worker.kill()


def attach_fakes(supervisor: Any, factory: FakeWorkerFactory) -> None:
    supervisor._start_worker_process = factory
    supervisor._find_gui_instance_for_path = lambda resolved_path: None


def make_supervisor(
    test: unittest.TestCase,
    upstream: Any,
    factory: FakeWorkerFactory,
    *,
    max_workers: int = 4,
    isolated_contexts: bool = False,
    sync_wait: float = 15.0,
) -> Any:
    from idalib_mcp.server import _build_managed_supervisor_class

    supervisor = _build_managed_supervisor_class(upstream)(
        upstream.mcp,
        isolated_contexts=isolated_contexts,
        max_workers=max_workers,
        worker_args=[],
        sync_wait=sync_wait,
    )
    attach_fakes(supervisor, factory)
    test.addCleanup(factory.stop_all)
    test.addCleanup(supervisor.shutdown)
    return supervisor


@contextlib.contextmanager
def preserved_upstream(upstream: Any) -> Iterator[None]:
    mcp = upstream.mcp
    registry = mcp.registry
    saved_supervisor = upstream.supervisor
    saved_tools_call = upstream._handle_tools_call
    saved_tools_list = upstream._handle_tools_list
    had_dispatch = "dispatch" in vars(registry)
    saved_dispatch = vars(registry).get("dispatch")
    saved_management = set(upstream.IDALIB_MANAGEMENT_TOOLS)
    saved_methods = dict(mcp.tools.methods)
    saved_require_session = mcp.require_streamable_http_session
    saved_http_server = mcp._http_server
    missing = object()
    saved_locals = {
        local_name: getattr(getattr(mcp, local_name), "data", missing)
        for local_name in ("_transport_session_id", "_enabled_extensions")
    }
    try:
        yield
    finally:
        try:
            installed = upstream.supervisor
            if installed is not saved_supervisor and installed is not None:
                try:
                    installed.shutdown()
                except Exception:
                    logger.exception("supervisor shutdown failed while restoring upstream")
            if mcp._http_server is not None and mcp._http_server is not saved_http_server:
                mcp.stop()
        finally:
            upstream.supervisor = saved_supervisor
            upstream._handle_tools_call = saved_tools_call
            upstream._handle_tools_list = saved_tools_list
            if had_dispatch:
                registry.dispatch = saved_dispatch
            else:
                vars(registry).pop("dispatch", None)
            upstream.IDALIB_MANAGEMENT_TOOLS.clear()
            upstream.IDALIB_MANAGEMENT_TOOLS.update(saved_management)
            mcp.tools.methods.clear()
            mcp.tools.methods.update(saved_methods)
            mcp.require_streamable_http_session = saved_require_session
            for local_name, value in saved_locals.items():
                local = getattr(mcp, local_name)
                if value is missing:
                    if hasattr(local, "data"):
                        del local.data
                else:
                    local.data = value
