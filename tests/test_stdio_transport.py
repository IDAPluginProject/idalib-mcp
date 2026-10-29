from __future__ import annotations

import json
import os
import threading
import time
import unittest
from typing import Any, Callable

from idalib_mcp.stdio_transport import serve_stdio

LOGGER_NAME = "idalib_mcp.stdio_transport"
WAIT = 5.0


def _wait_until(predicate: Callable[[], bool], timeout: float = WAIT) -> bool:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.005)
    return True


def _reply(msg: dict[str, Any], result: Any = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {} if result is None else result}


class _Sink:
    """Fake stdout; optionally splits each write into chunks with a yield between them."""

    def __init__(self, log: list[tuple[str, Any]] | None = None, chunk: int | None = None) -> None:
        self._cond = threading.Condition()
        self._data = bytearray()
        self._log = log
        self._chunk = chunk

    def write(self, data: bytes) -> int:
        if self._chunk is None:
            with self._cond:
                self._data += data
                if self._log is not None:
                    self._log.append(("write", json.loads(data).get("id")))
                self._cond.notify_all()
            return len(data)
        for start in range(0, len(data), self._chunk):
            with self._cond:
                self._data += data[start:start + self._chunk]
                self._cond.notify_all()
            time.sleep(0)
        return len(data)

    def flush(self) -> None:
        pass

    def raw_lines(self) -> list[bytes]:
        with self._cond:
            return bytes(self._data).split(b"\n")[:-1]

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.raw_lines()]

    def wait_lines(self, count: int, timeout: float = WAIT) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self._data.count(b"\n") >= count, timeout)

    def wait_for_id(self, msg_id: Any, timeout: float = WAIT) -> bool:
        return _wait_until(lambda: any(m.get("id") == msg_id for m in self.lines()), timeout)


class _Transport:
    """Runs serve_stdio on a thread, fed through an OS pipe so reads block as in production."""

    def __init__(
        self,
        test: unittest.TestCase,
        dispatch: Callable[[bytes], dict[str, Any] | None],
        stdout: Any,
        set_context: Callable[[str | None], None] | None = None,
    ) -> None:
        read_fd, write_fd = os.pipe()
        self.stdin = os.fdopen(read_fd, "rb")
        self.pipe = os.fdopen(write_fd, "wb", buffering=0)
        self.thread = threading.Thread(
            target=serve_stdio,
            args=(dispatch,),
            kwargs={
                "set_transport_context": set_context or (lambda value: None),
                "stdin": self.stdin,
                "stdout": stdout,
            },
            name="stdio-reader-test",
            daemon=True,
        )
        self.thread.start()
        test.addCleanup(self.stop)

    def send(self, *messages: dict[str, Any] | bytes) -> None:
        for message in messages:
            data = message if isinstance(message, bytes) else json.dumps(message).encode("utf-8")
            self.pipe.write(data + b"\n")

    def stop(self) -> None:
        if not self.pipe.closed:
            self.pipe.close()
        self.thread.join(WAIT)
        self.stdin.close()


class InlineOrderingTests(unittest.TestCase):
    def test_slow_initialize_answered_before_next_dispatch(self) -> None:
        log: list[tuple[str, Any]] = []

        def dispatch(line: bytes) -> dict[str, Any] | None:
            msg = json.loads(line)
            log.append(("dispatch", msg["method"]))
            if msg["method"] == "initialize":
                time.sleep(0.3)
            return _reply(msg)

        sink = _Sink(log=log)
        transport = _Transport(self, dispatch, sink)
        transport.send(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
        )

        self.assertTrue(sink.wait_lines(2))
        self.assertLess(log.index(("write", 1)), log.index(("dispatch", "ping")))

    def test_slow_initialized_notification_runs_inline(self) -> None:
        log: list[tuple[str, Any]] = []

        def dispatch(line: bytes) -> dict[str, Any] | None:
            msg = json.loads(line)
            log.append(("dispatch", msg["method"]))
            if msg["method"] == "notifications/initialized":
                time.sleep(0.3)
                log.append(("done", msg["method"]))
                return None
            return _reply(msg)

        sink = _Sink()
        transport = _Transport(self, dispatch, sink)
        transport.send(
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
        )

        self.assertTrue(sink.wait_lines(1))
        self.assertLess(
            log.index(("done", "notifications/initialized")),
            log.index(("dispatch", "ping")),
        )


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release = threading.Event()
        self.slow_done = threading.Event()
        self.notified = threading.Event()
        self.threads: dict[str, str] = {}
        self.addCleanup(self.release.set)

    def _dispatch(self, line: bytes) -> dict[str, Any] | None:
        msg = json.loads(line)
        method = msg["method"]
        self.threads[method] = threading.current_thread().name
        if method == "slow":
            self.release.wait(1.0)
            self.slow_done.set()
            return _reply(msg) if "id" in msg else None
        if method.startswith("notifications/"):
            self.notified.set()
            return None
        return _reply(msg)

    def test_slow_request_does_not_delay_ping(self) -> None:
        sink = _Sink()
        transport = _Transport(self, self._dispatch, sink)
        transport.send(
            {"jsonrpc": "2.0", "id": 1, "method": "slow"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
        )

        self.assertTrue(sink.wait_lines(2))
        self.assertEqual([m["id"] for m in sink.lines()], [2, 1])
        self.assertEqual(self.threads["slow"], "stdio-req-1")
        self.assertEqual(self.threads["ping"], "stdio-req-2")

    def test_slow_message_without_id_runs_threaded(self) -> None:
        sink = _Sink()
        transport = _Transport(self, self._dispatch, sink)
        transport.send(
            {"jsonrpc": "2.0", "method": "slow"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
        )

        self.assertTrue(sink.wait_lines(1, timeout=0.8))
        self.assertFalse(self.slow_done.is_set())
        self.assertEqual(sink.lines()[0]["id"], 2)
        self.assertEqual(self.threads["slow"], "stdio-req-None")

    def test_notification_dispatched_while_request_runs(self) -> None:
        sink = _Sink()
        transport = _Transport(self, self._dispatch, sink)
        transport.send(
            {"jsonrpc": "2.0", "id": 1, "method": "slow"},
            {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}},
        )

        self.assertTrue(self.notified.wait(0.8))
        self.assertFalse(self.slow_done.is_set())
        self.assertEqual(self.threads["notifications/cancelled"], "stdio-reader-test")

    def test_concurrent_large_responses_are_whole_lines(self) -> None:
        count = 20
        barrier = threading.Barrier(count)
        payload = "x" * 65536

        def dispatch(line: bytes) -> dict[str, Any] | None:
            msg = json.loads(line)
            barrier.wait(WAIT)
            return _reply(msg, {"data": payload})

        sink = _Sink(chunk=4096)
        transport = _Transport(self, dispatch, sink)
        transport.send(*({"jsonrpc": "2.0", "id": i, "method": "big"} for i in range(count)))

        self.assertTrue(sink.wait_lines(count, timeout=10.0))
        lines = sink.raw_lines()
        self.assertEqual(len(lines), count)
        messages = [json.loads(line) for line in lines]
        self.assertEqual(sorted(m["id"] for m in messages), list(range(count)))
        self.assertTrue(all(m["result"]["data"] == payload for m in messages))


class ContextAndErrorTests(unittest.TestCase):
    def test_transport_context_set_during_dispatch(self) -> None:
        local = threading.local()
        calls: list[tuple[str, str | None]] = []
        seen: dict[str, Any] = {}

        def set_context(value: str | None) -> None:
            calls.append((threading.current_thread().name, value))
            local.data = value

        def dispatch(line: bytes) -> dict[str, Any] | None:
            msg = json.loads(line)
            seen[threading.current_thread().name] = getattr(local, "data", "unset")
            if msg["method"] == "boom":
                raise RuntimeError("boom")
            return None if "id" not in msg else _reply(msg)

        sink = _Sink()
        transport = _Transport(self, dispatch, sink, set_context=set_context)
        with self.assertLogs(LOGGER_NAME, level="ERROR"):
            transport.send(
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                {"jsonrpc": "2.0", "id": 2, "method": "boom"},
            )
            self.assertTrue(sink.wait_lines(2))

        expected = ["stdio:default", None]
        for name in ("stdio-reader-test", "stdio-req-1", "stdio-req-2"):
            self.assertTrue(
                _wait_until(lambda: [v for n, v in calls if n == name] == expected),
                f"{name}: {[v for n, v in calls if n == name]}",
            )
            self.assertEqual(seen[name], "stdio:default")

    def test_raising_dispatch_returns_internal_error(self) -> None:
        def dispatch(line: bytes) -> dict[str, Any] | None:
            if line == b"not json":
                raise ValueError("unparsed")
            msg = json.loads(line)
            if msg["method"] == "ping":
                return _reply(msg)
            raise RuntimeError("boom")

        sink = _Sink()
        transport = _Transport(self, dispatch, sink)
        with self.assertLogs(LOGGER_NAME, level="ERROR") as logs:
            transport.send(
                b"not json",
                {"jsonrpc": "2.0", "method": "notifications/cancelled"},
                {"jsonrpc": "2.0", "id": 7, "method": "tools/call"},
                {"jsonrpc": "2.0", "id": 8, "method": "ping"},
            )
            self.assertTrue(sink.wait_for_id(7))
            self.assertTrue(sink.wait_for_id(8))

        self.assertEqual(len(logs.records), 3)
        lines = sink.lines()
        self.assertEqual(len(lines), 2)
        self.assertIn(
            {"jsonrpc": "2.0", "id": 7, "error": {"code": -32603, "message": "Internal error: boom"}},
            lines,
        )

    def test_unserialisable_return_gives_internal_error(self) -> None:
        def dispatch(line: bytes) -> dict[str, Any] | None:
            return _reply(json.loads(line), {"value": object()})

        sink = _Sink()
        transport = _Transport(self, dispatch, sink)
        with self.assertLogs(LOGGER_NAME, level="ERROR"):
            transport.send({"jsonrpc": "2.0", "id": "abc", "method": "tools/call"})
            self.assertTrue(sink.wait_lines(1))

        (line,) = sink.lines()
        self.assertEqual(line["id"], "abc")
        self.assertEqual(line["error"]["code"], -32603)
        self.assertTrue(line["error"]["message"].startswith("Internal error: "))

    def test_inline_write_value_error_keeps_reader(self) -> None:
        class _RaisingSink(_Sink):
            def write(self, data: bytes) -> int:
                if json.loads(data).get("id") == 1:
                    raise ValueError("I/O operation on closed file")
                return super().write(data)

        sink = _RaisingSink()
        transport = _Transport(self, lambda line: _reply(json.loads(line)), sink)
        with self.assertLogs(LOGGER_NAME, level="ERROR"):
            transport.send(
                {"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                {"jsonrpc": "2.0", "id": 2, "method": "ping"},
            )
            self.assertTrue(sink.wait_for_id(2))

        self.assertTrue(transport.thread.is_alive())
        self.assertEqual([m["id"] for m in sink.lines()], [2])


class ShutdownTests(unittest.TestCase):
    def test_eof_returns(self) -> None:
        transport = _Transport(self, lambda line: None, _Sink())
        transport.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        transport.pipe.close()

        transport.thread.join(WAIT)
        self.assertFalse(transport.thread.is_alive())

    def test_readline_error_returns(self) -> None:
        class _FailingStdin:
            def __init__(self, exc: BaseException) -> None:
                self.exc = exc

            def readline(self) -> bytes:
                raise self.exc

        for exc in (OSError("read failed"), ValueError("I/O operation on closed file")):
            with self.subTest(exc=type(exc).__name__):
                thread = threading.Thread(
                    target=serve_stdio,
                    args=(lambda line: None,),
                    kwargs={
                        "set_transport_context": lambda value: None,
                        "stdin": _FailingStdin(exc),
                        "stdout": _Sink(),
                    },
                    daemon=True,
                )
                with self.assertLogs(LOGGER_NAME, level="WARNING"):
                    thread.start()
                    thread.join(WAIT)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
