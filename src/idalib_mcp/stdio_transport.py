"""Newline-delimited JSON-RPC over stdio, with requests handled concurrently."""

from __future__ import annotations

import json
import logging
import sys
import threading
from typing import Any, BinaryIO, Callable

logger = logging.getLogger(__name__)


def _encode_line(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8") + b"\n"


def _error_line(msg: Any, exc: BaseException) -> bytes | None:
    if not isinstance(msg, dict) or "id" not in msg:
        return None
    return _encode_line(
        {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32603, "message": f"Internal error: {exc}"}},
    )


def _is_inline(msg: Any) -> bool:
    if not isinstance(msg, dict):
        return True
    method = msg.get("method")
    return method == "initialize" or (isinstance(method, str) and method.startswith("notifications/"))


def serve_stdio(
    dispatch: Callable[[bytes], dict[str, Any] | None],
    *,
    set_transport_context: Callable[[str | None], None],
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | None = None,
    context_id: str = "stdio:default",
) -> None:
    """Serve JSON-RPC lines from stdin, running requests on daemon threads and writing whole reply lines.

    Returns on EOF, KeyboardInterrupt, a read error or a failed write, without waiting for in-flight handlers.
    """
    reader = stdin if stdin is not None else sys.stdin.buffer
    writer = stdout if stdout is not None else sys.stdout.buffer
    write_lock = threading.Lock()
    closed = threading.Event()

    def write_line(data: bytes) -> None:
        with write_lock:
            try:
                writer.write(data)
                writer.flush()
            except OSError:
                logger.debug("stdio write failed; closing transport", exc_info=True)
                closed.set()

    def handle(line: bytes, msg: Any) -> None:
        try:
            set_transport_context(context_id)
            try:
                response = dispatch(line)
            finally:
                set_transport_context(None)
            if response is None:
                return
            data = _encode_line(response)
        except Exception as exc:
            logger.exception("stdio dispatch failed")
            data = _error_line(msg, exc)
            if data is None:
                return
        write_line(data)

    def run_handler(line: bytes, msg: Any) -> None:
        try:
            handle(line, msg)
        except Exception:
            logger.exception("stdio handler failed")

    try:
        while not closed.is_set():
            try:
                raw = reader.readline()
            except (OSError, ValueError):
                logger.warning("stdio read failed; stopping", exc_info=True)
                return
            if not raw:
                return
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except (ValueError, RecursionError):
                msg = None
            if _is_inline(msg):
                run_handler(line, msg)
                continue
            try:
                threading.Thread(
                    target=run_handler,
                    args=(line, msg),
                    name=f"stdio-req-{msg.get('id')}",
                    daemon=True,
                ).start()
            except Exception as exc:
                logger.exception("stdio handler thread could not start")
                data = _error_line(msg, exc)
                if data is not None:
                    write_line(data)
    except KeyboardInterrupt:
        return
