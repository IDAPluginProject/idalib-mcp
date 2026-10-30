from __future__ import annotations

import copy
import functools
import inspect
import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Final

from .dispatcher import Job

JOB_ROUTED_TOOLS: tuple[str, ...] = ("idalib_open", "idalib_close", "idalib_save", "idalib_health", "idalib_warmup")
TICKET_MESSAGE = (
    "Still running in the background; this is not a failure. "
    "Do not repeat the call; poll with idalib_job_status."
)
EXPIRED_MESSAGE = "Job result expired (results are kept 10 minutes)"
POLL_WAIT_SECONDS = 30
WAIT_STEP = 0.1
NO_RESULT: Final[object] = object()

_MAX_WAIT_SECONDS = 30.0
_MAX_RECENT = 100


def wait_for_job(job: Job, timeout: float, cancel_event: threading.Event | None = None) -> str:
    """Wait for `job` and return "done", "timeout" or "cancelled"; done wins over cancelled."""
    if not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number")
    deadline: float | None
    if (isinstance(timeout, float) and math.isnan(timeout)) or timeout <= 0:
        deadline = time.monotonic()
    elif timeout > threading.TIMEOUT_MAX:
        deadline = None
    else:
        deadline = time.monotonic() + timeout
    while True:
        if job.is_done():
            return "done"
        if cancel_event is not None and cancel_event.is_set():
            return "cancelled"
        if deadline is None:
            job.wait(WAIT_STEP)
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout"
        job.wait(min(WAIT_STEP, remaining))


def tool_result(payload: Any, *, is_error: bool = False, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
        "isError": is_error,
    }
    if not is_error:
        result["structuredContent"] = payload if isinstance(payload, dict) else {"result": payload}
    if meta is not None:
        result["_meta"] = meta
    return result


def render_error(message: str) -> dict[str, Any]:
    return tool_result({"error": message}, is_error=True)


def render_unknown_job(job_id: Any) -> dict[str, Any]:
    return render_error(f"Unknown or expired job id: {job_id}")


def render_request_cancelled() -> dict[str, Any]:
    return render_error("Request cancelled")


def ticket_payload(info: dict[str, Any], database_state: str) -> dict[str, Any]:
    job_id = info.get("job_id")
    return {
        "pending": True,
        "message": TICKET_MESSAGE,
        "job": info,
        "database_state": database_state,
        "poll": {
            "tool": "idalib_job_status",
            "arguments": {"job_id": job_id, "wait_seconds": POLL_WAIT_SECONDS},
        },
        "cancel": {"tool": "idalib_job_cancel", "arguments": {"job_id": job_id}},
    }


def render_ticket(info: dict[str, Any], database_state: str) -> dict[str, Any]:
    ticket = ticket_payload(info, database_state)
    return tool_result(ticket, is_error=True, meta={"idalib_job": ticket})


def render_job_failed(info: dict[str, Any]) -> dict[str, Any]:
    error = info.get("error")
    if isinstance(error, str) and error:
        return render_error(error)
    return render_error("Job was cancelled" if info.get("status") == "cancelled" else "Job failed")


def render_open_result(
    stored: dict[str, Any],
    open_fields: dict[str, Any] | None,
    info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session = stored["session"]
    payload = {
        "success": True,
        **(open_fields or {}),
        "session": session,
        "message": f"Binary opened and bound to context: {session['filename']} ({session['session_id']})",
    }
    return tool_result(payload, meta=None if info is None else {"idalib_job": info})


def render_job_status(
    job_id: str,
    info: dict[str, Any],
    kind: str,
    *,
    stored: Any = NO_RESULT,
    database_state: str = "ready",
    open_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status = info.get("status")
    if not isinstance(status, str):
        return render_unknown_job(job_id)
    if status == "ok":
        if stored is NO_RESULT:
            return tool_result({"error": EXPIRED_MESSAGE, "job": info}, is_error=True)
        if kind == "open":
            return render_open_result(stored, open_fields, info)
        if kind == "rpc":
            return tool_result({"job": info})
        if isinstance(stored, dict) and isinstance(stored.get("result"), dict):
            result = copy.deepcopy(stored["result"])
            old_meta = result.get("_meta")
            result["_meta"] = {**(old_meta if isinstance(old_meta, dict) else {}), "idalib_job": info}
            return result
        error = stored.get("error") if isinstance(stored, dict) else None
        if isinstance(error, dict) and "message" in error:
            return render_error(str(error["message"]))
        return render_error("Job returned no result")
    if status == "error":
        return tool_result({"error": info.get("error"), "job": info}, is_error=True)
    if status == "cancelled":
        return tool_result({"error": "Job was cancelled", "job": info}, is_error=True)
    return tool_result(ticket_payload(info, database_state))


def render_jobs(databases: list[dict[str, Any]]) -> dict[str, Any]:
    return tool_result({"databases": databases, "count": len(databases)})


def render_cancel(job_id: str, outcome: str, kind: str, info: dict[str, Any]) -> dict[str, Any]:
    if outcome == "unknown":
        return render_unknown_job(job_id)
    if outcome == "removed":
        message = "Removed from the queue; the job will not run."
    elif outcome == "cancelling":
        if kind == "open":
            message = "Cancelling: the opening database's worker is being terminated."
        else:
            message = "Cancelling: the worker finishes the current call, then the result is discarded."
    elif outcome == "already_cancelling":
        message = "Cancellation was already requested; the job ends when the worker returns."
    elif outcome == "finished":
        if kind == "open":
            message = (
                "The job had already finished; nothing was cancelled. "
                "An opened database stays open; use idalib_close to close it."
            )
        else:
            message = "The job had already finished; nothing was cancelled."
    else:
        message = f"Cancel outcome: {outcome}"
    return tool_result({"job_id": job_id, "outcome": outcome, "message": message, "job": info})


class JobArgumentError(ValueError):
    """An invalid job-tool argument; str(exc) is the full message."""


def _check_unexpected(arguments: Any, allowed: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        raise JobArgumentError("Invalid params: arguments must be an object")
    extra = [key for key in arguments if key not in allowed]
    if extra:
        raise JobArgumentError(f"Invalid params: unexpected parameters: {sorted(extra, key=str)}")
    return arguments


def _require_job_id(arguments: dict[str, Any]) -> str:
    job_id = arguments.get("job_id")
    if not isinstance(job_id, str):
        raise JobArgumentError("Invalid params: job_id must be a string")
    return job_id


def parse_job_status_arguments(arguments: dict[str, Any]) -> tuple[str, float]:
    arguments = _check_unexpected(arguments, ("job_id", "wait_seconds"))
    job_id = _require_job_id(arguments)
    wait = arguments.get("wait_seconds", POLL_WAIT_SECONDS)
    if isinstance(wait, bool) or not isinstance(wait, (int, float)):
        raise JobArgumentError("Invalid params: wait_seconds must be a number")
    if isinstance(wait, float) and math.isnan(wait):
        return job_id, 0.0
    return job_id, float(max(0, min(_MAX_WAIT_SECONDS, wait)))


def parse_jobs_arguments(arguments: dict[str, Any]) -> tuple[str | None, int]:
    arguments = _check_unexpected(arguments, ("database", "recent"))
    database = arguments.get("database")
    if database is not None and not isinstance(database, str):
        raise JobArgumentError("Invalid params: database must be a string or null")
    recent = arguments.get("recent", 20)
    if isinstance(recent, bool) or not isinstance(recent, int):
        raise JobArgumentError("Invalid params: recent must be an integer")
    return (database or None), max(0, min(_MAX_RECENT, recent))


def parse_job_cancel_arguments(arguments: dict[str, Any]) -> str:
    arguments = _check_unexpected(arguments, ("job_id",))
    return _require_job_id(arguments)


@dataclass(frozen=True)
class ToolsCall:
    name: str
    arguments: dict[str, Any]
    meta: dict[str, Any] | None


@dataclass(frozen=True)
class Rejected:
    response: dict[str, Any] | None


class ToolsCallValidator:
    """Validates a `tools/call` request exactly as zeromcp would, without running any tool."""

    def __init__(self, upstream: Any) -> None:
        self._local = threading.local()
        registry_type = type(upstream.mcp.registry)
        self._outer = registry_type()
        self._outer.methods["tools/call"] = self._stub(upstream.mcp._mcp_tools_call)
        self._inner = registry_type()
        self._defaults: dict[str, list[tuple[str, Any]]] = {}
        for name in JOB_ROUTED_TOOLS:
            func = getattr(upstream, name)
            self._inner.methods[name] = self._stub(func)
            self._defaults[name] = [
                (param.name, param.default) for param in inspect.signature(func).parameters.values()
            ]

    def _stub(self, target: Any) -> Any:
        local = self._local

        @functools.wraps(target)
        def capture(**kwargs: Any) -> dict[str, Any]:
            local.captured = kwargs
            return {}

        return capture

    def _run(self, registry: Any, request: dict[str, Any]) -> tuple[Any, dict[str, Any] | None]:
        self._local.captured = None
        response = registry.dispatch(request)
        captured = self._local.captured
        self._local.captured = None
        return response, captured

    def __call__(self, request_obj: dict[str, Any]) -> ToolsCall | Rejected:
        if not isinstance(request_obj, dict):
            raise TypeError("request_obj must be a dict")
        response, captured = self._run(self._outer, request_obj)
        if captured is None:
            return Rejected(response)
        name = captured["name"]
        arguments = captured.get("arguments")
        meta = captured.get("_meta")
        if name not in JOB_ROUTED_TOOLS:
            return ToolsCall(name, dict(arguments or {}), meta)

        inner_request = {"jsonrpc": "2.0", "method": name, "params": arguments, "id": None}
        inner_response, inner_captured = self._run(self._inner, inner_request)
        if inner_captured is None:
            if "id" not in request_obj:
                return Rejected(None)
            error = (inner_response or {}).get("error") or {}
            return Rejected(
                {
                    "jsonrpc": "2.0",
                    "result": {
                        "content": [{"type": "text", "text": error.get("message", "Unknown error")}],
                        "isError": True,
                    },
                    "id": request_obj["id"],
                }
            )
        filled = {
            param: inner_captured[param] if param in inner_captured else default
            for param, default in self._defaults[name]
        }
        return ToolsCall(name, filled, meta)
