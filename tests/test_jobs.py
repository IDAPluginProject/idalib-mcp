from __future__ import annotations

import importlib
import json
import math
import sys
import threading
import time
import unittest
from typing import Any

from idalib_mcp import jobs
from idalib_mcp.dispatcher import WorkerDispatcher
from idalib_mcp.jobs import (
    EXPIRED_MESSAGE,
    NO_RESULT,
    TICKET_MESSAGE,
    JobArgumentError,
    Rejected,
    ToolsCall,
    ToolsCallValidator,
    parse_job_cancel_arguments,
    parse_job_status_arguments,
    parse_jobs_arguments,
    render_cancel,
    render_error,
    render_job_failed,
    render_job_status,
    render_jobs,
    render_open_result,
    render_request_cancelled,
    render_ticket,
    render_unknown_job,
    ticket_payload,
    tool_result,
    wait_for_job,
)


def _upstream() -> Any:
    return importlib.import_module("ida_pro_mcp.idalib_supervisor")


def _info(**overrides: Any) -> dict[str, Any]:
    info: dict[str, Any] = {
        "job_id": "abc123def456",
        "database": "s1",
        "tool": "slow_tool",
        "kind": "tool",
        "status": "running",
        "phase": None,
    }
    info.update(overrides)
    return info


class ExactMixin(unittest.TestCase):
    def assertExact(self, actual: Any, expected: Any) -> None:
        self.assertEqual(actual, expected)
        self.assertEqual(json.dumps(actual), json.dumps(expected))


class FakeJob:
    """Duck-types the two Job methods wait_for_job uses; `on_wait` runs inside each wait()."""

    def __init__(self, done: bool = False, on_wait: Any = None) -> None:
        self.done = done
        self.waits: list[float] = []
        self.on_wait = on_wait

    def is_done(self) -> bool:
        return self.done

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)  # type: ignore[arg-type]
        if self.on_wait is not None:
            self.on_wait(self)
        return self.done


class WaitForJobTests(unittest.TestCase):
    def test_zero_negative_and_nan_check_once_without_blocking(self) -> None:
        for timeout in (0, 0.0, -1, -0.5, -math.inf, math.nan):
            with self.subTest(timeout=timeout):
                job = FakeJob()
                self.assertEqual(wait_for_job(job, timeout), "timeout")  # type: ignore[arg-type]
                self.assertEqual(job.waits, [])

    def test_zero_timeout_on_real_pending_job_returns_at_once(self) -> None:
        dispatcher = WorkerDispatcher("w")
        release = threading.Event()
        self.addCleanup(dispatcher.close, "test over")
        self.addCleanup(release.set)
        job = dispatcher.submit("t", 1, {}, lambda _job: release.wait(5))
        started = time.monotonic()
        self.assertEqual(wait_for_job(job, 0), "timeout")
        self.assertLess(time.monotonic() - started, 0.5)

    def test_done_wins_over_cancelled_and_timeout(self) -> None:
        event = threading.Event()
        event.set()
        for timeout in (0, math.nan, 5, math.inf):
            with self.subTest(timeout=timeout):
                self.assertEqual(wait_for_job(FakeJob(done=True), timeout, event), "done")  # type: ignore[arg-type]

    def test_cancelled_beats_timeout_even_for_zero(self) -> None:
        event = threading.Event()
        event.set()
        job = FakeJob()
        self.assertEqual(wait_for_job(job, 0, event), "cancelled")  # type: ignore[arg-type]
        self.assertEqual(wait_for_job(job, 5, event), "cancelled")  # type: ignore[arg-type]
        self.assertEqual(job.waits, [])

    def test_cancel_set_during_wait_returns_cancelled(self) -> None:
        event = threading.Event()
        job = FakeJob(on_wait=lambda _job: event.set())
        self.assertEqual(wait_for_job(job, 30, event), "cancelled")  # type: ignore[arg-type]
        self.assertEqual(len(job.waits), 1)

    def test_unset_cancel_event_does_not_cancel(self) -> None:
        job = FakeJob(on_wait=lambda j: setattr(j, "done", True))
        self.assertEqual(wait_for_job(job, 30, threading.Event()), "done")  # type: ignore[arg-type]

    def test_finite_timeout_expires_in_steps_of_at_most_wait_step(self) -> None:
        job = FakeJob(on_wait=lambda j: time.sleep(j.waits[-1]))
        started = time.monotonic()
        self.assertEqual(wait_for_job(job, 0.25), "timeout")  # type: ignore[arg-type]
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.24)
        self.assertLess(elapsed, 2.0)
        self.assertGreaterEqual(len(job.waits), 3)
        for step in job.waits:
            self.assertGreater(step, 0)
            self.assertLessEqual(step, jobs.WAIT_STEP)

    def test_finite_timeout_below_wait_step_waits_only_the_remainder(self) -> None:
        job = FakeJob(on_wait=lambda j: time.sleep(j.waits[-1]))
        self.assertEqual(wait_for_job(job, 0.03), "timeout")  # type: ignore[arg-type]
        self.assertTrue(all(step <= 0.03 for step in job.waits))

    def test_job_finishing_during_last_step_is_done_not_timeout(self) -> None:
        def finish_late(job: FakeJob) -> None:
            time.sleep(0.08)
            job.done = True

        self.assertEqual(wait_for_job(FakeJob(on_wait=finish_late), 0.03), "done")  # type: ignore[arg-type]

    def test_infinite_and_oversized_timeouts_have_no_deadline(self) -> None:
        for timeout in (math.inf, threading.TIMEOUT_MAX + 1, sys.float_info.max, 10**400):
            with self.subTest(timeout=timeout):
                job = FakeJob(on_wait=lambda j: setattr(j, "done", len(j.waits) >= 4))
                self.assertEqual(wait_for_job(job, timeout), "done")  # type: ignore[arg-type]
                self.assertEqual(job.waits, [jobs.WAIT_STEP] * 4)

    def test_timeout_max_itself_is_a_deadline(self) -> None:
        job = FakeJob(on_wait=lambda j: setattr(j, "done", True))
        self.assertEqual(wait_for_job(job, threading.TIMEOUT_MAX), "done")  # type: ignore[arg-type]
        self.assertEqual(job.waits, [jobs.WAIT_STEP])

    def test_non_numeric_timeout_raises_type_error(self) -> None:
        for timeout in ("1", None, [1], b"1"):
            with self.subTest(timeout=timeout):
                job = FakeJob(done=True)
                with self.assertRaises(TypeError):
                    wait_for_job(job, timeout)  # type: ignore[arg-type]

    def test_returns_promptly_when_real_job_finishes_mid_wait(self) -> None:
        dispatcher = WorkerDispatcher("w")
        release = threading.Event()
        self.addCleanup(dispatcher.close, "test over")
        self.addCleanup(release.set)
        job = dispatcher.submit("t", 1, {}, lambda _job: release.wait(5) and "v")
        threading.Timer(0.2, release.set).start()
        started = time.monotonic()
        self.assertEqual(wait_for_job(job, 5), "done")
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertTrue(job.is_done())

    def test_real_job_not_finishing_times_out(self) -> None:
        dispatcher = WorkerDispatcher("w")
        release = threading.Event()
        self.addCleanup(dispatcher.close, "test over")
        self.addCleanup(release.set)
        job = dispatcher.submit("t", 1, {}, lambda _job: release.wait(5))
        started = time.monotonic()
        self.assertEqual(wait_for_job(job, 0.2), "timeout")
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertFalse(job.is_done())


class ToolResultTests(ExactMixin):
    def test_dict_payload(self) -> None:
        self.assertExact(
            tool_result({"a": 1, "b": [1, 2]}),
            {
                "content": [{"type": "text", "text": '{"a":1,"b":[1,2]}'}],
                "isError": False,
                "structuredContent": {"a": 1, "b": [1, 2]},
            },
        )

    def test_non_dict_payload_is_wrapped_in_structured_content(self) -> None:
        self.assertExact(
            tool_result([1, "x"]),
            {
                "content": [{"type": "text", "text": '[1,"x"]'}],
                "isError": False,
                "structuredContent": {"result": [1, "x"]},
            },
        )

    def test_none_payload(self) -> None:
        self.assertExact(
            tool_result(None),
            {"content": [{"type": "text", "text": "null"}], "isError": False, "structuredContent": {"result": None}},
        )

    def test_error_has_no_structured_content(self) -> None:
        self.assertExact(
            tool_result({"error": "x"}, is_error=True),
            {"content": [{"type": "text", "text": '{"error":"x"}'}], "isError": True},
        )

    def test_meta_is_appended_last_and_only_when_not_none(self) -> None:
        self.assertExact(
            tool_result({"a": 1}, meta={"m": 1}),
            {
                "content": [{"type": "text", "text": '{"a":1}'}],
                "isError": False,
                "structuredContent": {"a": 1},
                "_meta": {"m": 1},
            },
        )
        self.assertExact(
            tool_result({"a": 1}, is_error=True, meta={}),
            {"content": [{"type": "text", "text": '{"a":1}'}], "isError": True, "_meta": {}},
        )

    def test_equals_upstream_call_tool_result_without_meta(self) -> None:
        upstream = _upstream()
        for payload in ({"a": 1}, [1], "s", None, 3):
            for is_error in (False, True):
                with self.subTest(payload=payload, is_error=is_error):
                    self.assertEqual(
                        json.dumps(tool_result(payload, is_error=is_error)),
                        json.dumps(upstream._call_tool_result(payload, is_error=is_error)),
                    )

    def test_unserialisable_payload_propagates(self) -> None:
        with self.assertRaises(TypeError):
            tool_result({"a": object()})


class SmallRendererTests(ExactMixin):
    def test_render_error(self) -> None:
        self.assertExact(
            render_error("boom"),
            {"content": [{"type": "text", "text": '{"error":"boom"}'}], "isError": True},
        )

    def test_render_unknown_job(self) -> None:
        self.assertExact(
            render_unknown_job("j1"),
            {"content": [{"type": "text", "text": '{"error":"Unknown or expired job id: j1"}'}], "isError": True},
        )
        self.assertIn("Unknown or expired job id: 5", render_unknown_job(5)["content"][0]["text"])

    def test_render_request_cancelled(self) -> None:
        self.assertExact(
            render_request_cancelled(),
            {"content": [{"type": "text", "text": '{"error":"Request cancelled"}'}], "isError": True},
        )

    def test_constants(self) -> None:
        self.assertEqual(
            jobs.JOB_ROUTED_TOOLS, ("idalib_open", "idalib_close", "idalib_save", "idalib_health", "idalib_warmup")
        )
        self.assertEqual(
            TICKET_MESSAGE,
            "Still running in the background; this is not a failure. Do not repeat the call; poll with idalib_job_status.",
        )
        self.assertEqual(EXPIRED_MESSAGE, "Job result expired (results are kept 10 minutes)")
        self.assertEqual(jobs.POLL_WAIT_SECONDS, 30)
        self.assertEqual(jobs.WAIT_STEP, 0.1)


class TicketTests(ExactMixin):
    def test_ticket_payload_keys_and_order(self) -> None:
        info = _info()
        self.assertExact(
            ticket_payload(info, "opening"),
            {
                "pending": True,
                "message": "Still running in the background; this is not a failure. "
                "Do not repeat the call; poll with idalib_job_status.",
                "job": {
                    "job_id": "abc123def456",
                    "database": "s1",
                    "tool": "slow_tool",
                    "kind": "tool",
                    "status": "running",
                    "phase": None,
                },
                "database_state": "opening",
                "poll": {"tool": "idalib_job_status", "arguments": {"job_id": "abc123def456", "wait_seconds": 30}},
                "cancel": {"tool": "idalib_job_cancel", "arguments": {"job_id": "abc123def456"}},
            },
        )

    def test_ticket_payload_embeds_info_and_state_as_given(self) -> None:
        info = _info()
        payload = ticket_payload(info, "anything")
        self.assertIs(payload["job"], info)
        self.assertEqual(payload["database_state"], "anything")

    def test_ticket_payload_without_job_id_uses_null(self) -> None:
        payload = ticket_payload({}, "ready")
        self.assertIsNone(payload["poll"]["arguments"]["job_id"])
        self.assertIsNone(payload["cancel"]["arguments"]["job_id"])

    def test_render_ticket(self) -> None:
        info = {"job_id": "j1", "status": "queued"}
        ticket = {
            "pending": True,
            "message": "Still running in the background; this is not a failure. "
            "Do not repeat the call; poll with idalib_job_status.",
            "job": {"job_id": "j1", "status": "queued"},
            "database_state": "ready",
            "poll": {"tool": "idalib_job_status", "arguments": {"job_id": "j1", "wait_seconds": 30}},
            "cancel": {"tool": "idalib_job_cancel", "arguments": {"job_id": "j1"}},
        }
        self.assertExact(
            render_ticket(info, "ready"),
            {
                "content": [{"type": "text", "text": json.dumps(ticket, separators=(",", ":"))}],
                "isError": True,
                "_meta": {"idalib_job": ticket},
            },
        )
        self.assertNotIn("structuredContent", render_ticket(info, "ready"))


class JobFailedTests(ExactMixin):
    def test_error_text_is_used(self) -> None:
        self.assertExact(
            render_job_failed({"status": "error", "error": "no good"}),
            {"content": [{"type": "text", "text": '{"error":"no good"}'}], "isError": True},
        )

    def test_error_text_wins_over_cancelled_status(self) -> None:
        self.assertEqual(
            json.loads(render_job_failed({"status": "cancelled", "error": "Cancelled while queued"})["content"][0]["text"]),
            {"error": "Cancelled while queued"},
        )

    def test_fallbacks(self) -> None:
        cases = [
            ({"status": "cancelled", "error": None}, "Job was cancelled"),
            ({"status": "cancelled", "error": ""}, "Job was cancelled"),
            ({"status": "cancelled"}, "Job was cancelled"),
            ({"status": "error", "error": None}, "Job failed"),
            ({"status": "error", "error": ""}, "Job failed"),
            ({"status": "error", "error": 5}, "Job failed"),
            ({"status": "ok"}, "Job failed"),
            ({}, "Job failed"),
        ]
        for info, message in cases:
            with self.subTest(info=info):
                self.assertExact(
                    render_job_failed(info),
                    {"content": [{"type": "text", "text": json.dumps({"error": message}, separators=(",", ":"))}], "isError": True},
                )


class OpenResultTests(ExactMixin):
    SESSION = {"session_id": "s1", "input_path": "C:/x/a.exe", "filename": "a.exe"}
    STORED = {"session_id": "s1", "session": SESSION, "context_id": "c1"}

    def test_without_fields_or_info(self) -> None:
        payload = {
            "success": True,
            "session": {"session_id": "s1", "input_path": "C:/x/a.exe", "filename": "a.exe"},
            "message": "Binary opened and bound to context: a.exe (s1)",
        }
        self.assertExact(
            render_open_result(self.STORED, None),
            {
                "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
                "isError": False,
                "structuredContent": payload,
            },
        )

    def test_with_fields_and_info(self) -> None:
        info = _info(kind="open", status="ok")
        payload = {
            "success": True,
            "context_id": "c1",
            "isolated": False,
            "session": {"session_id": "s1", "input_path": "C:/x/a.exe", "filename": "a.exe"},
            "message": "Binary opened and bound to context: a.exe (s1)",
        }
        expected = {
            "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
            "isError": False,
            "structuredContent": payload,
            "_meta": {"idalib_job": dict(info)},
        }
        self.assertExact(render_open_result(self.STORED, {"context_id": "c1", "isolated": False}, info), expected)

    def test_empty_info_still_adds_meta(self) -> None:
        self.assertEqual(render_open_result(self.STORED, {}, {})["_meta"], {"idalib_job": {}})

    def test_malformed_stored_value_raises(self) -> None:
        with self.assertRaises(KeyError):
            render_open_result({"session_id": "s1"}, None)
        with self.assertRaises(KeyError):
            render_open_result({"session": {"session_id": "s1"}}, None)
        with self.assertRaises(TypeError):
            render_open_result({"session": None}, None)  # type: ignore[arg-type]


class JobStatusTests(ExactMixin):
    def _text(self, result: dict[str, Any]) -> Any:
        return json.loads(result["content"][0]["text"])

    def test_unknown_for_non_str_status(self) -> None:
        for info in ({}, {"status": None}, {"status": 3}):
            with self.subTest(info=info):
                self.assertExact(render_job_status("j9", info, "tool"), render_unknown_job("j9"))

    def test_ok_with_released_result(self) -> None:
        info = _info(status="ok", result_available=False)
        payload = {"error": "Job result expired (results are kept 10 minutes)", "job": dict(info)}
        self.assertExact(
            render_job_status("j", info, "tool"),
            {"content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}], "isError": True},
        )
        self.assertExact(
            render_job_status("j", info, "open", stored=NO_RESULT),
            {"content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}], "isError": True},
        )

    def test_ok_with_none_stored_is_not_the_sentinel(self) -> None:
        result = render_job_status("j", _info(status="ok"), "tool", stored=None)
        self.assertEqual(self._text(result), {"error": "Job returned no result"})

    def test_ok_open_renders_open_result(self) -> None:
        info = _info(kind="open", status="ok")
        stored = {"session_id": "s1", "session": {"session_id": "s1", "filename": "a.exe"}, "context_id": "c1"}
        self.assertExact(
            render_job_status("j", info, "open", stored=stored, open_fields={"context_id": "c1"}),
            render_open_result(stored, {"context_id": "c1"}, info),
        )
        payload = {
            "success": True,
            "context_id": "c1",
            "session": {"session_id": "s1", "filename": "a.exe"},
            "message": "Binary opened and bound to context: a.exe (s1)",
        }
        self.assertExact(
            render_job_status("j", info, "open", stored=stored, open_fields={"context_id": "c1"}),
            {
                "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
                "isError": False,
                "structuredContent": payload,
                "_meta": {"idalib_job": dict(info)},
            },
        )

    def test_ok_rpc_is_status_only(self) -> None:
        info = _info(kind="rpc", status="ok")
        payload = {"job": dict(info)}
        self.assertExact(
            render_job_status("j", info, "rpc", stored={"result": {"content": []}}),
            {
                "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
                "isError": False,
                "structuredContent": payload,
            },
        )

    def test_ok_stored_call_result_gets_job_meta(self) -> None:
        info = _info(status="ok")
        stored_result = {
            "content": [{"type": "text", "text": '{"slept":1}'}],
            "structuredContent": {"slept": 1},
            "isError": False,
        }
        stored = {"jsonrpc": "2.0", "result": stored_result, "id": 7}
        self.assertExact(
            render_job_status("j", info, "tool", stored=stored),
            {
                "content": [{"type": "text", "text": '{"slept":1}'}],
                "structuredContent": {"slept": 1},
                "isError": False,
                "_meta": {"idalib_job": dict(info)},
            },
        )

    def test_ok_stored_call_result_keeps_old_meta_and_is_a_deep_copy(self) -> None:
        info = _info(status="ok")
        stored_result = {"content": [], "isError": True, "_meta": {"progress": 1, "idalib_job": "old"}}
        stored = {"result": stored_result}
        rendered = render_job_status("j", info, "tool", stored=stored)
        self.assertExact(
            rendered,
            {"content": [], "isError": True, "_meta": {"progress": 1, "idalib_job": dict(info)}},
        )
        self.assertEqual(stored_result["_meta"], {"progress": 1, "idalib_job": "old"})
        rendered["content"].append("x")
        self.assertEqual(stored_result["content"], [])

    def test_ok_stored_result_with_non_dict_meta_is_replaced(self) -> None:
        info = _info(status="ok")
        rendered = render_job_status("j", info, "tool", stored={"result": {"content": [], "_meta": "junk"}})
        self.assertEqual(rendered["_meta"], {"idalib_job": info})

    def test_ok_stored_error_response(self) -> None:
        info = _info(status="ok")
        stored = {"jsonrpc": "2.0", "error": {"code": -32603, "message": "worker died"}, "id": 7}
        self.assertExact(
            render_job_status("j", info, "tool", stored=stored),
            {"content": [{"type": "text", "text": '{"error":"worker died"}'}], "isError": True},
        )

    def test_ok_stored_error_message_is_stringified(self) -> None:
        stored = {"error": {"message": 42}}
        self.assertEqual(self._text(render_job_status("j", _info(status="ok"), "tool", stored=stored)), {"error": "42"})

    def test_ok_stored_without_usable_result_or_error(self) -> None:
        for stored in ({}, {"result": "x"}, {"result": None}, {"error": "x"}, {"error": {}}, "text", 5, [1]):
            with self.subTest(stored=stored):
                self.assertExact(
                    render_job_status("j", _info(status="ok"), "tool", stored=stored),
                    {"content": [{"type": "text", "text": '{"error":"Job returned no result"}'}], "isError": True},
                )

    def test_error_status(self) -> None:
        info = _info(status="error", error="boom")
        payload = {"error": "boom", "job": dict(info)}
        self.assertExact(
            render_job_status("j", info, "tool"),
            {"content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}], "isError": True},
        )
        self.assertIsNone(self._text(render_job_status("j", _info(status="error"), "tool"))["error"])

    def test_cancelled_status(self) -> None:
        info = _info(status="cancelled", error="Cancelled while queued")
        payload = {"error": "Job was cancelled", "job": dict(info)}
        self.assertExact(
            render_job_status("j", info, "tool"),
            {"content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}], "isError": True},
        )

    def test_pending_statuses_render_a_non_error_ticket(self) -> None:
        for status in ("queued", "running", "cancelling", "something-new"):
            with self.subTest(status=status):
                info = _info(status=status)
                ticket = ticket_payload(info, "opening")
                self.assertExact(
                    render_job_status("j", info, "tool", database_state="opening"),
                    {
                        "content": [{"type": "text", "text": json.dumps(ticket, separators=(",", ":"))}],
                        "isError": False,
                        "structuredContent": ticket,
                    },
                )

    def test_pending_default_database_state_is_ready(self) -> None:
        rendered = render_job_status("j", _info(status="queued"), "tool")
        self.assertEqual(rendered["structuredContent"]["database_state"], "ready")

    def test_pending_ignores_stored_value(self) -> None:
        rendered = render_job_status("j", _info(status="running"), "tool", stored={"result": {"content": []}})
        self.assertTrue(rendered["structuredContent"]["pending"])


class JobsAndCancelTests(ExactMixin):
    def test_render_jobs(self) -> None:
        databases = [
            {"database": "s1", "filename": "a.exe", "state": "ready", "running": None, "queued": [], "recent": []},
            {"database": "s2", "filename": "b.exe", "state": "closed", "running": None, "queued": [], "recent": []},
        ]
        payload = {"databases": databases, "count": 2}
        self.assertExact(
            render_jobs(databases),
            {
                "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
                "isError": False,
                "structuredContent": payload,
            },
        )

    def test_render_jobs_empty(self) -> None:
        self.assertExact(
            render_jobs([]),
            {
                "content": [{"type": "text", "text": '{"databases":[],"count":0}'}],
                "isError": False,
                "structuredContent": {"databases": [], "count": 0},
            },
        )

    def _cancel_expected(self, outcome: str, message: str, info: dict[str, Any]) -> dict[str, Any]:
        payload = {"job_id": "j1", "outcome": outcome, "message": message, "job": info}
        return {
            "content": [{"type": "text", "text": json.dumps(payload, separators=(",", ":"))}],
            "isError": False,
            "structuredContent": payload,
        }

    def test_render_cancel_messages(self) -> None:
        info = {"job_id": "j1", "status": "cancelled"}
        cases = [
            ("removed", "tool", "Removed from the queue; the job will not run."),
            ("removed", "open", "Removed from the queue; the job will not run."),
            ("cancelling", "open", "Cancelling: the opening database's worker is being terminated."),
            (
                "cancelling",
                "tool",
                "Cancelling: the worker finishes the current call, then the result is discarded.",
            ),
            (
                "cancelling",
                "rpc",
                "Cancelling: the worker finishes the current call, then the result is discarded.",
            ),
            (
                "already_cancelling",
                "tool",
                "Cancellation was already requested; the job ends when the worker returns.",
            ),
            (
                "already_cancelling",
                "open",
                "Cancellation was already requested; the job ends when the worker returns.",
            ),
            (
                "finished",
                "open",
                "The job had already finished; nothing was cancelled. "
                "An opened database stays open; use idalib_close to close it.",
            ),
            ("finished", "tool", "The job had already finished; nothing was cancelled."),
            ("finished", "close", "The job had already finished; nothing was cancelled."),
            ("mystery", "tool", "Cancel outcome: mystery"),
            ("mystery", "open", "Cancel outcome: mystery"),
        ]
        for outcome, kind, message in cases:
            with self.subTest(outcome=outcome, kind=kind):
                self.assertExact(render_cancel("j1", outcome, kind, info), self._cancel_expected(outcome, message, info))

    def test_render_cancel_unknown_is_the_unknown_job_error(self) -> None:
        self.assertExact(render_cancel("j1", "unknown", "tool", {}), render_unknown_job("j1"))
        self.assertExact(render_cancel("j1", "unknown", "open", {"job_id": "x"}), render_unknown_job("j1"))

    def test_render_cancel_embeds_info_as_given(self) -> None:
        info = {"job_id": "j1"}
        self.assertEqual(render_cancel("j1", "removed", "tool", info)["structuredContent"]["job"], info)


class ArgumentParserTests(unittest.TestCase):
    def assertInvalid(self, parser: Any, arguments: Any, message: str) -> None:
        with self.assertRaises(JobArgumentError) as caught:
            parser(arguments)
        self.assertEqual(str(caught.exception), message)

    def test_error_type_is_a_value_error(self) -> None:
        self.assertTrue(issubclass(JobArgumentError, ValueError))

    def test_job_status_defaults(self) -> None:
        result = parse_job_status_arguments({"job_id": "j"})
        self.assertEqual(result, ("j", 30.0))
        self.assertIsInstance(result[1], float)

    def test_job_status_wait_values(self) -> None:
        cases = [
            (0, 0.0),
            (5, 5.0),
            (12.5, 12.5),
            (30, 30.0),
            (31, 30.0),
            (1000.5, 30.0),
            (10**400, 30.0),
            (-1, 0.0),
            (-0.0, 0.0),
            (-math.inf, 0.0),
            (math.inf, 30.0),
            (math.nan, 0.0),
        ]
        for given, expected in cases:
            with self.subTest(wait_seconds=given):
                job_id, wait = parse_job_status_arguments({"job_id": "j", "wait_seconds": given})
                self.assertEqual(job_id, "j")
                self.assertEqual(wait, expected)
                self.assertIsInstance(wait, float)
                self.assertFalse(math.isnan(wait))

    def test_job_status_wait_type_errors(self) -> None:
        for given in (True, False, "5", None, [1], {"a": 1}):
            with self.subTest(wait_seconds=given):
                self.assertInvalid(
                    parse_job_status_arguments,
                    {"job_id": "j", "wait_seconds": given},
                    "Invalid params: wait_seconds must be a number",
                )

    def test_job_id_must_be_a_string(self) -> None:
        for arguments in ({}, {"job_id": None}, {"job_id": 5}, {"job_id": True}, {"job_id": ["j"]}):
            with self.subTest(arguments=arguments):
                self.assertInvalid(parse_job_status_arguments, arguments, "Invalid params: job_id must be a string")
                self.assertInvalid(parse_job_cancel_arguments, arguments, "Invalid params: job_id must be a string")

    def test_empty_job_id_is_accepted(self) -> None:
        self.assertEqual(parse_job_status_arguments({"job_id": ""}), ("", 30.0))
        self.assertEqual(parse_job_cancel_arguments({"job_id": ""}), "")

    def test_unexpected_parameters_are_sorted_and_checked_first(self) -> None:
        self.assertInvalid(
            parse_job_status_arguments,
            {"job_id": 5, "wait_seconds": "x", "b": 1, "a": 2},
            "Invalid params: unexpected parameters: ['a', 'b']",
        )
        self.assertInvalid(
            parse_job_status_arguments,
            {"job_id": "j", "a": 2, "b": 1},
            "Invalid params: unexpected parameters: ['a', 'b']",
        )
        self.assertInvalid(
            parse_jobs_arguments,
            {"database": 5, "recent": "x", "zz": 1},
            "Invalid params: unexpected parameters: ['zz']",
        )
        self.assertInvalid(
            parse_job_cancel_arguments, {"job_id": 5, "other": 1}, "Invalid params: unexpected parameters: ['other']"
        )

    def test_parameters_are_checked_in_signature_order(self) -> None:
        self.assertInvalid(
            parse_job_status_arguments,
            {"job_id": 5, "wait_seconds": "x"},
            "Invalid params: job_id must be a string",
        )
        self.assertInvalid(
            parse_jobs_arguments,
            {"database": 5, "recent": "x"},
            "Invalid params: database must be a string or null",
        )

    def test_non_dict_arguments(self) -> None:
        for parser in (parse_job_status_arguments, parse_jobs_arguments, parse_job_cancel_arguments):
            for arguments in (None, [], "x", 5):
                with self.subTest(parser=parser.__name__, arguments=arguments):
                    self.assertInvalid(parser, arguments, "Invalid params: arguments must be an object")

    def test_jobs_defaults_and_database_normalisation(self) -> None:
        self.assertEqual(parse_jobs_arguments({}), (None, 20))
        self.assertEqual(parse_jobs_arguments({"database": None}), (None, 20))
        self.assertEqual(parse_jobs_arguments({"database": ""}), (None, 20))
        self.assertEqual(parse_jobs_arguments({"database": "a.exe", "recent": 3}), ("a.exe", 3))

    def test_jobs_database_type_error(self) -> None:
        for given in (5, True, ["a"], {"a": 1}, 1.5):
            with self.subTest(database=given):
                self.assertInvalid(
                    parse_jobs_arguments, {"database": given}, "Invalid params: database must be a string or null"
                )

    def test_jobs_recent_values(self) -> None:
        for given, expected in ((0, 0), (20, 20), (100, 100), (101, 100), (10**30, 100), (-1, 0), (-(10**30), 0)):
            with self.subTest(recent=given):
                result = parse_jobs_arguments({"recent": given})[1]
                self.assertEqual(result, expected)
                self.assertIs(type(result), int)

    def test_jobs_recent_type_errors(self) -> None:
        for given in (True, False, "3", None, 3.0, math.nan, [3]):
            with self.subTest(recent=given):
                self.assertInvalid(parse_jobs_arguments, {"recent": given}, "Invalid params: recent must be an integer")

    def test_cancel_returns_the_job_id(self) -> None:
        self.assertEqual(parse_job_cancel_arguments({"job_id": "abc"}), "abc")

    def test_parsers_do_not_mutate_arguments(self) -> None:
        arguments = {"job_id": "j", "wait_seconds": 99}
        parse_job_status_arguments(arguments)
        self.assertEqual(arguments, {"job_id": "j", "wait_seconds": 99})


class ValidatorTestCase(unittest.TestCase):
    validator: ToolsCallValidator
    upstream: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.upstream = _upstream()
        cls.validator = ToolsCallValidator(cls.upstream)

    def request(self, params: Any = ..., *, id: Any = 1, **extra: Any) -> dict[str, Any]:
        request: dict[str, Any] = {"jsonrpc": "2.0", "method": "tools/call"}
        if params is not ...:
            request["params"] = params
        if id is not ...:
            request["id"] = id
        request.update(extra)
        return request

    def assertRejectedLikeUpstream(self, request: dict[str, Any]) -> None:
        expected = self.upstream._original_dispatch(request)
        actual = self.validator(request)
        self.assertIsInstance(actual, Rejected)
        assert isinstance(actual, Rejected)
        self.assertEqual(actual.response, expected)
        self.assertEqual(json.dumps(actual.response), json.dumps(expected))


class ValidatorOuterRejectionTests(ValidatorTestCase):
    def test_rejections_match_original_dispatch(self) -> None:
        cases: dict[str, dict[str, Any]] = {
            "arguments list": self.request({"name": "idalib_list", "arguments": []}),
            "arguments digit string": self.request({"name": "idalib_list", "arguments": "123"}),
            "arguments non-json string": self.request({"name": "idalib_list", "arguments": "abc"}),
            "arguments json list string": self.request({"name": "idalib_list", "arguments": "[1]"}),
            "arguments null string": self.request({"name": "idalib_list", "arguments": "null"}),
            "arguments int": self.request({"name": "idalib_list", "arguments": 5}),
            "extra params key": self.request({"name": "idalib_list", "arguments": {}, "extra": 1}),
            "name missing": self.request({}),
            "name not a string": self.request({"name": 5}),
            "name null": self.request({"name": None}),
            "meta not a dict": self.request({"name": "idalib_list", "_meta": "s"}),
            "params empty list": self.request([]),
            "params four items": self.request([1, 2, 3, 4]),
            "params string": self.request("idalib_list"),
            "params missing": self.request(),
            "params null": self.request(None),
            "no jsonrpc": {"method": "tools/call", "params": {"name": "idalib_list"}, "id": 1},
            "wrong jsonrpc": {"jsonrpc": "1.0", "method": "tools/call", "params": {"name": "idalib_list"}, "id": 1},
            "string id": self.request({"name": "idalib_list", "arguments": []}, id="abc"),
            "null id": self.request({"name": "idalib_list", "arguments": []}, id=None),
        }
        self.assertGreaterEqual(len(cases), 15)
        for label, request in cases.items():
            with self.subTest(case=label):
                self.assertRejectedLikeUpstream(request)

    def test_every_outer_rejection_is_an_error_response(self) -> None:
        response = self.validator(self.request({"name": "idalib_list", "arguments": []}, id=9))
        assert isinstance(response, Rejected)
        assert response.response is not None
        self.assertEqual(response.response["id"], 9)
        self.assertEqual(response.response["error"]["code"], -32602)
        self.assertEqual(
            response.response["error"]["message"], "Invalid params: expected dict | NoneType for arguments, got list"
        )

    def test_notification_rejections_are_none_like_upstream(self) -> None:
        for params in (
            {"name": "idalib_list", "arguments": []},
            {"name": 5},
            {},
            [],
            "x",
        ):
            with self.subTest(params=params):
                request = self.request(params, id=...)
                self.assertNotIn("id", request)
                self.assertIsNone(self.upstream._original_dispatch(request))
                self.assertEqual(self.validator(request), Rejected(None))

    def test_non_dict_request_raises_type_error(self) -> None:
        for request in ("{}", None, [], b"{}", 5):
            with self.subTest(request=request):
                with self.assertRaises(TypeError):
                    self.validator(request)  # type: ignore[arg-type]


class ValidatorRoutedRejectionTests(ValidatorTestCase):
    def test_routed_tool_rejections_match_original_dispatch(self) -> None:
        cases: dict[str, Any] = {
            "open without arguments": {"name": "idalib_open"},
            "open null arguments": {"name": "idalib_open", "arguments": None},
            "open empty arguments": {"name": "idalib_open", "arguments": {}},
            "open wrong input_path type": {"name": "idalib_open", "arguments": {"input_path": 5}},
            "open wrong bool type": {"name": "idalib_open", "arguments": {"input_path": "a", "run_auto_analysis": "x"}},
            "open wrong session_id type": {"name": "idalib_open", "arguments": {"input_path": "a", "session_id": 3}},
            "open unexpected key": {"name": "idalib_open", "arguments": {"input_path": "a", "bogus": 1}},
            "open string-encoded bad": {"name": "idalib_open", "arguments": '{"input_path": 5}'},
            "close missing session_id": {"name": "idalib_close", "arguments": {}},
            "close null session_id": {"name": "idalib_close", "arguments": {"session_id": None}},
            "close wrong type": {"name": "idalib_close", "arguments": {"session_id": 1}},
            "save wrong path type": {"name": "idalib_save", "arguments": {"path": 5}},
            "save unexpected key": {"name": "idalib_save", "arguments": {"nope": 1}},
            "health wrong session_id": {"name": "idalib_health", "arguments": {"session_id": 5}},
            "warmup wrong bool": {"name": "idalib_warmup", "arguments": {"build_caches": "yes"}},
            "warmup null bool": {"name": "idalib_warmup", "arguments": {"init_hexrays": None}},
        }
        for label, params in cases.items():
            with self.subTest(case=label):
                self.assertRejectedLikeUpstream(self.request(params, id=11))

    def test_routed_rejection_response_shape(self) -> None:
        result = self.validator(self.request({"name": "idalib_close", "arguments": {}}, id=4))
        self.assertEqual(
            result,
            Rejected(
                {
                    "jsonrpc": "2.0",
                    "result": {
                        "content": [
                            {"type": "text", "text": "Invalid params: missing required parameters: ['session_id']"}
                        ],
                        "isError": True,
                    },
                    "id": 4,
                }
            ),
        )

    def test_routed_rejection_keeps_the_request_id_verbatim(self) -> None:
        for request_id in ("abc", 0, None, 1.5):
            with self.subTest(id=request_id):
                result = self.validator(self.request({"name": "idalib_close", "arguments": {}}, id=request_id))
                assert isinstance(result, Rejected) and result.response is not None
                self.assertEqual(result.response["id"], request_id)
                self.assertTrue(result.response["result"]["isError"])

    def test_routed_rejection_for_a_notification_is_none(self) -> None:
        request = self.request({"name": "idalib_close", "arguments": {}}, id=...)
        self.assertNotIn("id", request)
        self.assertIsNone(self.upstream._original_dispatch(request))
        self.assertEqual(self.validator(request), Rejected(None))


class ValidatorAcceptedTests(ValidatorTestCase):
    def test_default_filling_for_the_five_tools(self) -> None:
        cases = [
            ("idalib_open", {"input_path": "a.exe"}, {"input_path": "a.exe", "run_auto_analysis": True, "session_id": None}),
            ("idalib_close", {"session_id": "s1"}, {"session_id": "s1"}),
            ("idalib_save", {}, {"path": "", "session_id": None}),
            ("idalib_health", {}, {"session_id": None}),
            (
                "idalib_warmup",
                {},
                {"session_id": None, "wait_auto_analysis": True, "build_caches": True, "init_hexrays": True},
            ),
        ]
        for name, given, expected in cases:
            with self.subTest(tool=name):
                result = self.validator(self.request({"name": name, "arguments": given}))
                self.assertIsInstance(result, ToolsCall)
                assert isinstance(result, ToolsCall)
                self.assertEqual(result.name, name)
                self.assertEqual(result.arguments, expected)
                self.assertEqual(list(result.arguments), list(expected))
                self.assertIsNone(result.meta)

    def test_given_values_override_defaults_and_keep_signature_order(self) -> None:
        result = self.validator(
            self.request(
                {
                    "name": "idalib_warmup",
                    "arguments": {"init_hexrays": False, "session_id": "s2", "build_caches": False},
                }
            )
        )
        assert isinstance(result, ToolsCall)
        self.assertEqual(
            list(result.arguments.items()),
            [("session_id", "s2"), ("wait_auto_analysis", True), ("build_caches", False), ("init_hexrays", False)],
        )

    def test_explicit_null_is_kept_for_optional_parameters(self) -> None:
        result = self.validator(self.request({"name": "idalib_open", "arguments": {"input_path": "a", "session_id": None}}))
        assert isinstance(result, ToolsCall)
        self.assertEqual(result.arguments, {"input_path": "a", "run_auto_analysis": True, "session_id": None})
        result = self.validator(self.request({"name": "idalib_open", "arguments": {"input_path": "a", "run_auto_analysis": False}}))
        assert isinstance(result, ToolsCall)
        self.assertIs(result.arguments["run_auto_analysis"], False)

    def test_missing_and_null_arguments_for_tools_without_required_parameters(self) -> None:
        for params in ({"name": "idalib_save"}, {"name": "idalib_save", "arguments": None}):
            with self.subTest(params=params):
                result = self.validator(self.request(params))
                self.assertEqual(result, ToolsCall("idalib_save", {"path": "", "session_id": None}, None))

    def test_string_encoded_arguments_are_decoded(self) -> None:
        result = self.validator(self.request({"name": "idalib_open", "arguments": '{"input_path": "a.exe"}'}))
        self.assertEqual(
            result,
            ToolsCall("idalib_open", {"input_path": "a.exe", "run_auto_analysis": True, "session_id": None}, None),
        )
        result = self.validator(self.request({"name": "slow_tool", "arguments": '{"seconds": 2}'}))
        self.assertEqual(result, ToolsCall("slow_tool", {"seconds": 2}, None))

    def test_meta_is_returned(self) -> None:
        result = self.validator(
            self.request({"name": "idalib_health", "arguments": {}, "_meta": {"progressToken": "t1"}})
        )
        self.assertEqual(result, ToolsCall("idalib_health", {"session_id": None}, {"progressToken": "t1"}))
        result = self.validator(self.request({"name": "echo", "_meta": {"progressToken": 7}}))
        self.assertEqual(result, ToolsCall("echo", {}, {"progressToken": 7}))

    def test_list_params_are_mapped_by_position(self) -> None:
        self.assertEqual(self.validator(self.request(["idalib_list"])), ToolsCall("idalib_list", {}, None))
        self.assertEqual(
            self.validator(self.request(["idalib_close", {"session_id": "s"}, {"m": 1}])),
            ToolsCall("idalib_close", {"session_id": "s"}, {"m": 1}),
        )

    def test_other_names_pass_through_as_a_new_dict(self) -> None:
        arguments = {"x": 1, "nested": {"y": [1, 2]}}
        for name in ("idalib_list", "idalib_switch", "idalib_job_status", "list_instances", "slow_tool", "no_such_tool"):
            with self.subTest(tool=name):
                result = self.validator(self.request({"name": name, "arguments": arguments}))
                assert isinstance(result, ToolsCall)
                self.assertEqual(result.name, name)
                self.assertEqual(result.arguments, arguments)
                self.assertIsNot(result.arguments, arguments)
                self.assertIsNone(result.meta)

    def test_other_names_accept_missing_null_and_empty_arguments(self) -> None:
        for params in ({"name": "echo"}, {"name": "echo", "arguments": None}, {"name": "echo", "arguments": {}}):
            with self.subTest(params=params):
                self.assertEqual(self.validator(self.request(params)), ToolsCall("echo", {}, None))

    def test_other_names_are_not_validated_against_any_signature(self) -> None:
        result = self.validator(self.request({"name": "idalib_job_status", "arguments": {"job_id": 5, "zzz": []}}))
        self.assertEqual(result, ToolsCall("idalib_job_status", {"job_id": 5, "zzz": []}, None))

    def test_notification_with_valid_arguments_is_accepted(self) -> None:
        cases = [
            ({"name": "idalib_save", "arguments": {"path": "x.i64"}}, ToolsCall("idalib_save", {"path": "x.i64", "session_id": None}, None)),
            ({"name": "slow_tool", "arguments": {"seconds": 1}}, ToolsCall("slow_tool", {"seconds": 1}, None)),
            ({"name": "idalib_list"}, ToolsCall("idalib_list", {}, None)),
        ]
        for params, expected in cases:
            with self.subTest(params=params):
                request = self.request(params, id=...)
                self.assertNotIn("id", request)
                self.assertEqual(self.validator(request), expected)

    def test_valid_request_ids_of_any_json_type_are_accepted(self) -> None:
        for request_id in (0, "x", None, 1.5):
            with self.subTest(id=request_id):
                result = self.validator(self.request({"name": "idalib_health"}, id=request_id))
                self.assertEqual(result, ToolsCall("idalib_health", {"session_id": None}, None))

    def test_validation_runs_no_tool_body_and_does_not_touch_upstream_registries(self) -> None:
        methods_before = dict(self.upstream.mcp.registry.methods)
        tools_before = dict(self.upstream.mcp.tools.methods)
        for name in jobs.JOB_ROUTED_TOOLS:
            self.validator(self.request({"name": name, "arguments": {"input_path": "a", "session_id": "s"}}))
        self.assertEqual(dict(self.upstream.mcp.registry.methods), methods_before)
        self.assertEqual(dict(self.upstream.mcp.tools.methods), tools_before)
        self.assertIsNone(self.upstream.supervisor)

    def test_concurrent_calls_do_not_see_each_others_captures(self) -> None:
        failures: list[str] = []
        barrier = threading.Barrier(8)

        def worker(index: int) -> None:
            name = jobs.JOB_ROUTED_TOOLS[index % len(jobs.JOB_ROUTED_TOOLS)]
            arguments = {"input_path": f"p{index}"} if name == "idalib_open" else {"session_id": f"s{index}"}
            expected_session = arguments.get("session_id")
            barrier.wait(5)
            for _ in range(150):
                result = self.validator(self.request({"name": name, "arguments": dict(arguments)}))
                if not isinstance(result, ToolsCall) or result.name != name:
                    failures.append(f"{index}: {result!r}")
                    return
                if name == "idalib_open":
                    ok = result.arguments["input_path"] == f"p{index}"
                else:
                    ok = result.arguments["session_id"] == expected_session
                if not ok:
                    failures.append(f"{index}: {result!r}")
                    return

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(8)]
        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        self.addCleanup(sys.setswitchinterval, interval)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])


class ValidatorConstructionTests(unittest.TestCase):
    def test_missing_upstream_attribute_raises_attribute_error(self) -> None:
        class Bare:
            class mcp:  # noqa: N801
                registry = _upstream().mcp.registry
                _mcp_tools_call = _upstream().mcp._mcp_tools_call

        with self.assertRaises(AttributeError):
            ToolsCallValidator(Bare())


if __name__ == "__main__":
    unittest.main()
