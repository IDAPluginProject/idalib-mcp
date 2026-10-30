from __future__ import annotations

import gc
import threading
import time
import unittest
import weakref

from idalib_mcp.dispatcher import WorkerDispatcher


class WorkerDispatcherBasicsTests(unittest.TestCase):
    def test_run_marks_record_ok_and_appends_to_history(self) -> None:
        dispatcher = WorkerDispatcher("session-a")

        result = dispatcher.run(
            tool="ping",
            json_rpc_id=1,
            args={"x": 1},
            fn=lambda cancel_event: {"ok": True},
            cancel_event=threading.Event(),
        )

        self.assertEqual(result, {"ok": True})
        snapshot = dispatcher.snapshot()
        self.assertIsNone(snapshot["current"])
        self.assertEqual(snapshot["queued"], [])
        self.assertEqual(len(snapshot["history"]), 1)
        record = snapshot["history"][0]
        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["tool"], "ping")
        self.assertEqual(record["session_id"], "session-a")
        self.assertEqual(record["args_preview"], '{"x":1}')
        self.assertIsNone(record["args_full_id"])
        self.assertEqual(record["result_preview"], '{"ok":true}')
        self.assertGreaterEqual(record["duration_ms"], 0)
        self.assertIsNone(record["error"])

    def test_run_marks_record_error_when_closure_raises(self) -> None:
        dispatcher = WorkerDispatcher("session-a")

        def boom(_cancel_event):
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            dispatcher.run("explode", 1, {}, boom, threading.Event())

        history = dispatcher.snapshot()["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["status"], "error")
        self.assertEqual(history[0]["error"], "boom")

    def test_history_capped_at_100(self) -> None:
        dispatcher = WorkerDispatcher("s")
        for i in range(150):
            dispatcher.run(
                tool=f"t{i}",
                json_rpc_id=i,
                args={},
                fn=lambda _cancel: {"i": _cancel is None or i},
                cancel_event=threading.Event(),
            )
        snapshot = dispatcher.snapshot()
        self.assertEqual(len(snapshot["history"]), 100)
        self.assertEqual(snapshot["history"][0]["tool"], "t50")
        self.assertEqual(snapshot["history"][-1]["tool"], "t149")

    def test_long_args_and_result_produce_full_payload_ids(self) -> None:
        dispatcher = WorkerDispatcher("s")
        big = "a" * 5000
        dispatcher.run(
            tool="t",
            json_rpc_id=1,
            args={"blob": big},
            fn=lambda _c: {"blob": big},
            cancel_event=threading.Event(),
        )
        record = dispatcher.snapshot()["history"][0]
        self.assertIsNotNone(record["args_full_id"])
        self.assertIsNotNone(record["result_full_id"])
        full_args = dispatcher.get_payload(record["args_full_id"])
        self.assertIn(big, full_args)

    def test_payload_cap_evicts_oldest(self) -> None:
        dispatcher = WorkerDispatcher("s")
        dispatcher.PAYLOAD_CAP = 5
        big = "a" * 5000
        for i in range(dispatcher.PAYLOAD_CAP + 10):
            dispatcher.run(
                tool="t",
                json_rpc_id=i,
                args={"blob": big, "i": i},
                fn=lambda _c: {},
                cancel_event=threading.Event(),
            )
        self.assertLessEqual(len(dispatcher._payloads), dispatcher.PAYLOAD_CAP)
        first_record = dispatcher.snapshot()["history"][0]
        self.assertIsNone(dispatcher.get_payload(first_record["args_full_id"]))


class WorkerDispatcherCancellationTests(unittest.TestCase):
    def test_cancel_before_run_raises_cancelled(self) -> None:
        from idalib_mcp.dispatcher import CancelledError

        dispatcher = WorkerDispatcher("s")
        event = threading.Event()
        event.set()

        with self.assertRaises(CancelledError):
            dispatcher.run(
                tool="t", json_rpc_id=1, args={},
                fn=lambda _c: {"unreachable": True},
                cancel_event=event,
            )

        history = dispatcher.snapshot()["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["status"], "cancelled")

    def test_cancel_while_queued_unblocks_second_caller(self) -> None:
        from idalib_mcp.dispatcher import CancelledError

        dispatcher = WorkerDispatcher("s")
        first_release = threading.Event()
        first_started = threading.Event()

        def first(_cancel):
            first_started.set()
            first_release.wait(timeout=5)
            return {"ok": True}

        first_event = threading.Event()
        second_event = threading.Event()

        t1 = threading.Thread(
            target=dispatcher.run,
            args=("first", 1, {}, first, first_event),
        )
        t1.start()
        self.assertTrue(first_started.wait(timeout=2))

        outcome: list[BaseException] = []

        def second_call():
            try:
                dispatcher.run("second", 2, {}, lambda _c: {}, second_event)
            except BaseException as exc:
                outcome.append(exc)

        t2 = threading.Thread(target=second_call)
        t2.start()

        # Wait until the second call is registered as queued.
        for _ in range(50):
            if any(r["tool"] == "second" for r in dispatcher.snapshot()["queued"]):
                break
            time.sleep(0.02)
        else:
            self.fail("second call never entered the queue")

        second_event.set()
        t2.join(timeout=2)
        first_release.set()
        t1.join(timeout=2)

        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], CancelledError)
        history = {r["tool"]: r for r in dispatcher.snapshot()["history"]}
        self.assertEqual(history["first"]["status"], "ok")
        self.assertEqual(history["second"]["status"], "cancelled")

    def test_different_dispatchers_run_in_parallel(self) -> None:
        a = WorkerDispatcher("a")
        b = WorkerDispatcher("b")
        barrier = threading.Barrier(2, timeout=2)

        def body(_cancel):
            barrier.wait()
            return {}

        threads = [
            threading.Thread(target=a.run, args=("t", 1, {}, body, threading.Event())),
            threading.Thread(target=b.run, args=("t", 1, {}, body, threading.Event())),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3)
        for t in threads:
            self.assertFalse(t.is_alive(), "thread did not finish — likely serialized")


def _completes_in_thread(func, timeout: float = 2.0) -> bool:
    """Run func on another thread; False if it is still blocked (e.g. on a held lock) after timeout."""
    done = threading.Event()

    def target() -> None:
        func()
        done.set()

    threading.Thread(target=target, daemon=True).start()
    return done.wait(timeout)


class _QueueTestCase(unittest.TestCase):
    def _block(self, dispatcher: WorkerDispatcher, tool: str = "blocker", kind: str = "tool"):
        """Submit a job that runs until the returned release event is set."""
        started = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def body(_job):
            started.set()
            release.wait(10)
            return "blocked"

        job = dispatcher.submit(tool, 0, {}, body, kind=kind)
        self.assertTrue(started.wait(5))
        return job, release

    def _wait_queued(self, dispatcher: WorkerDispatcher, count: int) -> None:
        deadline = time.monotonic() + 5
        while dispatcher.queued_count() != count:
            if time.monotonic() > deadline:
                self.fail(f"queue never reached {count} jobs")
            time.sleep(0.01)


class WorkerDispatcherQueueTests(_QueueTestCase):
    def test_submit_rejects_non_callable_without_recording(self) -> None:
        dispatcher = WorkerDispatcher("s")
        with self.assertRaises(TypeError):
            dispatcher.submit("t", 1, {}, "not callable")
        snapshot = dispatcher.snapshot()
        self.assertEqual(snapshot["queued"], [])
        self.assertEqual(snapshot["history"], [])

    def test_fifo_order_across_threads_without_overlap(self) -> None:
        dispatcher = WorkerDispatcher("s")
        blocker, release = self._block(dispatcher)
        lock = threading.Lock()
        order: list[int] = []
        active = [0, 0]  # current, maximum

        def make(i: int):
            def body(_job):
                with lock:
                    active[0] += 1
                    active[1] = max(active[1], active[0])
                time.sleep(0.01)
                with lock:
                    order.append(i)
                    active[0] -= 1
                return i

            return body

        turns = [threading.Event() for _ in range(6)]
        jobs: dict[int, object] = {}

        def submitter(i: int) -> None:
            turns[i].wait(5)
            jobs[i] = dispatcher.submit(f"t{i}", i, {}, make(i))
            turns[i + 1].set()

        threads = [threading.Thread(target=submitter, args=(i,)) for i in reversed(range(5))]
        for t in threads:
            t.start()
        turns[0].set()
        self.assertTrue(turns[5].wait(5))
        self.assertEqual([r["tool"] for r in dispatcher.snapshot()["queued"]], [f"t{i}" for i in range(5)])
        release.set()
        for i in range(5):
            self.assertTrue(jobs[i].wait(5))
        self.assertEqual(order, [0, 1, 2, 3, 4])
        self.assertEqual(active[1], 1)
        self.assertEqual(blocker.status, "ok")

    def test_cancel_queued_job_is_removed_and_never_called(self) -> None:
        dispatcher = WorkerDispatcher("s")
        _blocker, release = self._block(dispatcher)
        called = threading.Event()
        job = dispatcher.submit("victim", 1, {}, lambda _job: called.set())

        self.assertEqual(dispatcher.cancel(job.id), "removed")
        self.assertTrue(job.wait(0))
        self.assertEqual(job.status, "cancelled")
        self.assertEqual(job.info()["error"], "Cancelled while queued")
        self.assertEqual(dispatcher.queued_count(), 0)
        self.assertEqual(dispatcher.cancel(job.id), "finished")

        after = dispatcher.submit("after", 2, {}, lambda _job: "after")
        release.set()
        self.assertTrue(after.wait(5))
        self.assertFalse(called.is_set())
        self.assertEqual(after.result(), "after")

    def test_cancel_running_job_discards_result_and_holds_next_job(self) -> None:
        dispatcher = WorkerDispatcher("s")
        started = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        lock_free: list[bool] = []

        def body(_job):
            lock_free.append(_completes_in_thread(dispatcher.snapshot))
            started.set()
            release.wait(10)
            return {"secret": 1}

        job = dispatcher.submit("slow", 1, {}, body)
        self.assertTrue(started.wait(5))
        next_started = threading.Event()
        next_job = dispatcher.submit("next", 2, {}, lambda _job: next_started.set())

        self.assertEqual(dispatcher.cancel(job.id), "cancelling")
        self.assertTrue(job.cancel_requested)
        self.assertEqual(dispatcher.snapshot()["current"]["status"], "cancelling")
        self.assertIs(dispatcher.current_job(), job)
        self.assertEqual(dispatcher.cancel(job.id), "already_cancelling")
        self.assertFalse(next_started.wait(0.2))
        self.assertEqual(next_job.status, "queued")

        release.set()
        self.assertTrue(job.wait(5))
        self.assertTrue(next_job.wait(5))
        self.assertEqual(lock_free, [True])
        self.assertEqual(job.status, "cancelled")
        self.assertEqual(job.info()["error"], "Cancelled while running; result discarded")
        self.assertFalse(job.result_available())
        with self.assertRaises(LookupError):
            job.result()
        record = dispatcher.snapshot()["history"][0]
        self.assertEqual(record["id"], job.id)
        self.assertIsNone(record["result_preview"])
        self.assertEqual(next_job.status, "ok")
        self.assertEqual(dispatcher.cancel(job.id), "finished")


class WorkerDispatcherCancelHookTests(_QueueTestCase):
    def test_hook_set_before_cancel_runs_once_at_cancel_without_lock(self) -> None:
        dispatcher = WorkerDispatcher("s")
        job, release = self._block(dispatcher)
        threads: list[threading.Thread] = []
        lock_free: list[bool] = []

        def hook() -> None:
            threads.append(threading.current_thread())
            lock_free.append(_completes_in_thread(dispatcher.snapshot))

        job.set_cancel_hook(hook)
        self.assertEqual(threads, [])
        self.assertEqual(dispatcher.cancel(job.id), "cancelling")
        self.assertEqual(threads, [threading.current_thread()])
        self.assertEqual(lock_free, [True])
        self.assertEqual(dispatcher.cancel(job.id), "already_cancelling")
        release.set()
        self.assertTrue(job.wait(5))
        self.assertEqual(len(threads), 1)
        self.assertEqual(job.status, "cancelled")

    def test_hook_set_after_cancel_runs_immediately_once(self) -> None:
        dispatcher = WorkerDispatcher("s")
        job, release = self._block(dispatcher)
        calls: list[str] = []

        self.assertEqual(dispatcher.cancel(job.id), "cancelling")
        job.set_cancel_hook(lambda: calls.append("late"))
        self.assertEqual(calls, ["late"])
        job.set_cancel_hook(lambda: calls.append("second"))
        self.assertEqual(calls, ["late"])
        release.set()
        self.assertTrue(job.wait(5))
        job.set_cancel_hook(lambda: calls.append("finished"))
        self.assertEqual(calls, ["late"])

    def test_set_cancel_hook_none_invokes_nothing(self) -> None:
        dispatcher = WorkerDispatcher("s")
        job, release = self._block(dispatcher)
        calls: list[str] = []

        job.set_cancel_hook(lambda: calls.append("cleared"))
        job.set_cancel_hook(None)
        self.assertEqual(dispatcher.cancel(job.id), "cancelling")
        job.set_cancel_hook(None)
        self.assertEqual(calls, [])
        job.set_cancel_hook(lambda: calls.append("late"))
        self.assertEqual(calls, ["late"])
        release.set()
        self.assertTrue(job.wait(5))

    def test_hook_exception_is_logged_and_cancel_still_succeeds(self) -> None:
        dispatcher = WorkerDispatcher("s")
        job, release = self._block(dispatcher)

        def hook() -> None:
            raise OSError("terminate failed")

        job.set_cancel_hook(hook)
        with self.assertLogs("idalib_mcp.dispatcher", "ERROR") as logs:
            self.assertEqual(dispatcher.cancel(job.id), "cancelling")
        self.assertIn("terminate failed", "\n".join(logs.output))
        release.set()
        self.assertTrue(job.wait(5))
        self.assertEqual(job.status, "cancelled")


class WorkerDispatcherCloseTests(_QueueTestCase):
    def test_close_fails_queued_jobs_and_lets_running_finish(self) -> None:
        from idalib_mcp.dispatcher import QueueClosedError

        dispatcher = WorkerDispatcher("s")
        blocker, release = self._block(dispatcher)
        called = threading.Event()
        first = dispatcher.submit("q1", 1, {}, lambda _job: called.set())
        second = dispatcher.submit("q2", 2, {}, lambda _job: called.set())

        failed = dispatcher.close("bye")
        self.assertEqual([job.id for job in failed], [first.id, second.id])
        for job in failed:
            self.assertTrue(job.is_done())
            self.assertEqual(job.status, "error")
            self.assertEqual(job.info()["error"], "bye")
            self.assertIsNone(job.exception())
        self.assertTrue(dispatcher.closed)
        self.assertEqual(dispatcher.close_reason, "bye")
        self.assertIs(dispatcher.current_job(), blocker)
        self.assertEqual(blocker.status, "running")
        with self.assertRaises(QueueClosedError) as ctx:
            dispatcher.submit("late", 3, {}, lambda _job: None)
        self.assertEqual(str(ctx.exception), "bye")
        self.assertEqual(dispatcher.close("again"), [])
        self.assertEqual(dispatcher.close_reason, "bye")

        release.set()
        self.assertTrue(blocker.wait(5))
        self.assertEqual(blocker.result(), "blocked")
        self.assertFalse(called.is_set())
        dispatcher._thread.join(5)
        self.assertFalse(dispatcher._thread.is_alive())

    def test_submit_close_while_draining(self) -> None:
        from idalib_mcp.dispatcher import QueueClosedError

        dispatcher = WorkerDispatcher("s")
        _blocker, release = self._block(dispatcher)
        close_job = dispatcher.submit("idalib_close", 1, {}, lambda _job: "closed", kind="close")
        self.assertTrue(dispatcher.draining())

        for kind in ("tool", "close"):
            with self.assertRaises(QueueClosedError) as ctx:
                dispatcher.submit("t", 2, {}, lambda _job: None, kind=kind)
            self.assertEqual(str(ctx.exception), "Database s is closing")
        self.assertEqual([r["id"] for r in dispatcher.snapshot()["queued"]], [close_job.id])

        self.assertEqual(dispatcher.cancel(close_job.id), "removed")
        self.assertFalse(dispatcher.draining())
        accepted = dispatcher.submit("t", 3, {}, lambda _job: "ok")
        release.set()
        self.assertTrue(accepted.wait(5))
        self.assertEqual(accepted.result(), "ok")

    def test_running_close_job_rejects_with_close_reason_once_closed(self) -> None:
        from idalib_mcp.dispatcher import QueueClosedError

        dispatcher = WorkerDispatcher("s")
        started = threading.Event()
        proceed = threading.Event()
        closed = threading.Event()
        release = threading.Event()
        self.addCleanup(proceed.set)
        self.addCleanup(release.set)

        def close_body(_job):
            started.set()
            proceed.wait(10)
            dispatcher.close("Session closed: s")
            closed.set()
            release.wait(10)
            return "closed"

        job = dispatcher.submit("idalib_close", 1, {}, close_body, kind="close")
        self.assertTrue(started.wait(5))
        self.assertTrue(dispatcher.draining())
        with self.assertRaises(QueueClosedError) as ctx:
            dispatcher.submit("t", 2, {}, lambda _job: None)
        self.assertEqual(str(ctx.exception), "Database s is closing")

        proceed.set()
        self.assertTrue(closed.wait(5))
        with self.assertRaises(QueueClosedError) as ctx:
            dispatcher.submit("t", 3, {}, lambda _job: None)
        self.assertEqual(str(ctx.exception), "Session closed: s")
        release.set()
        self.assertTrue(job.wait(5))
        self.assertEqual(job.status, "ok")
        dispatcher._thread.join(5)
        self.assertFalse(dispatcher._thread.is_alive())


class WorkerDispatcherResultTests(_QueueTestCase):
    def test_result_ttl_expires_result_but_keeps_record(self) -> None:
        dispatcher = WorkerDispatcher("s")
        dispatcher.RESULT_TTL = 0.2
        job = dispatcher.submit("t", 1, {}, lambda _job: {"v": 1})
        self.assertTrue(job.wait(5))
        self.assertTrue(job.result_available())
        self.assertEqual(job.result(), {"v": 1})

        time.sleep(0.3)
        self.assertFalse(job.result_available())
        with self.assertRaises(LookupError):
            job.result()
        self.assertIs(dispatcher.job(job.id), job)
        self.assertFalse(job.info()["result_available"])
        record = dispatcher.snapshot()["history"][0]
        self.assertEqual(record["id"], job.id)
        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["result_preview"], '{"v":1}')
        self.assertFalse(record["result_available"])

    def test_history_eviction_makes_job_unknown(self) -> None:
        dispatcher = WorkerDispatcher("s")
        dispatcher.HISTORY_CAP = 3
        jobs = []
        for i in range(4):
            job = dispatcher.submit(f"t{i}", i, {}, lambda _job: "v")
            self.assertTrue(job.wait(5))
            jobs.append(job)

        evicted, kept = jobs[0], jobs[-1]
        self.assertIsNone(dispatcher.job(evicted.id))
        self.assertEqual(dispatcher.cancel(evicted.id), "unknown")
        self.assertIsNone(dispatcher.job_info(evicted.id))
        self.assertEqual(evicted.info(), {})
        self.assertFalse(evicted.result_available())
        self.assertIs(dispatcher.job(kept.id), kept)
        self.assertEqual(dispatcher.cancel(kept.id), "finished")
        self.assertEqual([r["tool"] for r in dispatcher.snapshot()["history"]], ["t1", "t2", "t3"])
        self.assertEqual(dispatcher.cancel(123), "unknown")


class WorkerDispatcherRunTests(_QueueTestCase):
    def test_run_reraises_same_exception_object(self) -> None:
        dispatcher = WorkerDispatcher("s")
        error = ValueError("same object")

        def boom(_cancel):
            raise error

        with self.assertLogs("idalib_mcp.dispatcher", "ERROR"):
            with self.assertRaises(ValueError) as ctx:
                dispatcher.run("boom", 1, {}, boom, threading.Event())
        self.assertIs(ctx.exception, error)

    def test_run_from_own_executor_raises_runtime_error(self) -> None:
        dispatcher = WorkerDispatcher("s")

        def body(_job):
            try:
                dispatcher.run("inner", 2, {}, lambda _cancel: "inner", threading.Event())
            except RuntimeError as exc:
                return str(exc)
            return "no error"

        job = dispatcher.submit("outer", 1, {}, body)
        self.assertTrue(job.wait(5))
        self.assertEqual(job.result(), "WorkerDispatcher.run called from its own executor")
        self.assertEqual([r["tool"] for r in dispatcher.snapshot()["history"]], ["outer"])

    def test_run_raises_queue_closed_when_closed_while_queued(self) -> None:
        from idalib_mcp.dispatcher import QueueClosedError

        dispatcher = WorkerDispatcher("s")
        _blocker, release = self._block(dispatcher)
        outcome: list[BaseException] = []

        def caller() -> None:
            try:
                dispatcher.run("queued", 1, {}, lambda _cancel: "unreachable", threading.Event())
            except BaseException as exc:
                outcome.append(exc)

        thread = threading.Thread(target=caller)
        thread.start()
        self._wait_queued(dispatcher, 1)
        dispatcher.close("gone")
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], QueueClosedError)
        self.assertEqual(str(outcome[0]), "gone")
        release.set()

    def test_run_returns_value_with_zero_result_ttl(self) -> None:
        dispatcher = WorkerDispatcher("s")
        dispatcher.RESULT_TTL = 0
        value = dispatcher.run("t", 1, {}, lambda _cancel: {"v": 2}, threading.Event())
        self.assertEqual(value, {"v": 2})
        self.assertFalse(dispatcher.snapshot()["history"][0]["result_available"])

    def test_idle_executor_drops_last_result(self) -> None:
        class Payload:
            pass

        dispatcher = WorkerDispatcher("s")
        dispatcher.RESULT_TTL = 0
        dispatcher.HISTORY_CAP = 0
        value = dispatcher.run("t", 1, {}, lambda _cancel: Payload(), threading.Event())
        ref = weakref.ref(value)
        del value

        deadline = time.monotonic() + 2
        while True:
            gc.collect()
            if ref() is None:
                break
            if time.monotonic() > deadline:
                self.fail("idle executor still holds the last job's result")
            time.sleep(0.01)


class JobWaitTests(_QueueTestCase):
    def test_wait_beyond_timeout_max_blocks_until_done(self) -> None:
        for timeout in (float("inf"), threading.TIMEOUT_MAX * 10):
            with self.subTest(timeout=timeout):
                dispatcher = WorkerDispatcher("s")
                job, release = self._block(dispatcher)
                entering = threading.Event()
                returned = threading.Event()
                outcome: list[object] = []

                def waiter() -> None:
                    entering.set()
                    try:
                        outcome.append(job.wait(timeout))
                    except BaseException as exc:
                        outcome.append(exc)
                    returned.set()

                threading.Thread(target=waiter, daemon=True).start()
                self.assertTrue(entering.wait(5))
                self.assertFalse(returned.wait(0.2))
                self.assertFalse(job.is_done())
                release.set()
                self.assertTrue(returned.wait(5))
                self.assertEqual(outcome, [True])

    def test_wait_non_positive_or_nan_returns_at_once(self) -> None:
        dispatcher = WorkerDispatcher("s")
        job, release = self._block(dispatcher)
        for timeout in (float("nan"), 0, -1.0, float("-inf")):
            with self.subTest(timeout=timeout):
                outcome: list[bool] = []
                self.assertTrue(_completes_in_thread(lambda: outcome.append(job.wait(timeout)), 1.0))
                self.assertEqual(outcome, [False])
        release.set()
        self.assertTrue(job.wait(None))


class JobInfoTests(_QueueTestCase):
    KEYS = {
        "job_id", "database", "tool", "kind", "status", "phase", "queue_position", "jobs_ahead",
        "enqueued_at", "started_at", "finished_at", "elapsed_seconds", "run_seconds", "error",
        "result_available", "args_preview",
    }

    def test_job_info_keys_and_queue_positions(self) -> None:
        dispatcher = WorkerDispatcher("db")
        running, release = self._block(dispatcher)
        first = dispatcher.submit(7, 1, {"a": 1}, lambda _job: "first")
        second = dispatcher.submit("second", 2, {}, lambda _job: "second", kind="close")
        first.set_phase("waiting")

        infos = [running.info(), first.info(), second.info()]
        for info in infos:
            self.assertEqual(set(info), self.KEYS)
        self.assertEqual(
            [(i["status"], i["queue_position"], i["jobs_ahead"]) for i in infos],
            [("running", None, 0), ("queued", 1, 1), ("queued", 2, 2)],
        )
        self.assertEqual(infos[1]["tool"], "7")
        self.assertEqual(infos[1]["database"], "db")
        self.assertEqual(infos[1]["phase"], "waiting")
        self.assertEqual(infos[1]["args_preview"], '{"a":1}')
        self.assertEqual(infos[2]["kind"], "close")
        self.assertIsNone(infos[1]["started_at"])
        self.assertIsNone(infos[1]["run_seconds"])
        self.assertIsInstance(infos[0]["started_at"], str)
        self.assertEqual(dispatcher.snapshot()["queued"][0]["phase"], "waiting")

        enqueued = dispatcher.snapshot()["queued"][0]["enqueued_at"]
        self.assertEqual(dispatcher.job_info(first.id, now=enqueued + 12.34)["elapsed_seconds"], 12.3)
        self.assertEqual(dispatcher.job_info(first.id, now=enqueued - 5)["elapsed_seconds"], 0.0)

        listing = dispatcher.jobs_info()
        self.assertEqual(listing["running"]["job_id"], running.id)
        self.assertEqual([i["job_id"] for i in listing["queued"]], [first.id, second.id])
        self.assertEqual(listing["recent"], [])
        with self.assertRaises(TypeError):
            dispatcher.jobs_info(recent="2")

        release.set()
        self.assertTrue(second.wait(5))
        done = first.info()
        self.assertEqual((done["status"], done["queue_position"], done["jobs_ahead"]), ("ok", None, None))
        self.assertIsInstance(done["finished_at"], str)
        self.assertTrue(done["result_available"])
        listing = dispatcher.jobs_info(recent=2)
        self.assertIsNone(listing["running"])
        self.assertEqual([i["job_id"] for i in listing["recent"]], [second.id, first.id])
        self.assertEqual(dispatcher.jobs_info(recent=-1)["recent"], [])


if __name__ == "__main__":
    unittest.main()
