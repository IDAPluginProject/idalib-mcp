from __future__ import annotations

import os
import subprocess
import sys
import threading
import unittest

from idalib_mcp.parentwatch import parent_is_alive, start_parent_death_watch


class ParentIsAliveTests(unittest.TestCase):
    def test_current_process_is_alive(self) -> None:
        self.assertTrue(parent_is_alive(os.getpid()))

    def test_nonpositive_pid_is_not_alive(self) -> None:
        self.assertFalse(parent_is_alive(0))
        self.assertFalse(parent_is_alive(-1))

    def test_exited_process_is_not_alive(self) -> None:
        proc = subprocess.Popen([sys.executable, "-c", "pass"])
        proc.wait()
        self.assertFalse(parent_is_alive(proc.pid))


class WatchdogTests(unittest.TestCase):
    def test_fires_callback_when_parent_dies(self) -> None:
        fired = threading.Event()
        calls = {"n": 0}

        def fake_alive(_pid: int) -> bool:
            calls["n"] += 1
            return calls["n"] <= 2  # alive twice, then gone

        start_parent_death_watch(
            1234,
            on_parent_exit=fired.set,
            poll_interval=0.01,
            is_alive=fake_alive,
        )
        self.assertTrue(fired.wait(timeout=2.0))

    def test_does_not_fire_while_parent_alive(self) -> None:
        fired = threading.Event()
        start_parent_death_watch(
            1234,
            on_parent_exit=fired.set,
            poll_interval=0.01,
            is_alive=lambda _pid: True,
        )
        self.assertFalse(fired.wait(timeout=0.2))


if __name__ == "__main__":
    unittest.main()
