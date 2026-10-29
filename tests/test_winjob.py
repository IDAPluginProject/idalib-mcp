from __future__ import annotations

import subprocess
import sys
import unittest

from idalib_mcp.winjob import assign_process_to_job, create_kill_on_close_job


class WinJobTests(unittest.TestCase):
    def test_no_job_off_windows(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Windows behaviour is covered separately")
        self.assertIsNone(create_kill_on_close_job())
        self.assertFalse(assign_process_to_job(None, object()))

    def test_assign_is_noop_without_job(self) -> None:
        # Regardless of platform, assigning to a missing job must not raise.
        self.assertFalse(assign_process_to_job(None, object()))

    @unittest.skipUnless(sys.platform == "win32", "Windows-only Job Object behaviour")
    def test_kill_on_close_terminates_assigned_process(self) -> None:
        import ctypes

        job = create_kill_on_close_job()
        self.assertIsNotNone(job)

        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            self.assertTrue(assign_process_to_job(job, proc))
            # Closing the only handle to the job triggers kill-on-close.
            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(job)
            # The OS should terminate the assigned process; wait() must return
            # (rather than raise TimeoutExpired) within the grace window.
            proc.wait(timeout=10)
            self.assertIsNotNone(proc.poll())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
