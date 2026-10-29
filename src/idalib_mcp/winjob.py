"""Windows Job Object backstop that kills workers when the supervisor exits.

The parent-death watchdog in :mod:`idalib_mcp.parentwatch` gives a graceful
shutdown, but it relies on the worker's interpreter being responsive enough to
process a signal. As a hard, OS-enforced backstop the supervisor places every
worker into a Job Object configured with ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``:
when the supervisor process dies (for any reason, including a hard kill that
skips Python cleanup), the OS closes the job handle and terminates every
assigned worker. This guarantees no worker is ever left orphaned holding an IDA
license seat.

On non-Windows platforms these helpers are no-ops (the watchdog handles those).
"""

from __future__ import annotations

import sys
from typing import Any

__all__ = ["create_kill_on_close_job", "assign_process_to_job"]


def create_kill_on_close_job() -> Any | None:
    """Create a kill-on-close Job Object.

    Returns an opaque job handle on Windows, or ``None`` on other platforms or
    on failure. The handle MUST be kept alive for the lifetime of the supervisor
    — closing it (or the owning process exiting) terminates every assigned
    worker.
    """
    if sys.platform != "win32":
        return None

    import ctypes
    from ctypes import wintypes

    JobObjectExtendedLimitInformation = 9
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.SetInformationJobObject.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)

    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None

    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(
        job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        kernel32.CloseHandle(job)
        return None
    return job


def assign_process_to_job(job: Any | None, process: Any) -> bool:
    """Assign ``process`` (a ``subprocess.Popen``) to ``job``.

    Returns True on success. A no-op returning False when there is no job
    (non-Windows, or job creation failed) so callers can assign unconditionally.
    """
    if job is None or sys.platform != "win32":
        return False

    handle = getattr(process, "_handle", None)
    if handle is None:
        return False

    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
    return bool(kernel32.AssignProcessToJobObject(job, int(handle)))
