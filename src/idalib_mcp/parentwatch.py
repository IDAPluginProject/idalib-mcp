"""Detect supervisor death from a worker and shut down cleanly.

Workers are spawned by the supervisor as child processes. When the supervisor
goes away — including when it is hard-killed and cannot run its own cleanup —
the worker must not linger as an orphan, because each worker holds an IDA
license seat (acquired at ``import idapro``) until its process exits.

This module provides a small daemon thread that watches the supervisor PID and,
when the supervisor disappears, raises ``SIGTERM`` so the worker runs its own
existing graceful shutdown handler (closing IDA sessions and releasing the
license seat). It is cross-platform; on Windows it pairs with the Job Object
backstop in :mod:`idalib_mcp.winjob`.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from typing import Callable

__all__ = ["parent_is_alive", "start_parent_death_watch"]


def parent_is_alive(pid: int) -> bool:
    """Return True if process ``pid`` currently exists.

    Cross-platform and side-effect free: it never sends a real signal to the
    target. On Windows it opens a handle with ``SYNCHRONIZE`` rights and checks
    whether the process object is signaled (exited); on POSIX it uses the
    standard ``os.kill(pid, 0)`` existence probe.
    """
    if pid <= 0:
        return False
    if sys.platform == "win32":
        return _parent_is_alive_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The process exists but is owned by another user — treat as alive.
        return True
    return True


def _parent_is_alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    SYNCHRONIZE = 0x00100000
    WAIT_TIMEOUT = 0x00000102  # still running; anything else => exited/error

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)

    handle = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
    if not handle:
        # Could not open a handle — the process is gone (or never existed).
        return False
    try:
        return kernel32.WaitForSingleObject(handle, 0) == WAIT_TIMEOUT
    finally:
        kernel32.CloseHandle(handle)


def _request_graceful_shutdown() -> None:
    """Ask the current process to shut down gracefully.

    Raising ``SIGTERM`` triggers the worker's own shutdown handler (which closes
    IDA sessions and releases the license seat). If raising the signal is
    unavailable for any reason, fall back to a hard exit so the worker can never
    linger as an orphan.
    """
    try:
        signal.raise_signal(signal.SIGTERM)
    except Exception:
        os._exit(0)


def start_parent_death_watch(
    pid: int,
    *,
    on_parent_exit: Callable[[], None] = _request_graceful_shutdown,
    poll_interval: float = 1.0,
    is_alive: Callable[[int], bool] = parent_is_alive,
) -> threading.Thread:
    """Start a daemon thread that calls ``on_parent_exit`` once ``pid`` dies.

    Returns the started thread. ``on_parent_exit`` and ``is_alive`` are
    injectable to keep the watchdog testable without real processes.
    """

    def _run() -> None:
        while is_alive(pid):
            time.sleep(poll_interval)
        on_parent_exit()

    thread = threading.Thread(target=_run, name="parent-death-watch", daemon=True)
    thread.start()
    return thread
