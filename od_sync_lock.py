"""
Cross-process lock shared by the dashboard Sync button and morning_od_sync.

Only blocks overlapping OD syncs. Does not affect other dashboard features.
Uses flock on Linux (server) and msvcrt locking on Windows (local testing).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent
LOCK_PATH = REPO_ROOT / "logs" / "od_sync.lock"


def try_acquire_sync_lock(holder: str) -> Optional[int]:
    """
    Non-blocking exclusive lock.

    Returns a lock fd to pass to release_sync_lock.
    Raises BlockingIOError if another process already holds the lock.
    """
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o644)
    payload = f"{os.getpid()} {holder}\n".encode()
    try:
        if os.name == "posix":
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, payload)
        else:
            import msvcrt

            # Write first so a lockable byte exists, then lock byte 0.
            os.ftruncate(fd, 0)
            os.write(fd, payload)
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    except (BlockingIOError, OSError):
        os.close(fd)
        raise BlockingIOError(f"OD sync lock busy: {LOCK_PATH}")

    return fd


def release_sync_lock(fd: Optional[int]) -> None:
    if fd is None:
        return
    try:
        if os.name == "posix":
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
        else:
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        os.close(fd)
    except Exception:
        pass


def sync_lock_busy() -> bool:
    """True if another process currently holds the OD sync lock."""
    try:
        fd = try_acquire_sync_lock("probe")
    except BlockingIOError:
        return True
    release_sync_lock(fd)
    return False


def sync_lock_holder() -> str:
    """Best-effort holder string from the lock file (may be stale if unlocked)."""
    try:
        text = LOCK_PATH.read_text(encoding="utf-8").strip()
        return text or "unknown"
    except Exception:
        return "unknown"
