"""Shared storage backends used as an RPC transport.

Any backend only needs these five operations. Swap FileStorage for an
S3Storage / RedisStorage later without touching rpc.py.
"""
from __future__ import annotations

import os
import threading
import uuid
from abc import ABC, abstractmethod


class SharedStorage(ABC):
    """A key -> bytes store that two processes can both read and write."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> None:
        """Write atomically: readers see the whole value or nothing."""

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Return the value, or None if the key does not exist."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove the key; a missing key is not an error."""

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """Return keys starting with prefix, sorted (oldest first if keys are time-ordered)."""

    @abstractmethod
    def claim(self, src: str, dst: str) -> bool:
        """Atomically move src -> dst. Return False if someone else got it first."""


class FileStorage(SharedStorage):
    """Keys are paths under a root directory, e.g. 'requests/abc.json'."""

    def __init__(self, root: str):
        self.root = os.path.abspath(root)

    def _path(self, key: str) -> str:
        path = os.path.abspath(os.path.join(self.root, key))
        if not path.startswith(self.root + os.sep):
            raise ValueError(f"key escapes storage root: {key!r}")
        return path

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Write to a hidden temp file in the same dir, then rename (atomic on POSIX).
        tmp = os.path.join(os.path.dirname(path), f".tmp-{uuid.uuid4().hex}")
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def get(self, key: str) -> bytes | None:
        try:
            with open(self._path(key), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def delete(self, key: str) -> None:
        try:
            os.remove(self._path(key))
        except FileNotFoundError:
            pass

    def list(self, prefix: str) -> list[str]:
        # prefix is treated as "<dir>/<name-prefix>"
        dir_part, _, name_prefix = prefix.rpartition("/")
        directory = self._path(dir_part) if dir_part else self.root
        try:
            names = os.listdir(directory)
        except FileNotFoundError:
            return []
        keys = [
            f"{dir_part}/{n}" if dir_part else n
            for n in names
            if n.startswith(name_prefix) and not n.startswith(".")
        ]
        return sorted(keys)

    def claim(self, src: str, dst: str) -> bool:
        dst_path = self._path(dst)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        try:
            os.rename(self._path(src), dst_path)  # only one racer succeeds
            return True
        except FileNotFoundError:
            return False


class MemoryStorage(SharedStorage):
    """In-process store; handy for tests (client and server in threads)."""

    def __init__(self):
        self._data: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def put(self, key: str, data: bytes) -> None:
        with self._lock:
            self._data[key] = bytes(data)

    def get(self, key: str) -> bytes | None:
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def list(self, prefix: str) -> list[str]:
        with self._lock:
            return sorted(k for k in self._data if k.startswith(prefix))

    def claim(self, src: str, dst: str) -> bool:
        with self._lock:
            if src not in self._data:
                return False
            self._data[dst] = self._data.pop(src)
            return True
