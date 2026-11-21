"""Minimal RPC over any SharedStorage (mailbox transport).

Layout in storage:
    requests/<id>.json    client writes, server claims
    processing/<id>.json  server-owned while running
    responses/<id>.json   server writes, client reads and deletes
"""
from __future__ import annotations

import json
import logging
import time
import traceback
import uuid
from typing import Any, Callable
import threading

from storage import SharedStorage

REQ, PROC, RESP = "requests/", "processing/", "responses/"

log = logging.getLogger("rpc.server")
log.addHandler(logging.NullHandler())  # silent unless the app configures logging


class RpcError(Exception):
    """Raised on the client when the remote call failed."""


def _new_id() -> str:
    # time-ordered prefix gives roughly FIFO processing via sorted list()
    return f"{time.time_ns():020d}-{uuid.uuid4().hex[:8]}"


class RpcServer:
    def __init__(self, storage: SharedStorage, poll_interval: float = 0.05):
        self.storage = storage
        self.poll_interval = poll_interval
        self.methods: dict[str, Callable[..., Any]] = {}
        self._running = False
        self._wake = threading.Event()

    def register(self, fn: Callable | None = None, *, name: str | None = None):
        """Use as @server.register or server.register(fn, name='x')."""
        def deco(f):
            self.methods[name or f.__name__] = f
            return f
        return deco(fn) if fn else deco

    def process_once(self) -> int:
        """Handle all pending requests once; return how many were handled."""
        handled = 0
        for key in self.storage.list(REQ):
            req_id = key[len(REQ):].removesuffix(".json")
            proc_key = f"{PROC}{req_id}.json"
            if not self.storage.claim(key, proc_key):
                log.debug("id=%s already claimed by another worker", req_id)
                continue
            raw = self.storage.get(proc_key)
            start = time.perf_counter()
            resp = self._dispatch(req_id, raw)
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.storage.put(f"{RESP}{req_id}.json", json.dumps(resp).encode())
            self.storage.delete(proc_key)
            if resp["ok"]:
                log.info("id=%s done ok in %.1f ms", req_id, elapsed_ms)
            else:
                log.warning("id=%s failed in %.1f ms: %s", req_id, elapsed_ms, resp["error"])
            handled += 1
        return handled

    def _dispatch(self, req_id: str, raw: bytes | None) -> dict:
        try:
            req = json.loads(raw or b"")
            if not isinstance(req, dict):
                raise ValueError("request must be a JSON object")
            if req.get("id") != req_id:
                raise ValueError("request id mismatch")
            method = req.get("method")
            args = req.get("args", [])
            kwargs = req.get("kwargs", {})
            if not isinstance(method, str) or not method:
                raise ValueError("method must be a non-empty string")
            if not isinstance(args, list):
                raise ValueError("args must be a JSON array")
            if not isinstance(kwargs, dict):
                raise ValueError("kwargs must be a JSON object")

            log.info("id=%s call %s args=%r kwargs=%r", req_id, method, args, kwargs)
            fn = self.methods.get(method)
            if fn is None:
                raise LookupError(f"unknown method: {method!r}")
            result = fn(*args, **kwargs)
            try:
                json.dumps(result)
            except (TypeError, ValueError) as e:
                raise ValueError(f"result is not JSON serializable: {e}") from e
            return {"id": req_id, "ok": True, "result": result}
        except Exception as e:
            log.debug("id=%s traceback:\n%s", req_id, traceback.format_exc())
            return {
                "id": req_id,
                "ok": False,
                "error": f"{type(e).__name__}: {e}",
            }

    def serve_forever(self) -> None:
        self._running = True
        self._wake.clear()
        log.info("server started; methods=%s poll_interval=%ss",
                 sorted(self.methods), self.poll_interval)
        try:
            while self._running:
                if not self.process_once():
                    self._wake.wait(self.poll_interval)
        finally:
            log.info("server stopped")

    def stop(self) -> None:
        self._running = False
        self._wake.set()


class RpcClient:
    def __init__(self, storage: SharedStorage, timeout: float = 10.0,
                 poll_interval: float = 0.02):
        self.storage = storage
        self.timeout = timeout
        self.poll_interval = poll_interval

    def call(self, method: str, *args, timeout: float | None = None, **kwargs) -> Any:
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")

        req_id = _new_id()
        req = {"id": req_id, "method": method, "args": args, "kwargs": kwargs}
        try:
            payload = json.dumps(req).encode()
        except (TypeError, ValueError) as e:
            raise ValueError(f"request is not JSON serializable: {e}") from e
        self.storage.put(f"{REQ}{req_id}.json", payload)

        resp_key = f"{RESP}{req_id}.json"
        deadline = time.monotonic() + (timeout if timeout is not None else self.timeout)
        while time.monotonic() < deadline:
            raw = self.storage.get(resp_key)
            if raw is not None:
                self.storage.delete(resp_key)
                try:
                    resp = json.loads(raw)
                except json.JSONDecodeError as e:
                    raise RpcError(f"Invalid response JSON: {e}") from e
                if not isinstance(resp, dict) or resp.get("id") != req_id:
                    raise RpcError("invalid response: request id mismatch")
                if resp.get("ok") is True:
                    return resp.get("result")
                if resp.get("ok") is False:
                    error = resp.get("error")
                    raise RpcError(error if isinstance(error, str) else "remote call failed")
                raise RpcError("invalid response: missing ok status")
            time.sleep(self.poll_interval)

        # Withdraw the request if the server never picked it up.
        self.storage.delete(f"{REQ}{req_id}.json")
        self.storage.delete(f"{PROC}{req_id}.json")
        raise TimeoutError(f"no response for {method!r} (id={req_id})")

    def __getattr__(self, name: str) -> Callable[..., Any]:
        """Lets you write client.add(2, 3) instead of client.call('add', 2, 3)."""
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda *a, **kw: self.call(name, *a, **kw)
