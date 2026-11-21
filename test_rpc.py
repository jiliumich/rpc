"""Run:  python3 -m unittest -v"""
import tempfile
import threading
import unittest

from rpc import RpcClient, RpcError, RpcServer
from storage import FileStorage, MemoryStorage


class RpcTests:
    """Shared tests; subclasses provide make_storage()."""

    def setUp(self):
        self.storage = self.make_storage()
        self.server = RpcServer(self.storage, poll_interval=0.01)
        self.server.register(lambda a, b: a + b, name="add")
        self.server.register(lambda a, b: a / b, name="divide")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = RpcClient(self.storage, timeout=5, poll_interval=0.01)

    def tearDown(self):
        self.server.stop()
        self.thread.join(timeout=2)

    def test_call(self):
        self.assertEqual(self.client.add(2, 3), 5)
        self.assertEqual(self.client.call("add", a="x", b="y"), "xy")

    def test_remote_error(self):
        with self.assertRaisesRegex(RpcError, "ZeroDivisionError"):
            self.client.divide(1, 0)

    def test_unknown_method(self):
        with self.assertRaisesRegex(RpcError, "unknown method"):
            self.client.nope()

    def test_concurrent_clients_and_cleanup(self):
        results = {}

        def worker(i):
            results[i] = self.client.add(i, i)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results, {i: 2 * i for i in range(20)})
        for prefix in ("requests/", "processing/", "responses/"):
            self.assertEqual(self.storage.list(prefix), [], prefix)

    def test_timeout_withdraws_request(self):
        self.server.stop()
        self.thread.join(timeout=2)
        with self.assertRaises(TimeoutError):
            self.client.call("add", 1, 2, timeout=0.1)
        self.assertEqual(self.storage.list("requests/"), [])

    def test_malformed_request(self):
        req_id = "malformed-request"
        self.storage.put("requests/malformed-request.json", b"not json")
        self.assertEqual(self.server.process_once(), 1)
        raw = self.storage.get("responses/malformed-request.json")
        self.assertIsNotNone(raw)

    def test_invalid_response_id(self):
        req_id = "bad-response-id"

        def get(key):
            if key.startswith("responses/"):
                return b'{"id":"other","ok":true,"result":1}'
            return self._original_get(key)

        self._original_get = self.storage.get
        self.storage.get = get
        try:
            with self.assertRaisesRegex(RpcError, "request id mismatch"):
                self.client.call("add", 1, 2, timeout=0.05)
        finally:
            self.storage.get = self._original_get


class MemoryStorageTest(RpcTests, unittest.TestCase):
    def make_storage(self):
        return MemoryStorage()


class FileStorageTest(RpcTests, unittest.TestCase):
    def make_storage(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        return FileStorage(self._tmp.name)


if __name__ == "__main__":
    unittest.main()
