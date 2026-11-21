"""Demo server. Run:  python3 server.py [storage_dir]
Set RPC_LOG_LEVEL=DEBUG to also see tracebacks and claim races."""
import logging
import os
import signal
import sys

from rpc import RpcServer
from storage import FileStorage

logging.basicConfig(
    level=os.environ.get("RPC_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

server = RpcServer(FileStorage(sys.argv[1] if len(sys.argv) > 1 else "./rpc_data"))


@server.register
def add(a, b):
    return a + b


@server.register
def echo(msg):
    return msg


@server.register
def divide(a, b):
    return a / b


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: server.stop())  # `kill <pid>` stops cleanly
    logging.getLogger("rpc.server").info("storage root: %s  (Ctrl+C to stop)", server.storage.root)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
