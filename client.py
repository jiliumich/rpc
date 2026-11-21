"""Demo client. Run (with server.py running):  python3 client.py [storage_dir]"""
import sys

from rpc import RpcClient, RpcError
from storage import FileStorage

client = RpcClient(FileStorage(sys.argv[1] if len(sys.argv) > 1 else "./rpc_data"))

print("add(2, 3) =", client.add(2, 3))
print("echo('hi') =", client.echo("hi"))
try:
    client.divide(1, 0)
except RpcError as e:
    print("divide(1, 0) raised:", e)
