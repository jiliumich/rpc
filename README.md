# Minimal Python RPC

A small RPC demo that uses a shared storage directory as a mailbox. It is designed for learning and local, trusted-process communication—not for production use over an untrusted network.

## How it works

- The client writes JSON to `requests/<request-id>.json`.
- The server atomically claims a request by moving it to `processing/<request-id>.json`.
- The server runs the registered Python function and writes `responses/<request-id>.json`.
- The client reads the response, deletes it, and returns or raises the remote result/error.

## Run

Start the server in one terminal:

```bash
python3 server.py
```

Run the demo client in another terminal:

```bash
python3 client.py
```

Both default to `./rpc_data`. A custom storage directory can be supplied:

```bash
python3 server.py ./rpc_data
python3 client.py ./rpc_data
```

## Tests

```bash
python3 -m unittest -v
```

## Storage backends

`SharedStorage` has five operations:

- `put(key, data)`
- `get(key)`
- `delete(key)`
- `list(prefix)`
- `claim(src, dst)`

`claim(src, dst)` must be atomic: exactly one worker may move a request from `requests/` to `processing/`.

## Limitations

- JSON only: arguments and results must be JSON serializable.
- No authentication or encryption.
- No retries, acknowledgements, leases, or distributed crash recovery.
- Shared storage must be available to both processes.
- Server tracebacks are logged but not exposed as client results.

For production, replace `SharedStorage` with a broker or durable queue that provides atomic consumption and visibility leases.
# rpc
