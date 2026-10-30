# idalib MCP Headless

Fresh headless MCP supervisor for IDA Pro 9.x through `idalib`.

This project keeps the IDA-facing tool surface compatible with `mrexodia/ida-pro-mcp` by using that package at runtime, then adds a thin supervisor layer for:

- multiple independent worker processes, one per open database/debug session;
- command-line IDA installation selection with `--ida-home`;
- a simple browser UI for listing and closing instances;
- MCP management tools such as `idalib_open`, `idalib_list`, and `idalib_close`;
- one FIFO job queue per database: a call still running after `--sync-wait` seconds returns a job ticket instead of blocking.

## Install

Use a project virtual environment with a Python supported by IDA/idapro and the upstream MCP package.

```powershell
cd D:\Projects\idalib-mcp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

After the project is published, installation can use the GitHub archive directly:

```powershell
pip install https://github.com/nyaoouo/idalib-mcp/archive/refs/heads/main.zip
```

If the `idapro` Python package is not already available, install it from your IDA installation:

```powershell
pip install D:\tools\IDA_PRO_9.1\idalib\python
```

`--ida-home` configures idalib for the worker process without rewriting your normal `%APPDATA%\Hex-Rays\IDA Pro\ida-config.json`.

## MCP Client Setup

Print generic MCP client snippets:

```powershell
idalib-mcp-headless --config --ida-home D:\tools\IDA_PRO_9.1
```

List supported client targets:

```powershell
idalib-mcp-headless --list-clients
```

Install project-level MCP config for a supported client:

```powershell
idalib-mcp-headless --install cursor --scope project --ida-home D:\tools\IDA_PRO_9.1
```

Install a stdio config that lets the MCP client launch the headless server itself:

```powershell
idalib-mcp-headless --install claude --transport stdio --scope global --ida-home D:\tools\IDA_PRO_9.1
```

In stdio mode the MCP protocol still uses stdin/stdout, and the instance manager is started as a local HTTP sidecar. The server writes the web UI URL to stderr, for example:

```text
Instance UI: http://127.0.0.1:8745/instances
```

If the configured port is already in use, stdio mode retries with an ephemeral port and prints the actual URL.

Install an HTTP config when you run the server separately:

```powershell
idalib-mcp-headless --install vscode --transport streamable-http --scope project --host 127.0.0.1 --port 8745
```

Remove config entries with `--uninstall`, for example:

```powershell
idalib-mcp-headless --uninstall cursor --scope project
```

This project is headless, so the installer only updates MCP client configuration. There is no IDA GUI plugin to install.

## Run

```powershell
idalib-mcp-headless --ida-home D:\tools\IDA_PRO_9.1 --host 127.0.0.1 --port 8745 --max-workers 4
```

Open the instance manager at:

```text
http://127.0.0.1:8745/instances
```

Connect MCP clients to:

```text
http://127.0.0.1:8745/mcp
```

Debugger tools are inherited from the upstream package and are hidden behind the `dbg` extension. Start the server with `--unsafe`, then connect to:

```text
http://127.0.0.1:8745/mcp?ext=dbg
```

Each `idalib_open` call creates or reuses a distinct worker process. Normal upstream tools receive an optional `database` argument at the supervisor layer so calls can target a specific session by session id, filename, or path.

`--max-workers` (default 4, `0` means unlimited) caps the worker processes, and each worker holds one IDA license seat. An `idalib_open` call takes its seat when it starts spawning its worker, so databases still opening count; at the limit, a call that needs a new worker fails at once.

## Useful Commands

Open a database through MCP:

```json
{"name":"idalib_open","arguments":{"input_path":"C:\\path\\to\\sample.exe","session_id":"sample-a"}}
```

List instances through MCP:

```json
{"name":"idalib_list","arguments":{}}
```

Close an instance through MCP:

```json
{"name":"idalib_close","arguments":{"session_id":"sample-a"}}
```

The browser UI uses the same supervisor session table and calls the close operation server-side.
When closing an instance from the browser UI, choose whether to save the IDB before the worker is stopped.
The browser UI State column shows each database's state and its queue, and a close from the UI waits its turn in that queue.

## Long-running Calls and Jobs

Calls for one database run one at a time, in order, in that database's FIFO queue: worker tools, `idalib_open` for a path that is not open yet, `idalib_close`, `idalib_save`, `idalib_health`, and `idalib_warmup`. Different databases run in parallel, and the other management tools answer without queueing, over HTTP and stdio alike. The management calls listed above, and a close from the UI, wait behind the earlier calls to the same database.

`--sync-wait SECONDS` (env `IDA_MCP_SYNC_WAIT`, default 15) sets how long a call waits: if it finishes in time it returns its normal result, otherwise it returns a job ticket and keeps running. `0` means no wait, and a negative or non-finite value stops the server at startup. A client that cannot handle tickets can raise `--sync-wait`, but should keep it below its own request timeout: a client that times out during the wait gets neither the result nor a ticket.

A ticket has `isError: true` but is not a failure, so do not repeat the call. Its text is JSON with `"pending": true`, `job` (with `job_id` and `status`), `database_state`, and ready-made `poll` and `cancel` calls; the same object is in `_meta.idalib_job`.

Poll a job through MCP:

```json
{"name":"idalib_job_status","arguments":{"job_id":"3f9c2a7b1e04"}}
```

`idalib_job_status` waits up to `wait_seconds` (0-30, default 30), then returns the original call's result if the job has finished, otherwise its status. Results are kept for 10 minutes after the job finishes.

List jobs through MCP:

```json
{"name":"idalib_jobs","arguments":{"database":"sample-a"}}
```

`idalib_jobs` lists running, queued, and recent jobs (`recent`, default 20) for every database or for one `database`. A closed database stays listed for 10 minutes.

Cancel a job through MCP:

```json
{"name":"idalib_job_cancel","arguments":{"job_id":"3f9c2a7b1e04"}}
```

`idalib_job_cancel` drops a queued job and terminates the worker of an `idalib_open` still loading; any other running job finishes in its worker, its result is discarded, and the next job starts only then. Clients should read `outcome` (`removed`, `cancelling`, `already_cancelling`, `finished`), not `message`. Cancelling the MCP request, or an HTTP disconnect, during the `--sync-wait` window cancels its job; after a ticket, only `idalib_job_cancel` does.

`idalib_list` entries carry `metadata.state` (`spawning`, `opening`, `reopening`, `ready`, or `closing`), `metadata.queued_jobs`, and `metadata.running_job`; tickets and `idalib_jobs` can also show `closed`. A database is listed from its `idalib_open` call on, and `idalib_current` or `idalib_switch` on it fails while it is still opening. A queued close makes the database reject new calls.

## Validation

Run the dependency-free local checks:

```powershell
python -m unittest discover -s tests -p "test*.py"
python -m py_compile src\idalib_mcp\config.py src\idalib_mcp\installer.py src\idalib_mcp\worker.py src\idalib_mcp\server.py
```

For an end-to-end IDA smoke test, install the dependencies in a virtual environment and start the server with the `--ida-home` path shown above.
