# idalib MCP Headless

一个基于 `idalib` 的全新 IDA Pro 9.x 无头 MCP supervisor。

本项目在运行时复用 `mrexodia/ida-pro-mcp`，从而保持面向 IDA 的工具接口兼容，同时增加一层轻量 supervisor，用于提供：

- 多个独立 worker 进程，每个打开的数据库/调试会话对应一个 worker；
- 通过 `--ida-home` 在命令行指定 IDA 安装目录；
- 用于列出和关闭实例的简单浏览器 UI；
- `idalib_open`、`idalib_list`、`idalib_close` 等 MCP 管理工具；
- 每个数据库一个 FIFO job 队列：调用在 `--sync-wait` 秒后仍未完成时，返回 job ticket，而不是一直阻塞。

## 安装

建议使用项目级虚拟环境，并选择 IDA/idapro 与上游 MCP 包支持的 Python 版本。

```powershell
cd D:\Projects\idalib-mcp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

项目发布后，也可以直接通过 GitHub archive 安装：

```powershell
pip install https://github.com/nyaoouo/idalib-mcp/archive/refs/heads/main.zip
```

如果当前环境还没有 `idapro` Python 包，请从 IDA 安装目录安装：

```powershell
pip install D:\tools\IDA_PRO_9.1\idalib\python
```

`--ida-home` 会为 worker 进程配置 idalib，不会改写你正常使用的 `%APPDATA%\Hex-Rays\IDA Pro\ida-config.json`。

## MCP 客户端配置

打印通用 MCP 客户端配置片段：

```powershell
idalib-mcp-headless --config --ida-home D:\tools\IDA_PRO_9.1
```

列出支持的客户端目标：

```powershell
idalib-mcp-headless --list-clients
```

为支持的客户端安装项目级 MCP 配置：

```powershell
idalib-mcp-headless --install cursor --scope project --ida-home D:\tools\IDA_PRO_9.1
```

安装 stdio 配置，让 MCP 客户端自行启动无头 server：

```powershell
idalib-mcp-headless --install claude --transport stdio --scope global --ida-home D:\tools\IDA_PRO_9.1
```

在 stdio 模式下，MCP 协议仍然使用 stdin/stdout；实例管理器会作为本地 HTTP sidecar 启动。server 会把 Web UI URL 写到 stderr，例如：

```text
Instance UI: http://127.0.0.1:8745/instances
```

如果配置的端口已被占用，stdio 模式会改用临时端口，并打印实际 URL。

如果你单独运行 server，可以安装 HTTP 配置：

```powershell
idalib-mcp-headless --install vscode --transport streamable-http --scope project --host 127.0.0.1 --port 8745
```

可以用 `--uninstall` 删除配置项，例如：

```powershell
idalib-mcp-headless --uninstall cursor --scope project
```

本项目是无头模式，因此安装器只更新 MCP 客户端配置，不会安装 IDA GUI 插件。

## 运行

```powershell
idalib-mcp-headless --ida-home D:\tools\IDA_PRO_9.1 --host 127.0.0.1 --port 8745 --max-workers 4
```

打开实例管理器：

```text
http://127.0.0.1:8745/instances
```

MCP 客户端连接地址：

```text
http://127.0.0.1:8745/mcp
```

调试器工具继承自上游包，并通过 `dbg` 扩展隐藏。启动 server 时加上 `--unsafe`，然后连接到：

```text
http://127.0.0.1:8745/mcp?ext=dbg
```

每次调用 `idalib_open` 都会创建或复用一个独立 worker 进程。普通上游工具在 supervisor 层支持可选的 `database` 参数，因此可以用 session id、文件名或路径指定目标会话。

`--max-workers`（默认 4，`0` 表示不限制）限制 worker 进程数量，每个 worker 占用一个 IDA license 席位。`idalib_open` 调用从开始启动 worker 时占用席位，因此仍在打开的数据库也会计入；达到上限后，需要新 worker 的调用会立即失败。

## 常用命令

通过 MCP 打开数据库：

```json
{"name":"idalib_open","arguments":{"input_path":"C:\\path\\to\\sample.exe","session_id":"sample-a"}}
```

通过 MCP 列出实例：

```json
{"name":"idalib_list","arguments":{}}
```

通过 MCP 关闭实例：

```json
{"name":"idalib_close","arguments":{"session_id":"sample-a"}}
```

浏览器 UI 使用同一份 supervisor session 表，并在 server 端执行关闭操作。
从浏览器 UI 关闭实例时，可以选择是否先保存 IDB，然后再停止 worker。
浏览器 UI 的 State 列显示每个数据库的状态和队列，从 UI 关闭实例时也要在该队列中按顺序等待。

## 耗时调用与 job

同一数据库的调用在该数据库的 FIFO 队列中按顺序逐个执行：worker 工具、`idalib_open`（针对尚未打开的路径）、`idalib_close`、`idalib_save`、`idalib_health` 和 `idalib_warmup`。不同数据库并行执行，其他管理工具无需排队即可返回，HTTP 和 stdio 均如此。上述管理调用以及从 UI 关闭实例，都要排在同一数据库此前的调用之后。

`--sync-wait SECONDS`（环境变量 `IDA_MCP_SYNC_WAIT`，默认 15）决定调用最多等待多久：及时完成则返回正常结果，否则返回 job ticket，调用继续在后台运行。`0` 表示不等待，负数或非有限值会使 server 在启动时退出。无法处理 ticket 的客户端可以调大 `--sync-wait`，但应保持低于客户端自身的请求超时：在等待期间超时的客户端既拿不到结果，也拿不到 ticket。

ticket 带有 `isError: true`，但并不是失败，请不要重复调用。其文本是 JSON，包含 `"pending": true`、`job`（含 `job_id` 和 `status`）、`database_state`，以及现成的 `poll` 和 `cancel` 调用；同一对象也出现在 `_meta.idalib_job` 中。

通过 MCP 轮询 job：

```json
{"name":"idalib_job_status","arguments":{"job_id":"3f9c2a7b1e04"}}
```

`idalib_job_status` 最多等待 `wait_seconds` 秒（0-30，默认 30），随后若 job 已完成则返回原调用的结果，否则返回其状态。结果在 job 完成后保留 10 分钟。

通过 MCP 列出 job：

```json
{"name":"idalib_jobs","arguments":{"database":"sample-a"}}
```

`idalib_jobs` 列出所有数据库或指定 `database` 的运行中、排队中和最近的 job（`recent`，默认 20）。已关闭的数据库会继续保留在列表中 10 分钟。

通过 MCP 取消 job：

```json
{"name":"idalib_job_cancel","arguments":{"job_id":"3f9c2a7b1e04"}}
```

`idalib_job_cancel` 会移除排队中的 job，并终止仍在加载的 `idalib_open` 的 worker；其他运行中的 job 会在 worker 中执行完毕，结果被丢弃，下一个 job 此时才开始。客户端应读取 `outcome`（`removed`、`cancelling`、`already_cancelling`、`finished`），而不是 `message`。在 `--sync-wait` 等待期间取消 MCP 请求或 HTTP 连接断开，会取消该请求的 job；返回 ticket 之后只有 `idalib_job_cancel` 能取消。

`idalib_list` 的条目带有 `metadata.state`（`spawning`、`opening`、`reopening`、`ready` 或 `closing`）、`metadata.queued_jobs` 和 `metadata.running_job`；ticket 和 `idalib_jobs` 还可能显示 `closed`。数据库从 `idalib_open` 调用起就会出现在列表中，在其仍处于打开过程时，对它执行 `idalib_current` 或 `idalib_switch` 会失败。排队中的关闭操作会使该数据库拒绝新的调用。

## 验证

运行不依赖 IDA 的本地检查：

```powershell
python -m unittest discover -s tests -p "test*.py"
python -m py_compile src\idalib_mcp\config.py src\idalib_mcp\installer.py src\idalib_mcp\worker.py src\idalib_mcp\server.py
```

如需端到端 IDA smoke test，请先在虚拟环境中安装依赖，然后使用上文中的 `--ida-home` 路径启动 server。
