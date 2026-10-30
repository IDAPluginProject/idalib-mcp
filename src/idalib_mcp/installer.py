from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse


MCP_SERVER_NAME = "idalib-mcp-headless"
REPOSITORY_URL = "https://github.com/nyaoouo/idalib-mcp"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8745
OLD_SERVER_NAMES = {"github.com/nyaoouo/idalib-mcp"}


def _load_installer_data():
    try:
        return importlib.import_module("ida_pro_mcp.installer_data")
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'ida-pro-mcp'. Install this project before using "
            "--install or --list-clients."
        ) from exc


def _load_installer_tui():
    try:
        return importlib.import_module("ida_pro_mcp.installer_tui")
    except ImportError:
        return None


def get_python_executable() -> str:
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        python = Path(venv) / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python3")
        if python.exists():
            return str(python)
    return sys.executable


def copy_python_env(env: dict[str, str]) -> bool:
    python_vars = [
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONSAFEPATH",
        "PYTHONPLATLIBDIR",
        "PYTHONPYCACHEPREFIX",
        "PYTHONNOUSERSITE",
        "PYTHONUSERBASE",
    ]
    copied = False
    for var in python_vars:
        value = os.environ.get(var)
        if value:
            env[var] = value
            copied = True
    return copied


def normalize_transport_url(transport: str) -> str:
    url = urlparse(transport)
    if url.hostname is None or url.port is None:
        raise ValueError(f"Invalid transport URL: {transport}")
    path = url.path or "/mcp"
    if path == "/":
        path = "/mcp"
    return urlunparse((url.scheme, f"{url.hostname}:{url.port}", path, "", "", ""))


def force_mcp_path(transport_url: str) -> str:
    url = urlparse(transport_url)
    return urlunparse((url.scheme, f"{url.hostname}:{url.port}", "/mcp", "", "", ""))


def infer_http_transport_type(transport_url: str) -> str:
    return "sse" if urlparse(transport_url).path.rstrip("/") == "/sse" else "http"


def resolve_transport_url(transport: str | None, *, host: str, port: int) -> str:
    if transport in (None, "http", "streamable-http", "streamable"):
        return f"http://{host}:{port}/mcp"
    if transport == "sse":
        return f"http://{host}:{port}/sse"
    return normalize_transport_url(transport)


def _stdio_args(
    *,
    ida_home: str | Path | None = None,
    unsafe: bool = False,
    isolated_contexts: bool = False,
    max_workers: int | None = None,
    sync_wait: float | None = None,
    profile: str | Path | None = None,
) -> list[str]:
    args = ["-m", "idalib_mcp", "--stdio"]
    if ida_home is not None:
        args.extend(["--ida-home", str(ida_home)])
    if unsafe:
        args.append("--unsafe")
    if isolated_contexts:
        args.append("--isolated-contexts")
    if max_workers is not None:
        args.extend(["--max-workers", str(max_workers)])
    if sync_wait is not None:
        args.extend(["--sync-wait", f"{sync_wait:.15g}"])
    if profile is not None:
        args.extend(["--profile", str(profile)])
    return args


def generate_mcp_config(
    *,
    client_name: str,
    transport: str = "stdio",
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    ida_home: str | Path | None = None,
    unsafe: bool = False,
    isolated_contexts: bool = False,
    max_workers: int | None = None,
    sync_wait: float | None = None,
    profile: str | Path | None = None,
) -> dict[str, Any]:
    if transport == "stdio":
        args = _stdio_args(
            ida_home=ida_home,
            unsafe=unsafe,
            isolated_contexts=isolated_contexts,
            max_workers=max_workers,
            sync_wait=sync_wait,
            profile=profile,
        )
        if client_name == "Opencode":
            mcp_config: dict[str, Any] = {"type": "local", "command": [get_python_executable(), *args]}
        else:
            mcp_config = {"command": get_python_executable(), "args": args}
        env: dict[str, str] = {}
        if copy_python_env(env):
            mcp_config["env"] = env
        return mcp_config

    transport_url = resolve_transport_url(transport, host=host, port=port)
    if client_name == "Opencode":
        return {"type": "remote", "url": transport_url}
    if client_name == "Codex":
        return {"url": force_mcp_path(transport_url)}
    if client_name in ("Claude", "Claude Code"):
        return {"type": infer_http_transport_type(transport_url), "url": transport_url}
    if client_name == "Antigravity IDE":
        return {"type": "http", "serverUrl": force_mcp_path(transport_url)}
    return {"type": "http", "url": force_mcp_path(transport_url)}


def _args_kwargs(args) -> dict[str, Any]:
    return {
        "host": args.host,
        "port": args.port,
        "ida_home": args.ida_home,
        "unsafe": args.unsafe,
        "isolated_contexts": args.isolated_contexts,
        "max_workers": args.max_workers,
        "sync_wait": args.sync_wait,
        "profile": args.profile,
    }


def print_mcp_config(args) -> None:
    kwargs = _args_kwargs(args)
    for title, transport in (
        ("STDIO MCP CONFIGURATION", "stdio"),
        ("STREAMABLE HTTP MCP CONFIGURATION", "streamable-http"),
        ("SSE MCP CONFIGURATION", "sse"),
    ):
        print(f"[{title}]")
        print(
            json.dumps(
                {
                    "mcpServers": {
                        MCP_SERVER_NAME: generate_mcp_config(
                            client_name="Generic",
                            transport=transport,
                            **kwargs,
                        )
                    }
                },
                indent=2,
            )
        )
        print()


def _get_scope_config_spec(
    *, project: bool, project_dir: str | None = None
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str | None, str]]]:
    data = _load_installer_data()
    if project:
        return data.get_project_configs(project_dir or os.getcwd()), data.PROJECT_SPECIAL_JSON_STRUCTURES
    return data.get_global_configs(), data.GLOBAL_SPECIAL_JSON_STRUCTURES


def _read_config_file(config_path: str, *, is_toml: bool) -> dict | None:
    try:
        if is_toml:
            with open(config_path, "rb") as f:
                return tomllib.load(f)
        with open(config_path, "r", encoding="utf-8") as f:
            data = f.read().strip()
        return json.loads(data) if data else {}
    except (json.JSONDecodeError, tomllib.TOMLDecodeError, OSError):
        return None


def _write_config_file(config_path: str, config: dict, *, is_toml: bool) -> None:
    config_dir = os.path.dirname(config_path)
    os.makedirs(config_dir, exist_ok=True)
    suffix = ".toml" if is_toml else ".json"
    fd, temp_path = tempfile.mkstemp(dir=config_dir, prefix=".tmp_", suffix=suffix, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            if is_toml:
                tomli_w = importlib.import_module("tomli_w")
                f.write(tomli_w.dumps(config))
            else:
                json.dump(config, f, indent=2)
        os.replace(temp_path, config_path)
    except Exception:
        os.unlink(temp_path)
        raise


def _get_mcp_servers_view(
    config: dict,
    *,
    client_name: str,
    is_toml: bool,
    special_json_structures: dict[str, tuple[str | None, str]],
) -> dict:
    if is_toml:
        return config.setdefault("mcp_servers", {})
    if client_name in special_json_structures:
        top_key, nested_key = special_json_structures[client_name]
        if top_key is None:
            return config.setdefault(nested_key, {})
        return config.setdefault(top_key, {}).setdefault(nested_key, {})
    return config.setdefault("mcpServers", {})


def _resolve_client_targets(
    configs: dict[str, tuple[str, str]], only: list[str] | None
) -> dict[str, tuple[str, str]]:
    if only is None:
        return configs

    data = _load_installer_data()
    available = list(configs.keys())
    filtered: dict[str, tuple[str, str]] = {}
    for target_name in only:
        resolved = data.resolve_client_name(target_name, available)
        if resolved is None:
            print(f"Unknown client: '{target_name}'. Use --list-clients to see available targets.")
        elif resolved not in filtered:
            filtered[resolved] = configs[resolved]
    return filtered


def is_client_installed(name: str, config_dir: str, config_file: str, *, project: bool = False) -> bool:
    config_path = os.path.join(config_dir, config_file)
    if not os.path.exists(config_path):
        return False

    is_toml = config_file.endswith(".toml")
    config = _read_config_file(config_path, is_toml=is_toml)
    if config is None:
        return False

    _, special_json_structures = _get_scope_config_spec(project=project)
    mcp_servers = _get_mcp_servers_view(
        config,
        client_name=name,
        is_toml=is_toml,
        special_json_structures=special_json_structures,
    )
    return MCP_SERVER_NAME in mcp_servers


def list_available_clients() -> None:
    data = _load_installer_data()
    configs = data.get_global_configs()
    if not configs:
        print(f"Unsupported platform: {sys.platform}")
        return

    print("Available installation targets:\n")
    print("  MCP Clients:")
    for name, (config_dir, _) in configs.items():
        supports_project = name in data.PROJECT_LEVEL_CONFIGS
        project_marker = " [supports --project]" if supports_project else ""
        status = "found" if os.path.exists(config_dir) else "not found"
        print(f"    {name:<25} ({status}){project_marker}")

    print()
    print("Usage examples:")
    print("  idalib-mcp-headless --install cursor --scope project --ida-home /path/to/IDA")
    print("  idalib-mcp-headless --install claude --transport stdio --ida-home /path/to/IDA")
    print("  idalib-mcp-headless --uninstall cursor --scope project")
    print("  idalib-mcp-headless --config --ida-home /path/to/IDA")


def install_mcp_servers(
    *,
    args,
    transport: str = "streamable-http",
    uninstall: bool = False,
    quiet: bool = False,
    only: list[str] | None = None,
    project: bool = False,
) -> None:
    configs, special_json_structures = _get_scope_config_spec(project=project)
    if not configs:
        print(f"Unsupported platform: {sys.platform}")
        return

    configs = _resolve_client_targets(configs, only)
    if not configs:
        return

    changed = 0
    for name, (config_dir, config_file) in configs.items():
        config_path = os.path.join(config_dir, config_file)
        is_toml = config_file.endswith(".toml")

        if not os.path.exists(config_dir):
            if project and not uninstall:
                os.makedirs(config_dir, exist_ok=True)
            else:
                action = "uninstall" if uninstall else "installation"
                if not quiet:
                    print(f"Skipping {name} {action}\n  Config: {config_path} (not found)")
                continue

        config: dict[str, Any] = {}
        if os.path.exists(config_path):
            loaded = _read_config_file(config_path, is_toml=is_toml)
            if loaded is None:
                if not quiet:
                    kind = "TOML" if is_toml else "JSON"
                    action = "uninstall" if uninstall else "installation"
                    print(f"Skipping {name} {action}\n  Config: {config_path} (invalid {kind})")
                continue
            config = loaded

        mcp_servers = _get_mcp_servers_view(
            config,
            client_name=name,
            is_toml=is_toml,
            special_json_structures=special_json_structures,
        )
        for old_name in OLD_SERVER_NAMES:
            if old_name in mcp_servers:
                mcp_servers[MCP_SERVER_NAME] = mcp_servers.pop(old_name)

        if uninstall:
            if MCP_SERVER_NAME not in mcp_servers:
                if not quiet:
                    print(f"Skipping {name} uninstall\n  Config: {config_path} (not installed)")
                continue
            del mcp_servers[MCP_SERVER_NAME]
        else:
            mcp_servers[MCP_SERVER_NAME] = generate_mcp_config(
                client_name=name,
                transport=transport,
                **_args_kwargs(args),
            )

        _write_config_file(config_path, config, is_toml=is_toml)
        if not quiet:
            action = "Uninstalled" if uninstall else "Installed"
            print(f"{action} {name} MCP server (restart required)\n  Config: {config_path}")
        changed += 1

    if not uninstall and changed == 0:
        print("No MCP servers installed. For unsupported MCP clients, use this config:\n")
        print_mcp_config(args)


def _resolve_transport(value: str | None) -> str:
    if value is None:
        return "streamable-http"
    lowered = value.strip().lower()
    if lowered == "stdio":
        return "stdio"
    if lowered == "sse":
        return "sse"
    if lowered in ("http", "streamable-http", "streamable"):
        return "streamable-http"
    return value


def _get_install_scope(args, *, interactive: bool) -> str | None:
    if args.scope:
        return args.scope
    if not interactive:
        return "project"

    tui = _load_installer_tui()
    if tui is None:
        return None
    choice = tui.interactive_choose(["Project (current directory)", "Global (user-level)"], "Select installation scope:")
    if choice is None:
        return None
    return "project" if choice.startswith("Project") else "global"


def _get_install_transport(args, *, uninstall: bool, interactive: bool) -> str | None:
    if uninstall:
        return "stdio"
    if args.transport is not None:
        return _resolve_transport(args.transport)
    if not interactive:
        return "streamable-http"

    tui = _load_installer_tui()
    if tui is None:
        return None
    choice = tui.interactive_choose(["Streamable HTTP (recommended)", "stdio", "SSE"], "Select transport mode:")
    if choice is None:
        return None
    if choice.startswith("stdio"):
        return "stdio"
    if choice.startswith("Streamable"):
        return "streamable-http"
    return "sse"


def _get_scope_selection_items(*, project: bool) -> list[tuple[str, bool]]:
    configs, _ = _get_scope_config_spec(project=project)
    return [
        (name, is_client_installed(name, config_dir, config_file, project=project))
        for name, (config_dir, config_file) in configs.items()
    ]


def _parse_client_targets(targets_str: str) -> list[str]:
    return [target.strip() for target in targets_str.split(",") if target.strip()]


def _interactive_install(*, uninstall: bool, args) -> None:
    tui = _load_installer_tui()
    if tui is None:
        print("Interactive install UI is unavailable.")
        return

    action = "uninstall" if uninstall else "install"
    transport = _get_install_transport(args, uninstall=uninstall, interactive=True)
    if transport is None:
        print("Cancelled.")
        return

    scope = _get_install_scope(args, interactive=True)
    if scope is None:
        print("Cancelled.")
        return

    items = _get_scope_selection_items(project=(scope == "project"))
    if not items:
        print(f"Unsupported platform: {sys.platform}")
        return

    selected = tui.interactive_select(items, f"Select {scope} targets to {action}:")
    if selected is None:
        print("Cancelled.")
        return

    if selected:
        install_mcp_servers(
            args=args,
            transport=transport,
            uninstall=uninstall,
            only=selected,
            project=(scope == "project"),
        )


def run_install_command(*, uninstall: bool, targets_str: str, args) -> None:
    if targets_str:
        install_mcp_servers(
            args=args,
            transport=_get_install_transport(args, uninstall=uninstall, interactive=False) or "streamable-http",
            uninstall=uninstall,
            only=_parse_client_targets(targets_str),
            project=(_get_install_scope(args, interactive=False) == "project"),
        )
        return

    if sys.stdin.isatty():
        _interactive_install(uninstall=uninstall, args=args)
        return

    action = "installed" if not uninstall else "uninstalled"
    print(
        f"No TTY available, so no MCP clients were {action}. "
        "Pass explicit client targets such as --install cursor,claude."
    )
