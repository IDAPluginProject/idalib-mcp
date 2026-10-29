from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path

from .config import configure_idalib_environment, default_ida_home_from_env


def split_worker_args(argv: list[str]) -> tuple[Path | None, Path | None, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ida-home", type=Path, default=default_ida_home_from_env())
    parser.add_argument("--ida-config-root", type=Path, default=None, help=argparse.SUPPRESS)
    args, remaining = parser.parse_known_args(argv)
    return args.ida_home, args.ida_config_root, remaining


def main() -> None:
    # Consume --supervisor-pid (added by the managed supervisor) before any
    # other parsing so it is not forwarded to the upstream idalib_server parser.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--supervisor-pid", type=int, default=None)
    pre_args, argv = pre.parse_known_args(sys.argv[1:])

    ida_home, config_root, remaining = split_worker_args(argv)

    try:
        configure_idalib_environment(ida_home, config_root=config_root)
    except Exception as exc:
        raise SystemExit(f"Failed to configure IDA home for idalib: {exc}") from exc

    sys.argv = [sys.argv[0], *remaining]

    try:
        upstream_server = importlib.import_module("ida_pro_mcp.idalib_server")
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'ida-pro-mcp'. Install this project with its "
            "dependencies, then retry."
        ) from exc

    # Register idalib-mcp extension tools. Importing this module triggers
    # the upstream @tool decorator, which inserts the tools into
    # ida_pro_mcp.ida_mcp.MCP_SERVER before idalib_server.main() starts
    # serving them.
    from . import api_sigworker  # noqa: F401

    # Remove upstream sigmaker tools superseded by the advanced variants.
    from ida_pro_mcp.ida_mcp.rpc import MCP_SERVER
    for _name in (
        "make_signature",
        "make_signature_for_function",
        "make_signature_for_range",
        "find_xref_signatures",
    ):
        MCP_SERVER.tools.methods.pop(_name, None)

    os.environ.setdefault("IDA_MCP_WORKER", "1")

    # Shut down cleanly if the supervisor goes away (e.g. is hard-killed),
    # instead of lingering as an orphan that holds an IDA license seat. The
    # upstream main() registers the SIGTERM handler that this watchdog triggers.
    if pre_args.supervisor_pid is not None:
        from .parentwatch import start_parent_death_watch

        start_parent_death_watch(pre_args.supervisor_pid)

    upstream_server.main()


if __name__ == "__main__":
    main()
