from __future__ import annotations

import unittest

from idalib_mcp.installer import (
    MCP_SERVER_NAME,
    _parse_client_targets,
    generate_mcp_config,
    normalize_transport_url,
)


class InstallerTests(unittest.TestCase):
    def test_stdio_config_includes_ida_home_and_runtime_flags(self) -> None:
        config = generate_mcp_config(
            client_name="Generic",
            transport="stdio",
            ida_home="D:/tools/IDA_PRO_9.1",
            unsafe=True,
            isolated_contexts=True,
            max_workers=8,
            sync_wait=2.5,
        )

        self.assertIn("command", config)
        args = config["args"]
        self.assertIn("-m", args)
        self.assertIn("idalib_mcp", args)
        self.assertIn("--stdio", args)
        self.assertIn("--ida-home", args)
        self.assertIn("D:/tools/IDA_PRO_9.1", args)
        self.assertIn("--unsafe", args)
        self.assertIn("--isolated-contexts", args)
        self.assertIn("--max-workers", args)
        self.assertIn("8", args)
        self.assertEqual(args[args.index("--sync-wait") + 1], "2.5")

    def test_stdio_config_omits_sync_wait_when_not_given(self) -> None:
        config = generate_mcp_config(client_name="Generic", transport="stdio")

        self.assertNotIn("--sync-wait", config["args"])

    def test_http_config_for_claude_uses_streamable_http_shape(self) -> None:
        config = generate_mcp_config(
            client_name="Claude",
            transport="streamable-http",
            host="127.0.0.1",
            port=8745,
        )

        self.assertEqual(config, {"type": "http", "url": "http://127.0.0.1:8745/mcp"})

    def test_normalize_transport_url_defaults_to_mcp_path(self) -> None:
        self.assertEqual(normalize_transport_url("http://127.0.0.1:8745"), "http://127.0.0.1:8745/mcp")

    def test_parse_client_targets(self) -> None:
        self.assertEqual(_parse_client_targets(" cursor, claude ,, vscode "), ["cursor", "claude", "vscode"])

    def test_server_name_is_stable(self) -> None:
        self.assertEqual(MCP_SERVER_NAME, "idalib-mcp-headless")


if __name__ == "__main__":
    unittest.main()
