# agy-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com) stdio adapter for [Google Antigravity CLI](https://github.com/google-antigravity/antigravity-cli) (`agy`). It bridges `agy` into any ACP-compatible host like [Zed](https://zed.dev), enabling you to use Gemini models through `agy` inside Zed's Agent Panel.

## How It Works

`agy-acp` speaks JSON-RPC over stdin/stdout (the ACP transport). When a host like Zed sends a prompt, `agy-acp` spawns `agy` as a subprocess, streams the response back as incremental `session/update` notifications, and persists session state across restarts so you can resume conversations.

```
Zed (ACP host)  <--stdin/stdout JSON-RPC-->  agy-acp  <--subprocess-->  agy  <--API-->  Gemini
```

## Prerequisites

- **Rust** (1.70+) with Cargo
- **`agy`** installed and in your `PATH` — install from [google-antigravity/antigravity-cli releases](https://github.com/google-antigravity/antigravity-cli)
- **Authentication** — either set `GEMINI_API_KEY` or configure auth via `~/.gemini/antigravity-cli/settings.json`

## Build & Install

```bash
cargo build --release
```

The binary is at `target/release/agy-acp`. Copy it somewhere in your `PATH`:

```bash
cp target/release/agy-acp /usr/local/bin/
```

## Use with Zed

Add `agy-acp` as a custom agent server in your Zed settings (`~/.config/zed/settings.json`):

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": [],
      "env": {}
    }
  }
}
```

Then open the Agent Panel in Zed (`Cmd-?` on macOS, `Ctrl-?` on Linux), select **agy** from the agent dropdown, and start chatting.

### Model Selection

`agy-acp` queries available models by running `agy models` at startup. You can switch models from Zed's model selector in the agent thread — the adapter exposes them as ACP config options.

### Passing Extra Arguments

Set the `AGY_EXTRA_ARGS` environment variable to pass additional arguments to every `agy` invocation:

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": [],
      "env": {
        "AGY_EXTRA_ARGS": "--some-flag value"
      }
    }
  }
}
```

## Environment Variables

| Variable | Description |
|---|---|
| `GEMINI_API_KEY` | API key for Gemini (passed through to `agy`) |
| `AGY_EXTRA_ARGS` | Space-separated extra args passed to every `agy` invocation |

## Session Persistence

Sessions are persisted to `~/.openab/agy-acp/sessions.json`. When you resume a session in Zed, `agy-acp` restores the conversation binding and continues it with `agy --conversation <id>`. Live responses are streamed from `agy --output-format stream-json` as incremental `session/update` notifications.

## Debugging

To inspect the JSON-RPC messages between Zed and `agy-acp`, run `dev: open acp logs` from Zed's Command Palette.

## License

MIT
