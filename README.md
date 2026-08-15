# agy-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com) stdio adapter for [Google Antigravity CLI](https://github.com/google-antigravity/antigravity-cli) (`agy`). It bridges `agy` into any ACP-compatible host like [Zed](https://zed.dev), enabling you to use Gemini models through `agy` inside Zed's Agent Panel.

## Features

- **Real-Time Streaming**: Directly streams NDJSON events from `agy --output-format stream-json` to deliver fast, incremental text updates.
- **Thinking / Thought Streaming**: Streams model reasoning blocks as ACP thought updates, allowing compatible hosts to render the model's thought process in real time.
- **Rich Tool Execution**: Maps `agy` tool operations (`read`, `edit`, `delete`, `move`, `search`, `execute`, `fetch`, etc.) into structured ACP tool calls with target file paths, line ranges, and formatted outputs (such as directory listings and grep search results).
- **Session Cancellation**: Handles `session/cancel` by cleanly aborting in-flight prompts and terminating the underlying `agy` subprocess.
- **Dynamic Model Selection**: Automatically queries models via `agy models` on startup and exposes them as ACP configuration options. Supports both `session/set_model` and `session/setConfigOption`.
- **Session Persistence & Resume**: Saves conversation mappings to disk with atomic writes and file locking, allowing sessions to resume seamlessly across restarts.
- **Narration Filtering**: Provides a `--skip-naration` CLI flag to filter out leading narrative chatter (e.g., *"I will..."*) before model actions.

## How It Works

`agy-acp` speaks JSON-RPC over stdin/stdout (the ACP transport). When a host sends a prompt, `agy-acp` spawns `agy` in stream-json mode, streams the output incrementally back via `session/update` notifications, and binds the `conversation_id` so subsequent turns or resumed sessions retain context.

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

The binary is generated at `target/release/agy-acp`. Copy it to a directory in your `PATH`:

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
      "args": ["--dangerously-skip-permissions"],
      "env": {}
    }
  }
}
```

> [!WARNING]
> **Tool Execution & Permissions:** Antigravity confirmation prompts cannot be
> answered through ACP hosts. For a trusted local worktree only, pass
> `--dangerously-skip-permissions` to `agy-acp`; it forwards the flag to `agy`
> and bypasses those confirmations. Do not make this the default for untrusted
> repositories.

Then open the Agent Panel in Zed (`Cmd-?` on macOS, `Ctrl-?` on Linux), select **agy** from the agent dropdown, and start chatting.

### Filtering Narration

To suppress leading narrative chatter from the model, pass `--skip-naration` in the arguments:

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": ["--skip-naration"],
      "env": {}
    }
  }
}
```

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

`AGY_EXTRA_ARGS` is retained for compatibility. Prefer the explicit
`agy-acp --dangerously-skip-permissions` and `agy-acp --sandbox` switches for
those supported controls. When an explicit switch is supplied, a duplicate
legacy environment flag is ignored.

## Configuration & Environment

| Setting / Variable | Description |
|---|---|
| `--skip-naration` | CLI flag to filter out leading narrative preamble messages |
| `--dangerously-skip-permissions` | Forward the trusted-worktree permission bypass to `agy` |
| `--sandbox` | Forward the installed `agy` sandbox-mode request |
| `GEMINI_API_KEY` | API key for Gemini (passed through to `agy`) |
| `AGY_EXTRA_ARGS` | Space-separated extra args passed to every `agy` invocation |

## Session Persistence

Sessions are persisted to `~/.openab/agy-acp/sessions.json`. When you resume a session in Zed, `agy-acp` restores the conversation binding and continues it with `agy --conversation <id>`. State persistence uses atomic write-to-temp-and-rename under an exclusive file lock to avoid data corruption.

## Debugging

To inspect the JSON-RPC messages between Zed and `agy-acp`, run `dev: open acp logs` from Zed's Command Palette.

## License

MIT
