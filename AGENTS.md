# agy-acp

Single Rust crate. ACP (Agent Client Protocol) stdio adapter for Google Antigravity CLI (`agy`). Bridges `agy` into OpenAB's JSON-RPC protocol.

## Commands

```bash
cargo build                    # debug build
cargo build --release          # release build (required for e2e tests)
cargo test                     # unit tests only (fast, no I/O)
cargo test -- --include-ignored  # all tests including filesystem I/O tests
cargo test e2e -- --ignored --nocapture  # e2e only (needs agy binary + auth)
```

No separate lint/typecheck/format commands — just `cargo build` and `cargo test`.

## Architecture

- `main.rs` — stdin/stdout JSON-RPC loop. Reads lines, dispatches to adapter methods, writes responses.
- `adapter.rs` — core logic: session lifecycle, spawning `agy` subprocess, state persistence. `Adapter::new()` reads `HOME` for the state dir.
- `streaming.rs` — parses `agy --output-format stream-json` NDJSON (`init`, `step_update`, `result`) into ACP `session/update` notifications.
- `tools.rs` — maps agy tool names/parameters/output into ACP tool-call fields (`kind`, locations, content).
- `types.rs` — JSON-RPC types, `SessionStore` for persistence.

## Key paths

| Path | Purpose |
|---|---|
| `~/.openab/agy-acp/sessions.json` | Persisted session→conversation mapping (with `.lock` file for mutual exclusion) |

## Test tiers

1. **Unit tests** (`cargo test`) — stream-json parsing, narration filtering, JSON-RPC response shape. No filesystem or network I/O.
2. **Ignored I/O tests** (`-- --include-ignored`) — session persist/restore. Create temp dirs in `$TMPDIR`.
3. **E2E tests** (`e2e -- --ignored`) — spawn the release binary, send JSON-RPC over stdin, verify responses. Requires:
   - `agy` in `PATH` (install from `google-antigravity/antigravity-cli` releases)
   - Auth via `GEMINI_API_KEY` env var or macOS Keychain (`~/.gemini/antigravity-cli/settings.json`)
   - `cargo build --release` must have been run first

## Environment variables

| Var | Effect |
|---|---|
| `AGY_EXTRA_ARGS` | Space-separated extra args passed to every `agy` invocation |
| `GEMINI_API_KEY` | API key for e2e tests and CI |

## Quirks

- State persistence uses write-to-tmp-then-rename pattern under an exclusive file lock (`fs2`).
- Streaming writes JSON-RPC notifications directly to stdout from the `agy` stdout reader (not through the main channel). The main loop may still write concurrently if other requests arrive during a prompt.
- `handle_session_load` returns a `Vec<String>` (same shape as other multi-line handlers). History is not replayed; load restores the conversation binding so later prompts pass `--conversation`.
- Conversation binding: the `init` / `result` stream-json events include `conversation_id`, which is persisted and passed back as `--conversation` on subsequent prompts.
- `fetch_available_models()` runs `agy models` synchronously during `Adapter::new()`. If `agy` isn't installed, models list is empty (no error).
- `session/cancel` returns `{}` immediately but sets a flag that kills the in-flight `agy` subprocess.
- Both `session/set_model` and `session/setConfigOption` are accepted for model selection.
