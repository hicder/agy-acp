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
- `adapter.rs` — core logic: session lifecycle, spawning `agy` subprocess, state persistence. `Adapter::new()` reads `HOME` for state/conv dirs.
- `db.rs` — reads agy's SQLite conversation DBs (read-only). Table: `steps` with columns `idx`, `step_type`, `step_payload`.
- `protobuf.rs` — hand-rolled protobuf varint/field extraction (no prost/protobuf dependency). Extracts text from `step_payload` field 20 → sub-field 1.
- `streaming.rs` — polls SQLite every 500ms during `session/prompt`, emits incremental `session/update` notifications to stdout.
- `types.rs` — JSON-RPC types, `SessionStore` for persistence, `StreamingState`.

## Key paths

| Path | Purpose |
|---|---|
| `~/.openab/agy-acp/sessions.json` | Persisted session→conversation mapping (with `.lock` file for mutual exclusion) |
| `~/.gemini/antigravity-cli/conversations/*.db` | agy's SQLite conversation databases |

## Test tiers

1. **Unit tests** (`cargo test`) — protobuf parsing, narration filtering, JSON-RPC response shape. No filesystem or network I/O.
2. **Ignored I/O tests** (`-- --include-ignored`) — session persist/restore, SQLite read, conversation snapshot. Create temp dirs in `$TMPDIR`.
3. **E2E tests** (`e2e -- --ignored`) — spawn the release binary, send JSON-RPC over stdin, verify responses. Requires:
   - `agy` in `PATH` (install from `google-antigravity/antigravity-cli` releases)
   - Auth via `agy` OAuth sign-in (`~/.gemini/antigravity-cli/settings.json` must exist)
   - `cargo build --release` must have been run first

## Environment variables

| Var | Effect |
|---|---|
| `AGY_EXTRA_ARGS` | Space-separated extra args passed to every `agy` invocation |

## Quirks

- `rusqlite` uses `bundled` feature — no system SQLite dependency needed.
- SQLite reads use `SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX` — single-threaded access assumed per conversation DB.
- State persistence uses write-to-tmp-then-rename pattern under an exclusive file lock (`fs2`).
- Streaming writes JSON-RPC notifications directly to stdout from a background polling thread (not through the main channel). Both the main loop and the poller write to stdout concurrently.
- `handle_session_load` returns a `Vec<String>` (multiple notifications + final response), not a single response like other methods.
- Conversation binding: on first prompt for a new session, the adapter snapshots conversation DB filenames, then diffs after `agy` exits to discover the new conversation ID. Refuses to bind if multiple new DBs appear simultaneously.
- `fetch_available_models()` runs `agy models` synchronously during `Adapter::new()`. If `agy` isn't installed, models list is empty (no error).
- `session/cancel` is a no-op — always returns `{}`.
- Both `session/set_model` and `session/setConfigOption` are accepted for model selection.
