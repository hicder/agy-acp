# TODO: Implement HTTP 429 Rate Limit Feedback in ACP

This document outlines the design and implementation tasks to add structured HTTP 429 (Resource Exhaustion/Overloaded) error feedback in the ACP (Agent Client Protocol) adapter.

## Background
When the model API is overloaded, the Google Antigravity CLI (`agy`) records a step in its SQLite database with a 429 error (status `3`, type `17` / tool step type). 
Currently, the adapter ignores this step because the step lacks `Field 4` (tool call info), causing `parse_tool_run` to return `None`, thereby swallowing the 429 error and leaving the client unaware of the rate limit/overload status.

To support client-side handling (such as countdowns, UI banners, or native toasts), we need to extract the error code (`429`) and suggested backoff delay from the database and stream them to the client using the standard ACP `_meta` fields in a non-breaking manner.

---

## Technical Specifications

### 1. Database Parsing (Protobuf extraction in `protobuf.rs`)
In step type `17` (when status is `3`/failed or when the payload contains error fields), parse the raw protobuf `step_payload` to retrieve error details:
- **Top-level Field 24** (sub-message representing execution/error status):
  - **Field 3** (sub-message containing model provider API error details):
    - **Field 1 (string)**: Detailed retryable error description (`"Encountered retryable error..."`).
    - **Field 2 (string)**: Status string (`"RESOURCE_EXHAUSTED (code 429)..."`).
    - **Field 3 (string)**: HTTP status and headers (containing `TraceID` and header metadata).
    - **Field 5 (string)**: Raw JSON error string containing the error code and status.
    - **Field 7 (varint)**: Numeric error code (e.g. `429`).
    - **Field 9 (string)**: User-friendly short error message (`"The model API is currently overloaded..."`).
    - **Field 10 (repeated string)**: Sub-JSON metadata containing `RetryInfo` (e.g. `"retryDelay": "1127.298s"`).

### 2. JSON-RPC Notification Payload (`session/update`)
Since the agent stays `running` (retrying in the background), we will emit the warning as a `sessionUpdate: "agent_thought_chunk"`, injecting programmatic properties in the inner `_meta` field.

#### Format:
```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "<session_id>",
    "update": {
      "sessionUpdate": "agent_thought_chunk",
      "content": {
        "type": "text",
        "text": "\n> [!WARNING]\n> **Model API Rate Limited (HTTP 429)**\n> System is backing off and will retry automatically.\n"
      },
      "_meta": {
        "errorCode": 429,
        "errorStatus": "RESOURCE_EXHAUSTED",
        "backoffSeconds": 1127,
        "traceId": "0x1553e56c5d9c2dd9"
      }
    }
  }
}
```

---

## Action Items

### [ ] Phase 1: Update Protobuf Parsers (`src/protobuf.rs`)
- [ ] Define Rust structs to represent the parsed model API error (e.g., `ModelApiError` with fields `error_code`, `message`, `backoff_seconds`, `trace_id`).
- [ ] Implement a parser function `parse_model_api_error(blob: &[u8]) -> Option<ModelApiError>`:
  - Extract Field 24 -> Field 3.
  - Read Field 7 (varint) as the error code.
  - Read Field 9 (string) as the message.
  - Read Field 3 (string) and parse out `TraceID: 0x...` using regex or string splitting.
  - Read Field 10 (repeated strings), parse the JSON segments, extract `retryDelay`, and convert the seconds string (e.g., `"1127.298s"`) to integer seconds.
- [ ] Update `extract_tool_update_from_step_payload` to check for model API errors if `parse_tool_run` fails or returns `None`. If a model API error is found, map it to a thought chunk update or structured failure update.

### [ ] Phase 2: Update Streaming and Replay (`src/streaming.rs` & `src/db.rs`)
- [ ] Update the polling logic in `src/streaming.rs` to detect when a step contains a model API error and emit the corresponding structured JSON-RPC notification.
- [ ] Update history replay in `src/db.rs` to also replay these rate-limit warnings if needed.

### [ ] Phase 3: Update JSON-RPC Types (`src/types.rs`)
- [ ] Update JSON-RPC types (`JsonRpcNotification` and related payload types) to include the optional `_meta` field.
- [ ] Ensure that `_meta` serializes cleanly to JSON.

### [ ] Phase 4: Testing (`src/tests.rs`)
- [ ] Write unit tests verifying that `parse_model_api_error` successfully extracts `errorCode`, `backoffSeconds`, and `traceId` from a mock protobuf blob.
- [ ] Write integration/unit tests validating that `session/update` notifications carry the correct `_meta` structure.

---

## TODO: 解决磁盘 sessions.json 的无限增长问题

当前实现的内存 `evict_if_needed` 只负责清理内存缓存，没有从磁盘上的 `sessions.json` 中同步删除被驱逐的会话记录。

**改进方向**：
- 在 `StoredSession` 中引入 `last_accessed_at` 时间戳或使用 LRU (Least Recently Used) 算法。
- 在 `persist_session` 写回磁盘前，根据 LRU 算法剔除最旧的会话，将磁盘存储的会话上限限制在 64 个。

