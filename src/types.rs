use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub id: Option<Value>,
    pub method: Option<String>,
    pub params: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: &'static str,
    pub method: String,
    pub params: Value,
}

/// Persisted session→conversation mapping stored in ~/.openab/agy-acp/sessions.json
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionStore {
    pub sessions: HashMap<String, StoredSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSession {
    pub conversation_id: Option<String>,
    /// Last step idx read from SQLite; used for delta extraction.
    #[serde(default)]
    pub last_step_idx: i64,
    /// Selected model ID for this session.
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub additional_directories: Vec<String>,
    #[serde(default)]
    pub mode_id: Option<String>,
}

pub struct Session {
    pub conversation_id: Option<String>,
    /// Last step idx read from SQLite.
    pub last_step_idx: i64,
    /// Selected model ID for this session.
    pub model_id: Option<String>,
    pub cwd: Option<String>,
    pub additional_directories: Vec<String>,
    pub mode_id: Option<String>,
}

#[cfg(test)]
pub struct ConversationDelta {
    pub text: Option<String>,
    pub max_step_idx: i64,
}

#[derive(Debug, Default)]
pub struct StreamingState {
    pub conversation_id: Option<String>,
    pub base_step_idx: i64,
    pub last_step_idx: i64,
    pub had_updates: bool,
    pub agent_text_lengths: HashMap<i64, usize>,
    pub thought_text_lengths: HashMap<i64, usize>,
    pub emitted_tool_steps: HashSet<i64>,
    pub last_title: Option<String>,
    pub skip_naration: bool,
    pub child_pid: Option<u32>,
}
