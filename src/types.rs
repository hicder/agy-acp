use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// ACP/agy session config option ids.
pub const CONFIG_ID_MODE: &str = "mode";
pub const CONFIG_ID_MODEL: &str = "model";
pub const CONFIG_ID_EFFORT: &str = "effort";
pub const CONFIG_ID_SANDBOX: &str = "sandbox";
pub const CONFIG_ID_SKIP_PERMISSIONS: &str = "skip_permissions";

pub const DEFAULT_MODE: &str = "default";
pub const DEFAULT_EFFORT: &str = "medium";
pub const MODE_VALUES: &[&str] = &["default", "accept-edits", "plan"];
pub const EFFORT_VALUES: &[&str] = &["low", "medium", "high"];
pub const ON_OFF_VALUES: &[&str] = &["off", "on"];

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
    /// Agent execution mode (`default` | `accept-edits` | `plan`).
    #[serde(default)]
    pub mode: Option<String>,
    /// Reasoning effort (`low` | `medium` | `high`).
    #[serde(default)]
    pub effort: Option<String>,
    /// Whether to pass `--sandbox` to agy.
    #[serde(default)]
    pub sandbox: bool,
    /// Whether to pass `--dangerously-skip-permissions` to agy.
    #[serde(default)]
    pub skip_permissions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub conversation_id: Option<String>,
    /// Last step idx read from SQLite.
    pub last_step_idx: i64,
    /// Selected model ID for this session.
    pub model_id: Option<String>,
    /// Agent execution mode (`default` | `accept-edits` | `plan`).
    pub mode: Option<String>,
    /// Reasoning effort (`low` | `medium` | `high`).
    pub effort: Option<String>,
    /// Whether to pass `--sandbox` to agy.
    pub sandbox: bool,
    /// Whether to pass `--dangerously-skip-permissions` to agy.
    pub skip_permissions: bool,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            conversation_id: None,
            last_step_idx: -1,
            model_id: None,
            mode: Some(DEFAULT_MODE.to_string()),
            effort: Some(DEFAULT_EFFORT.to_string()),
            sandbox: false,
            skip_permissions: false,
        }
    }
}

impl Session {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_stored(stored: &StoredSession) -> Self {
        Self {
            conversation_id: stored.conversation_id.clone(),
            last_step_idx: stored.last_step_idx,
            model_id: stored.model_id.clone(),
            mode: stored.mode.clone().or_else(|| Some(DEFAULT_MODE.to_string())),
            effort: stored
                .effort
                .clone()
                .or_else(|| Some(DEFAULT_EFFORT.to_string())),
            sandbox: stored.sandbox,
            skip_permissions: stored.skip_permissions,
        }
    }

    pub fn to_stored(&self) -> StoredSession {
        StoredSession {
            conversation_id: self.conversation_id.clone(),
            last_step_idx: self.last_step_idx,
            model_id: self.model_id.clone(),
            mode: self.mode.clone(),
            effort: self.effort.clone(),
            sandbox: self.sandbox,
            skip_permissions: self.skip_permissions,
        }
    }

    pub fn mode_or_default(&self) -> &str {
        self.mode.as_deref().unwrap_or(DEFAULT_MODE)
    }

    pub fn effort_or_default(&self) -> &str {
        self.effort.as_deref().unwrap_or(DEFAULT_EFFORT)
    }

    pub fn sandbox_value(&self) -> &'static str {
        if self.sandbox {
            "on"
        } else {
            "off"
        }
    }

    pub fn skip_permissions_value(&self) -> &'static str {
        if self.skip_permissions {
            "on"
        } else {
            "off"
        }
    }
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
    /// OS pid of the spawned `agy` child, used to bind conversation DBs via open FDs.
    pub child_pid: Option<u32>,
}
