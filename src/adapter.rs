use fs2::FileExt;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use uuid::Uuid;

#[cfg(test)]
use crate::db::read_delta_from_db;
use crate::db::read_replay_updates_from_db;
use crate::log_scan;
use crate::streaming::poll_streaming_delta;
use crate::types::*;

pub struct Adapter {
    pub sessions: HashMap<String, Session>,
    pub working_dir: String,
    pub conversations_dir: PathBuf,
    pub state_file: PathBuf,
    pub available_models: Vec<String>,
    pub skip_naration: bool,
}

impl Adapter {
    pub fn new() -> Self {
        Self::new_with_skip_naration(false)
    }

    pub fn new_with_skip_naration(skip_naration: bool) -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let state_dir = PathBuf::from(&home).join(".openab/agy-acp");
        let mut adapter = Self {
            sessions: HashMap::new(),
            working_dir: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/tmp".to_string()),
            conversations_dir: PathBuf::from(&home).join(".gemini/antigravity-cli/conversations"),
            state_file: state_dir.join("sessions.json"),
            available_models: Vec::new(),
            skip_naration,
        };
        let models = adapter.fetch_available_models();
        if !models.is_empty() {
            eprintln!(
                "[agy-acp] fetched {} models from `agy models`, updating cache",
                models.len()
            );
            adapter.save_models_cache(&models);
            adapter.available_models = models;
        } else if let Some(cached) = adapter.load_cached_models() {
            eprintln!(
                "[agy-acp] `agy models` failed, using cached model list ({} models)",
                cached.len()
            );
            adapter.available_models = cached;
        } else {
            eprintln!("[agy-acp] `agy models` failed and no cache found, using hardcoded fallback");
            adapter.available_models = Self::static_fallback_models();
        }
        adapter
    }

    // --- Model cache ---

    pub fn models_cache_path(&self) -> PathBuf {
        self.state_file.with_file_name("models_cache.json")
    }

    pub fn load_cached_models(&self) -> Option<Vec<String>> {
        let path = self.models_cache_path();
        let content = fs::read_to_string(&path).ok()?;
        serde_json::from_str::<Vec<String>>(&content)
            .ok()
            .filter(|v| !v.is_empty())
    }

    pub fn save_models_cache(&self, models: &[String]) {
        if let Some(parent) = self.models_cache_path().parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(json) = serde_json::to_string(models) {
            let tmp = self.models_cache_path().with_extension("tmp");
            if fs::write(&tmp, &json).is_ok() {
                let _ = fs::rename(&tmp, self.models_cache_path());
            }
        }
    }

    pub fn static_fallback_models() -> Vec<String> {
        vec![
            "Gemini 3.5 Flash (Medium)".to_string(),
            "Gemini 3.5 Flash (High)".to_string(),
            "Gemini 3.5 Flash (Low)".to_string(),
            "Gemini 3.1 Pro (Low)".to_string(),
            "Gemini 3.1 Pro (High)".to_string(),
        ]
    }

    /// Resolve the `agy` binary to use.
    ///
    /// Resolution order:
    /// 1. `AGY_BIN_PATH` environment variable (full path to the binary).
    /// 2. `AGY_INSTALL_PATH` environment variable (directory containing the binary).
    /// 3. `agy` in the caller's PATH.
    fn agy_bin() -> String {
        if let Ok(path) = std::env::var("AGY_BIN_PATH") {
            if !path.is_empty() {
                return path;
            }
        }
        if let Ok(dir) = std::env::var("AGY_INSTALL_PATH") {
            if !dir.is_empty() {
                return std::path::PathBuf::from(dir)
                    .join("agy")
                    .to_string_lossy()
                    .to_string();
            }
        }
        "agy".to_string()
    }

    /// Run `agy models` and parse the output into a list of model names.
    fn fetch_available_models(&self) -> Vec<String> {
        std::process::Command::new(Self::agy_bin())
            .arg("models")
            .stderr(Stdio::null())
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_available_models(&mut self) -> &[String] {
        if self.available_models.is_empty() {
            let models = self.fetch_available_models();
            if !models.is_empty() {
                eprintln!(
                    "[agy-acp] fetched {} models from `agy models`, updating cache",
                    models.len()
                );
                self.save_models_cache(&models);
                self.available_models = models;
            } else if let Some(cached) = self.load_cached_models() {
                eprintln!(
                    "[agy-acp] `agy models` failed, using cached model list ({} models)",
                    cached.len()
                );
                self.available_models = cached;
            } else {
                eprintln!(
                    "[agy-acp] `agy models` failed and no cache found, using hardcoded fallback"
                );
                self.available_models = Self::static_fallback_models();
            }
        }
        &self.available_models
    }

    /// Build the ACP `models` JSON for a session, given its current model_id.
    pub fn session_models_json(&mut self, model_id: Option<&str>) -> Value {
        let models = self.get_available_models();
        let current = model_id
            .or_else(|| models.first().map(|s| s.as_str()))
            .unwrap_or("");
        let available: Vec<Value> = models
            .iter()
            .map(|name| {
                json!({
                    "modelId": name,
                    "name": name,
                })
            })
            .collect();
        json!({
            "currentModelId": current,
            "availableModels": available,
        })
    }

    fn select_option(value: &str, name: &str, description: &str) -> Value {
        json!({
            "value": value,
            "name": name,
            "description": description,
        })
    }

    fn on_off_options() -> Vec<Value> {
        vec![
            Self::select_option("off", "Off", "Disabled"),
            Self::select_option("on", "On", "Enabled"),
        ]
    }

    fn parse_on_off(value: &str) -> Option<bool> {
        match value {
            "on" | "true" | "1" => Some(true),
            "off" | "false" | "0" => Some(false),
            _ => None,
        }
    }

    /// Snapshot of config-relevant fields for a session (defaults if missing).
    fn session_config_snapshot(&self, session_id: &str) -> Session {
        self.sessions
            .get(session_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Build the full ACP configOptions list for a session.
    ///
    /// Order is intentional (ACP priority): mode, model, effort, sandbox, skip_permissions.
    pub fn session_config_options_json(&mut self, session: &Session) -> Value {
        let models = self.get_available_models();
        let current_model = session
            .model_id
            .as_deref()
            .or_else(|| models.first().map(|s| s.as_str()))
            .unwrap_or("");
        let model_options: Vec<Value> = models
            .iter()
            .map(|name| {
                json!({
                    "value": name,
                    "name": name,
                })
            })
            .collect();

        json!([
            {
                "id": CONFIG_ID_MODE,
                "name": "Mode",
                "description": "Controls how the agent applies edits and requests review",
                "category": "mode",
                "type": "select",
                "currentValue": session.mode_or_default(),
                "options": [
                    Self::select_option(
                        "default",
                        "Default",
                        "Request review before applying file writes"
                    ),
                    Self::select_option(
                        "accept-edits",
                        "Accept Edits",
                        "Apply file edits automatically"
                    ),
                    Self::select_option(
                        "plan",
                        "Plan",
                        "Plan without applying edits"
                    ),
                ],
            },
            {
                "id": CONFIG_ID_MODEL,
                "name": "Model",
                "description": "Model used for this session",
                "category": "model",
                "type": "select",
                "currentValue": current_model,
                "options": model_options,
            },
            {
                "id": CONFIG_ID_EFFORT,
                "name": "Effort",
                "description": "Reasoning effort / thinking level",
                "category": "thought_level",
                "type": "select",
                "currentValue": session.effort_or_default(),
                "options": [
                    Self::select_option("low", "Low", "Faster, less deliberation"),
                    Self::select_option("medium", "Medium", "Balanced reasoning effort"),
                    Self::select_option("high", "High", "Deeper reasoning"),
                ],
            },
            {
                "id": CONFIG_ID_SANDBOX,
                "name": "Sandbox",
                "description": "Run tool commands in the OS sandbox",
                "category": "_safety",
                "type": "select",
                "currentValue": session.sandbox_value(),
                "options": Self::on_off_options(),
            },
            {
                "id": CONFIG_ID_SKIP_PERMISSIONS,
                "name": "Skip Permissions",
                "description": "Auto-approve all tool permission requests (dangerous)",
                "category": "_safety",
                "type": "select",
                "currentValue": session.skip_permissions_value(),
                "options": Self::on_off_options(),
            },
        ])
    }

    pub fn session_config_result_json(&mut self, session_id: &str) -> Value {
        let session = self.session_config_snapshot(session_id);
        let model_id = session.model_id.clone();
        json!({
            "sessionId": session_id,
            "models": self.session_models_json(model_id.as_deref()),
            // Dual-publish legacy modes for clients that do not yet use configOptions.
            "modes": {
                "currentModeId": session.mode_or_default(),
                "availableModes": [
                    { "id": "default", "name": "Default", "description": "Request review before applying file writes" },
                    { "id": "accept-edits", "name": "Accept Edits", "description": "Apply file edits automatically" },
                    { "id": "plan", "name": "Plan", "description": "Plan without applying edits" },
                ],
            },
            "configOptions": self.session_config_options_json(&session),
        })
    }

    /// Persist the full in-memory session binding.
    pub fn persist_session_state(&self, session_id: &str, session: &Session) {
        let Some(_lock) = self.lock_state_file() else {
            return;
        };
        let mut store = self.load_store_inner();
        store
            .sessions
            .insert(session_id.to_string(), session.to_stored());
        let tmp = self.state_file.with_extension("tmp");
        if let Ok(file) = fs::File::create(&tmp) {
            if serde_json::to_writer_pretty(&file, &store).is_ok() {
                let _ = fs::rename(&tmp, &self.state_file);
            }
        }
    }

    /// Acquire exclusive lock on a dedicated lock file for read-write mutual exclusion.
    fn lock_state_file(&self) -> Option<fs::File> {
        if let Some(parent) = self.state_file.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let lock_path = self.state_file.with_extension("lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .ok()?;
        lock_file.lock_exclusive().ok()?;
        Some(lock_file)
    }

    /// Load persisted session store (caller must hold lock).
    fn load_store_inner(&self) -> SessionStore {
        let Some(file) = fs::File::open(&self.state_file).ok() else {
            return SessionStore::default();
        };
        serde_json::from_reader(&file).unwrap_or_default()
    }

    /// Load persisted session store with lock.
    pub fn load_store(&self) -> SessionStore {
        let _lock = self.lock_state_file();
        self.load_store_inner()
    }

    /// Try to restore a full session from persisted state.
    ///
    /// Returns `None` when the session id is unknown. Sessions may exist with
    /// config only (no conversation yet) after `set_config_option`.
    pub fn restore_session(&self, session_id: &str) -> Option<Session> {
        let store = self.load_store();
        store.sessions.get(session_id).map(Session::from_stored)
    }

    /// Persist a session binding (read-modify-write under single lock).
    ///
    /// Preserves existing config fields (mode/effort/sandbox/skip_permissions) when
    /// the session is already stored and only conversation/model indices change.
    pub fn persist_session(
        &self,
        session_id: &str,
        conversation_id: Option<&str>,
        last_step_idx: i64,
        model_id: Option<&str>,
    ) {
        let existing = self
            .sessions
            .get(session_id)
            .cloned()
            .or_else(|| {
                let store = self.load_store();
                store.sessions.get(session_id).map(Session::from_stored)
            })
            .unwrap_or_default();
        let mut session = existing;
        session.conversation_id = conversation_id.map(String::from);
        session.last_step_idx = last_step_idx;
        session.model_id = model_id.map(String::from);
        self.persist_session_state(session_id, &session);
    }

    /// Scans the conversations directory for SQLite database files (`*.db`)
    /// and returns their file stems as a set of conversation IDs.
    pub fn conversation_snapshot(&self) -> HashSet<String> {
        let Ok(entries) = fs::read_dir(&self.conversations_dir) else {
            return HashSet::new();
        };
        entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let path = e.path();
                if path.extension().map(|x| x == "db").unwrap_or(false) {
                    path.file_stem().map(|s| s.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    #[cfg(test)]
    pub fn new_conversation_id(&self, before: &HashSet<String>) -> Option<String> {
        let after = self.conversation_snapshot();
        let mut created: Vec<_> = after.difference(before).collect();
        if created.is_empty() {
            return None;
        }
        if created.len() > 1 {
            eprintln!(
                "[agy-acp] WARN: multiple new agy conversation files appeared; \
                 refusing to bind"
            );
            return None;
        }
        Some(created.remove(0).clone())
    }

    pub fn read_replay_updates_from_db_inner(
        &self,
        conversation_id: &str,
    ) -> Option<(Vec<Value>, i64)> {
        read_replay_updates_from_db(&self.conversations_dir, conversation_id, self.skip_naration)
    }

    #[cfg(test)]
    fn read_delta_from_db_inner(
        &self,
        conversation_id: &str,
        after_step_idx: i64,
    ) -> Option<crate::types::ConversationDelta> {
        read_delta_from_db(&self.conversations_dir, conversation_id, after_step_idx)
    }

    #[cfg(test)]
    pub fn read_response_from_db(
        &self,
        conversation_id: &str,
        after_step_idx: i64,
    ) -> Option<(String, i64)> {
        self.read_delta_from_db_inner(conversation_id, after_step_idx)
            .and_then(|delta| delta.text.map(|text| (text, delta.max_step_idx)))
    }

    /// Filter out leading narration ("I will ...", "I'll ...") from response parts.
    #[cfg(test)]
    pub fn filter_narration(parts: &[String]) -> Option<String> {
        filter_narration(parts)
    }

    /// A part is considered narration if every non-empty line starts with "I will" or "I'll".
    #[cfg(test)]
    pub fn is_narration(text: &str) -> bool {
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            return false;
        }
        lines.iter().all(|l| {
            let line = l.trim_start();
            line.starts_with("I will") || line.starts_with("I'll") || line.starts_with("I’ll")
        })
    }

    fn evict_if_needed(&mut self) {
        const MAX_SESSIONS: usize = 64;
        while self.sessions.len() >= MAX_SESSIONS {
            if let Some(key) = self.sessions.keys().next().cloned() {
                self.sessions.remove(&key);
            }
        }
    }

    pub fn restore_session_state(&mut self, session_id: &str) -> bool {
        let Some(session) = self.restore_session(session_id) else {
            return false;
        };
        if !self.sessions.contains_key(session_id) {
            self.evict_if_needed();
        }
        self.sessions.insert(session_id.to_string(), session);
        true
    }

    pub fn handle_initialize(&self, id: Value) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({
                "protocolVersion": 1,
                "agentInfo": { "name": "agy", "version": env!("CARGO_PKG_VERSION") },
                "agentCapabilities": {
                    "loadSession": true,
                    "streaming": true,
                    "promptCapabilities": {
                        "text": true,
                    },
                    "sessionCapabilities": {
                        "resume": true,
                        "list": true,
                        "delete": true,
                    },
                },
                "authMethods": [],
            })),
            error: None,
        }
    }

    pub fn handle_session_new(&mut self, id: Value) -> JsonRpcResponse {
        let session_id = Uuid::new_v4().to_string();
        self.evict_if_needed();
        self.sessions.insert(session_id.clone(), Session::new());
        let result = self.session_config_result_json(&session_id);
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn handle_session_load(&mut self, id: Value, params: &Value) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if session_id.is_empty() {
            return vec![serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            })
            .unwrap()];
        }

        if !self.sessions.contains_key(session_id) && !self.restore_session_state(session_id) {
            return vec![serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            })
            .unwrap()];
        }

        let mut output_lines: Vec<String> = Vec::new();

        let replay_conv_id = self
            .sessions
            .get(session_id)
            .and_then(|session| session.conversation_id.clone());
        if let Some(conv_id) = replay_conv_id {
            if let Some((updates, max_step_idx)) = self.read_replay_updates_from_db_inner(&conv_id)
            {
                for update in updates {
                    let notification = serde_json::to_string(&JsonRpcNotification {
                        jsonrpc: "2.0",
                        method: "session/update".to_string(),
                        params: json!({
                            "sessionId": session_id,
                            "update": update,
                        }),
                    })
                    .unwrap();
                    output_lines.push(notification);
                }
                if let Some(session) = self.sessions.get_mut(session_id) {
                    session.last_step_idx = max_step_idx;
                }
                if let Some(session) = self.sessions.get(session_id).cloned() {
                    self.persist_session_state(session_id, &session);
                }
            }
        }

        output_lines.push({
            let result = self.session_config_result_json(session_id);
            serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            })
            .unwrap()
        });

        output_lines
    }

    pub fn handle_session_resume(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if session_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            };
        }

        if self.sessions.contains_key(session_id) || self.restore_session_state(session_id) {
            let result = self.session_config_result_json(session_id);
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            };
        }

        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(json!({
                "code": -32000,
                "message": format!("unknown sessionId: {session_id}"),
            })),
        }
    }

    pub fn handle_session_list(&self, id: Value) -> JsonRpcResponse {
        let store = self.load_store();
        let sessions: Vec<Value> = store
            .sessions
            .iter()
            .map(|(session_id, stored)| {
                json!({
                    "sessionId": session_id,
                    "conversationId": stored.conversation_id,
                    "modelId": stored.model_id,
                    "mode": stored.mode.clone().unwrap_or_else(|| DEFAULT_MODE.to_string()),
                    "effort": stored.effort.clone().unwrap_or_else(|| DEFAULT_EFFORT.to_string()),
                    "sandbox": stored.sandbox,
                    "skipPermissions": stored.skip_permissions,
                    "lastStepIdx": stored.last_step_idx,
                })
            })
            .collect();
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "sessions": sessions })),
            error: None,
        }
    }

    pub fn handle_session_delete(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if session_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            };
        }
        self.sessions.remove(session_id);
        if let Some(_lock) = self.lock_state_file() {
            let mut store = self.load_store_inner();
            store.sessions.remove(session_id);
            let tmp = self.state_file.with_extension("tmp");
            if let Ok(file) = fs::File::create(&tmp) {
                if serde_json::to_writer_pretty(&file, &store).is_ok() {
                    let _ = fs::rename(&tmp, &self.state_file);
                }
            }
        }
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({})),
            error: None,
        }
    }

    pub fn handle_session_set_model(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let model_id = params.get("modelId").and_then(|v| v.as_str()).unwrap_or("");

        if session_id.is_empty() || model_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId or modelId"})),
            };
        }

        if !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        let Some(session) = self.sessions.get_mut(session_id) else {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            };
        };

        session.model_id = Some(model_id.to_string());
        let snapshot = session.clone();
        self.persist_session_state(session_id, &snapshot);

        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({})),
            error: None,
        }
    }

    pub fn handle_session_set_config_option(
        &mut self,
        id: Value,
        params: &Value,
    ) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let config_id = params
            .get("configId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        // Prefer string values; accept boolean for on/off options as a convenience.
        let value = params
            .get("value")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_bool().map(|b| if b { "on" } else { "off" }.to_string()))
            })
            .unwrap_or_default();

        if session_id.is_empty() || config_id.is_empty() || value.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(
                    json!({"code":-32602,"message":"missing sessionId, configId, or value"}),
                ),
            };
        }

        if !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        if !self.sessions.contains_key(session_id) {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            };
        }

        let apply_error = {
            let session = self.sessions.get_mut(session_id).unwrap();
            match config_id {
                CONFIG_ID_MODEL => {
                    session.model_id = Some(value.clone());
                    None
                }
                CONFIG_ID_MODE => {
                    if !MODE_VALUES.contains(&value.as_str()) {
                        Some(format!(
                            "invalid mode `{value}` (valid: {})",
                            MODE_VALUES.join(", "),
                        ))
                    } else {
                        session.mode = Some(value.clone());
                        None
                    }
                }
                CONFIG_ID_EFFORT => {
                    if !EFFORT_VALUES.contains(&value.as_str()) {
                        Some(format!(
                            "invalid effort `{value}` (valid: {})",
                            EFFORT_VALUES.join(", "),
                        ))
                    } else {
                        session.effort = Some(value.clone());
                        None
                    }
                }
                CONFIG_ID_SANDBOX => match Self::parse_on_off(&value) {
                    Some(on) => {
                        session.sandbox = on;
                        None
                    }
                    None => Some(format!(
                        "invalid sandbox `{value}` (valid: {})",
                        ON_OFF_VALUES.join(", "),
                    )),
                },
                CONFIG_ID_SKIP_PERMISSIONS => match Self::parse_on_off(&value) {
                    Some(on) => {
                        session.skip_permissions = on;
                        None
                    }
                    None => Some(format!(
                        "invalid skip_permissions `{value}` (valid: {})",
                        ON_OFF_VALUES.join(", "),
                    )),
                },
                other => Some(format!("unknown configId: {other}")),
            }
        };

        if let Some(message) = apply_error {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code": -32602, "message": message})),
            };
        }

        let snapshot = self.sessions.get(session_id).cloned().unwrap();
        self.persist_session_state(session_id, &snapshot);

        let config_options = self.session_config_options_json(&snapshot);
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "configOptions": config_options })),
            error: None,
        }
    }

    pub async fn handle_session_prompt(
        &mut self,
        id: Value,
        params: &Value,
        cancelled: Arc<AtomicBool>,
    ) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !session_id.is_empty() && !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        let prompt_text = params
            .get("prompt")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        let clean_prompt = prompt_text.trim();

        let snapshot = if self
            .sessions
            .get(session_id)
            .map(|s| s.conversation_id.is_none())
            .unwrap_or(false)
        {
            Some(self.conversation_snapshot())
        } else {
            None
        };

        let log_pre_snapshot = log_scan::snapshot_agy_logs(&self.conversations_dir);
        let spawn_time = std::time::SystemTime::now();

        let mut args: Vec<String> = Vec::new();
        args.push("--add-dir".to_string());
        args.push(self.working_dir.clone());
        if let Some(dirs) = params
            .get("additionalDirectories")
            .and_then(|v| v.as_array())
        {
            for dir in dirs {
                if let Some(dir_str) = dir.as_str() {
                    args.push("--add-dir".to_string());
                    args.push(dir_str.to_string());
                }
            }
        }
        if let Ok(extra) = std::env::var("AGY_EXTRA_ARGS") {
            if let Ok(parsed) = shell_words::split(&extra) {
                args.extend(parsed);
            } else {
                eprintln!("[agy-acp] WARN: failed to parse AGY_EXTRA_ARGS, ignoring");
            }
        }
        if let Some(session) = self.sessions.get(session_id) {
            if let Some(conv_id) = &session.conversation_id {
                args.push("--conversation".to_string());
                args.push(conv_id.clone());
            }
            if let Some(model_id) = &session.model_id {
                args.push("--model".to_string());
                args.push(model_id.clone());
            }
            let mode = session.mode_or_default();
            if mode != DEFAULT_MODE {
                args.push("--mode".to_string());
                args.push(mode.to_string());
            }
            args.push("--effort".to_string());
            args.push(session.effort_or_default().to_string());
            if session.sandbox {
                args.push("--sandbox".to_string());
            }
            if session.skip_permissions {
                args.push("--dangerously-skip-permissions".to_string());
            }
        }
        args.push("-p".to_string());
        args.push(clean_prompt.to_string());

        let spawn_result = Command::new(Self::agy_bin())
            .args(&args)
            .current_dir(&self.working_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                return vec![serde_json::to_string(&JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(json!({"code":-32000,"message":format!("failed to run agy: {e}")})),
                })
                .unwrap()];
            }
        };

        let mut stdout = child.stdout.take();
        let stdout_reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut stdout) = stdout.take() {
                let _ = stdout.read_to_end(&mut buf).await;
            }
            buf
        });

        let mut stderr = child.stderr.take();
        let stderr_reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut stderr) = stderr.take() {
                let _ = stderr.read_to_end(&mut buf).await;
            }
            buf
        });

        let initial_conv_id = self
            .sessions
            .get(session_id)
            .and_then(|s| s.conversation_id.clone());
        let initial_step_idx = self
            .sessions
            .get(session_id)
            .map(|s| s.last_step_idx)
            .unwrap_or(-1);
        let streaming_state = Arc::new(Mutex::new(StreamingState {
            conversation_id: initial_conv_id,
            base_step_idx: initial_step_idx,
            last_step_idx: initial_step_idx,
            had_updates: false,
            agent_text_lengths: HashMap::new(),
            thought_text_lengths: HashMap::new(),
            emitted_tool_steps: HashSet::new(),
            last_title: None,
            skip_naration: self.skip_naration,
            child_pid: child.id(),
        }));
        let stop_polling = Arc::new(AtomicBool::new(false));
        let poll_conversations_dir = self.conversations_dir.clone();
        let poll_snapshot = snapshot.clone();
        let poll_session_id = session_id.to_string();
        let poll_state = Arc::clone(&streaming_state);
        let poll_stop = Arc::clone(&stop_polling);

        let poller = std::thread::spawn(move || {
            let mut stdout = io::stdout();
            while !poll_stop.load(Ordering::SeqCst) {
                for line in poll_streaming_delta(
                    &poll_conversations_dir,
                    poll_snapshot.as_ref(),
                    &poll_session_id,
                    &poll_state,
                ) {
                    let _ = writeln!(stdout, "{}", line);
                }
                let _ = stdout.flush();
                std::thread::sleep(Duration::from_millis(500));
            }
        });

        let mut was_cancelled = false;
        let result = tokio::select! {
            result = child.wait() => result,
            _ = async {
                while !cancelled.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            } => {
                was_cancelled = true;
                let _ = child.kill().await;
                child.wait().await
            }
        };
        let _ = stdout_reader.await;
        let stderr_bytes = stderr_reader.await.unwrap_or_default();
        stop_polling.store(true, Ordering::SeqCst);
        let _ = poller.join();

        let mut final_lines = Vec::new();
        for attempt in 0..3 {
            let lines = poll_streaming_delta(
                &self.conversations_dir,
                snapshot.as_ref(),
                session_id,
                &streaming_state,
            );
            final_lines.extend(lines);
            if attempt < 2 {
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        {
            let mut stdout = io::stdout();
            for line in &final_lines {
                let _ = writeln!(stdout, "{}", line);
            }
            let _ = stdout.flush();
        }

        let state = streaming_state.lock().unwrap();
        let bound_conv_id = state.conversation_id.clone();
        let new_step_idx = state.last_step_idx;
        let had_updates = state.had_updates || !final_lines.is_empty();
        drop(state);

        if let Some(session) = self.sessions.get_mut(session_id) {
            if session.conversation_id.is_none() {
                session.conversation_id = bound_conv_id.clone();
            }
            if bound_conv_id.is_some() {
                session.last_step_idx = new_step_idx;
            }
        }
        if bound_conv_id.is_some() {
            if let Some(session) = self.sessions.get(session_id).cloned() {
                self.persist_session_state(session_id, &session);
            }
        }

        let stop_reason = if was_cancelled {
            "cancelled"
        } else if result.as_ref().map(|s| !s.success()).unwrap_or(false) {
            "error"
        } else {
            "end_turn"
        };

        match result {
            Ok(status) => {
                let stderr_text = String::from_utf8_lossy(&stderr_bytes);
                if !stderr_text.is_empty() {
                    eprintln!("[agy-acp] agy stderr: {}", stderr_text.trim_end());
                }
                if !was_cancelled && !status.success() {
                    eprintln!("[agy-acp] WARN: agy exited with status: {}", status);
                }
                // agy --print swallows backend failures (e.g. quota 429 /
                // RESOURCE_EXHAUSTED) with a 0 exit code and empty stdout/stderr,
                // recording the cause only in its own cli.log; an empty successful
                // turn is therefore almost always a hidden error.
                let swallowed_error = if !was_cancelled && status.success() && !had_updates {
                    log_scan::detect_swallowed_agy_error(
                        &self.conversations_dir,
                        &log_pre_snapshot,
                        spawn_time,
                    )
                } else {
                    None
                };
                if let Some((code, msg)) = log_scan::decide_turn_error(
                    was_cancelled,
                    status.success(),
                    had_updates,
                    &status.to_string(),
                    &stderr_text,
                    swallowed_error.as_deref(),
                ) {
                    eprintln!("[agy-acp] surfacing turn error ({code}): {msg}");
                    return vec![serde_json::to_string(&JsonRpcResponse {
                        jsonrpc: "2.0",
                        id,
                        result: None,
                        error: Some(json!({"code":code,"message":msg})),
                    })
                    .unwrap()];
                }
            }
            Err(e) => {
                return vec![serde_json::to_string(&JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(
                        json!({"code":-32000,"message":format!("failed to wait for agy: {e}")}),
                    ),
                })
                .unwrap()];
            }
        }

        vec![serde_json::to_string(&JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "stopReason": stop_reason })),
            error: None,
        })
        .unwrap()]
    }
}

/// Filter out leading narration ("I will ...", "I'll ...") from response parts.
pub fn filter_narration(parts: &[String]) -> Option<String> {
    let text = parts
        .iter()
        .filter(|part| !is_narration(part))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

/// A part is considered narration if every non-empty line starts with "I will" or "I'll".
pub fn is_narration(text: &str) -> bool {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.is_empty() {
        return false;
    }
    lines.iter().all(|l| {
        let line = l.trim_start();
        line.starts_with("I will") || line.starts_with("I'll") || line.starts_with("I’ll")
    })
}
