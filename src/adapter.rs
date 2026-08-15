use fs2::FileExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use uuid::Uuid;

use crate::streaming::StreamProcessor;
use crate::types::*;

pub struct Adapter {
    pub sessions: HashMap<String, Session>,
    pub working_dir: String,
    pub state_file: PathBuf,
    pub available_models: Vec<String>,
    pub skip_naration: bool,
    pub dangerously_skip_permissions: bool,
    pub sandbox: bool,
}

impl Adapter {
    pub const MODEL_CONFIG_ID: &'static str = "model";
    pub const EFFORT_CONFIG_ID: &'static str = "effort";
    pub const DEFAULT_MODE_ID: &'static str = "accept-edits";
    pub const AVAILABLE_MODES: [(&'static str, &'static str); 2] =
        [("accept-edits", "Accept edits"), ("plan", "Plan")];
    pub const AVAILABLE_EFFORTS: [&'static str; 3] = ["low", "medium", "high"];

    #[cfg(test)]
    pub fn new() -> Self {
        Self::new_with_skip_naration(false)
    }

    #[cfg(test)]
    pub fn new_with_skip_naration(skip_naration: bool) -> Self {
        Self::new_with_options(skip_naration, false, false)
    }

    pub fn new_with_options(
        skip_naration: bool,
        dangerously_skip_permissions: bool,
        sandbox: bool,
    ) -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let state_dir = PathBuf::from(&home).join(".openab/agy-acp");
        Self {
            sessions: HashMap::new(),
            working_dir: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/tmp".to_string()),
            state_file: state_dir.join("sessions.json"),
            available_models: Self::fetch_available_models(),
            skip_naration,
            dangerously_skip_permissions,
            sandbox,
        }
    }

    /// Run `agy models` and parse the output into a list of model names.
    fn fetch_available_models() -> Vec<String> {
        std::process::Command::new("agy")
            .arg("models")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| parse_available_models(&String::from_utf8_lossy(&o.stdout)))
            .unwrap_or_default()
    }

    /// Build the ACP `models` JSON for a session, given its current model_id.
    pub fn session_models_json(&mut self, model_id: Option<&str>) -> Value {
        if self.available_models.is_empty() {
            self.available_models = Self::fetch_available_models();
        }
        let current = model_id
            .or_else(|| self.available_models.first().map(|s| s.as_str()))
            .unwrap_or("");
        let available: Vec<Value> = self
            .available_models
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

    /// Build the ACP session config option that Zed uses for its model selector.
    pub fn session_modes_json(mode_id: Option<&str>) -> Value {
        let current = mode_id.unwrap_or(Self::DEFAULT_MODE_ID);
        json!({
            "currentModeId": current,
            "availableModes": Self::AVAILABLE_MODES.iter().map(|(id, name)| json!({ "id": id, "name": name })).collect::<Vec<_>>(),
        })
    }

    #[cfg(test)]
    pub fn session_config_options_json(&mut self, model_id: Option<&str>) -> Value {
        self.session_config_options_json_with_effort(model_id, None)
    }

    pub fn session_config_options_json_with_effort(
        &mut self,
        model_id: Option<&str>,
        effort: Option<&str>,
    ) -> Value {
        if self.available_models.is_empty() {
            self.available_models = Self::fetch_available_models();
        }
        let current = model_id
            .or_else(|| self.available_models.first().map(|s| s.as_str()))
            .unwrap_or("");
        let options: Vec<Value> = self
            .available_models
            .iter()
            .map(|name| {
                json!({
                    "value": name,
                    "name": name,
                })
            })
            .collect();
        json!([{
            "id": Self::MODEL_CONFIG_ID,
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": current,
            "options": options,
        }, {
            "id": Self::EFFORT_CONFIG_ID,
            "name": "Effort",
            "category": "general",
            "type": "select",
            "currentValue": effort.unwrap_or("medium"),
            "options": Self::AVAILABLE_EFFORTS.iter().map(|value| json!({ "value": value, "name": value })).collect::<Vec<_>>(),
        }])
    }

    pub fn session_config_result_json(
        &mut self,
        session_id: &str,
        model_id: Option<&str>,
        mode_id: Option<&str>,
        effort: Option<&str>,
    ) -> Value {
        json!({
            "sessionId": session_id,
            "models": self.session_models_json(model_id),
            "modes": Self::session_modes_json(mode_id),
            "configOptions": self.session_config_options_json_with_effort(model_id, effort),
        })
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

    /// Try to restore conversation_id, last_step_idx, and model_id from persisted state.
    #[cfg(test)]
    pub fn restore_session(&self, session_id: &str) -> Option<(String, i64, Option<String>)> {
        self.restore_session_with_controls(session_id).map(
            |(conversation_id, last_step_idx, model_id, _, _)| {
                (conversation_id, last_step_idx, model_id)
            },
        )
    }

    pub fn restore_session_with_controls(
        &self,
        session_id: &str,
    ) -> Option<(String, i64, Option<String>, String, Option<String>)> {
        let store = self.load_store();
        store.sessions.get(session_id).and_then(|s| {
            s.conversation_id.clone().map(|cid| {
                (
                    cid,
                    s.last_step_idx,
                    s.model_id.clone(),
                    s.mode_id.clone(),
                    s.effort.clone(),
                )
            })
        })
    }

    /// Persist a session binding (read-modify-write under single lock).
    #[cfg(test)]
    pub fn persist_session(
        &self,
        session_id: &str,
        conversation_id: Option<&str>,
        last_step_idx: i64,
        model_id: Option<&str>,
    ) {
        self.persist_session_with_controls(
            session_id,
            conversation_id,
            last_step_idx,
            model_id,
            Self::DEFAULT_MODE_ID,
            None,
        )
    }

    pub fn persist_session_with_controls(
        &self,
        session_id: &str,
        conversation_id: Option<&str>,
        last_step_idx: i64,
        model_id: Option<&str>,
        mode_id: &str,
        effort: Option<&str>,
    ) {
        let Some(_lock) = self.lock_state_file() else {
            return;
        };
        let mut store = self.load_store_inner();
        store.sessions.insert(
            session_id.to_string(),
            StoredSession {
                conversation_id: conversation_id.map(String::from),
                last_step_idx,
                model_id: model_id.map(String::from),
                mode_id: mode_id.to_string(),
                effort: effort.map(String::from),
            },
        );
        let tmp = self.state_file.with_extension("tmp");
        if let Ok(file) = fs::File::create(&tmp) {
            if serde_json::to_writer_pretty(&file, &store).is_ok() {
                let _ = fs::rename(&tmp, &self.state_file);
            }
        }
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
        let Some((conversation_id, last_step_idx, model_id, mode_id, effort)) =
            self.restore_session_with_controls(session_id)
        else {
            return false;
        };
        if !self.sessions.contains_key(session_id) {
            self.evict_if_needed();
        }
        self.sessions.insert(
            session_id.to_string(),
            Session {
                conversation_id: Some(conversation_id),
                last_step_idx,
                model_id,
                mode_id,
                effort,
            },
        );
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
                    "sessionCapabilities": { "resume": {} },
                },
                "authMethods": [],
            })),
            error: None,
        }
    }

    pub fn handle_session_new(&mut self, id: Value) -> JsonRpcResponse {
        let session_id = Uuid::new_v4().to_string();
        self.evict_if_needed();
        self.sessions.insert(
            session_id.clone(),
            Session {
                conversation_id: None,
                last_step_idx: -1,
                model_id: None,
                mode_id: Self::DEFAULT_MODE_ID.to_string(),
                effort: None,
            },
        );
        let result =
            self.session_config_result_json(&session_id, None, Some(Self::DEFAULT_MODE_ID), None);
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

        vec![{
            let session = self.sessions.get(session_id).unwrap();
            let model_id = session.model_id.clone();
            let mode_id = session.mode_id.clone();
            let effort = session.effort.clone();
            let result = self.session_config_result_json(
                session_id,
                model_id.as_deref(),
                Some(&mode_id),
                effort.as_deref(),
            );
            serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            })
            .unwrap()
        }]
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
            let session = self.sessions.get(session_id).unwrap();
            let model_id = session.model_id.clone();
            let mode_id = session.mode_id.clone();
            let effort = session.effort.clone();
            let result = self.session_config_result_json(
                session_id,
                model_id.as_deref(),
                Some(&mode_id),
                effort.as_deref(),
            );
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
        let model_id_str = session.model_id.clone();
        let last_step_idx = session.last_step_idx;
        let conv_id = session.conversation_id.clone();
        let mode_id = session.mode_id.clone();
        let effort = session.effort.clone();

        self.persist_session_with_controls(
            session_id,
            conv_id.as_deref(),
            last_step_idx,
            model_id_str.as_deref(),
            &mode_id,
            effort.as_deref(),
        );

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
        let value = params.get("value").and_then(|v| v.as_str()).unwrap_or("");

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

        if config_id != Self::MODEL_CONFIG_ID && config_id != Self::EFFORT_CONFIG_ID {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32602,
                    "message": format!("unknown configId: {config_id}"),
                })),
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

        if config_id == Self::MODEL_CONFIG_ID {
            session.model_id = Some(value.to_string());
        } else if !Self::AVAILABLE_EFFORTS.contains(&value) {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(
                    json!({ "code": -32602, "message": format!("invalid effort: {value}") }),
                ),
            };
        } else {
            session.effort = Some(value.to_string());
        }
        let model_id_str = session.model_id.clone();
        let last_step_idx = session.last_step_idx;
        let conv_id = session.conversation_id.clone();
        let mode_id = session.mode_id.clone();
        let effort = session.effort.clone();

        self.persist_session_with_controls(
            session_id,
            conv_id.as_deref(),
            last_step_idx,
            model_id_str.as_deref(),
            &mode_id,
            effort.as_deref(),
        );

        let config_options = self
            .session_config_options_json_with_effort(model_id_str.as_deref(), effort.as_deref());
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "configOptions": config_options })),
            error: None,
        }
    }

    pub fn handle_session_set_mode(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let mode_id = params.get("modeId").and_then(|v| v.as_str()).unwrap_or("");
        if session_id.is_empty() || mode_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId or modeId"})),
            };
        }
        if !Self::AVAILABLE_MODES.iter().any(|(id, _)| *id == mode_id) {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":format!("invalid modeId: {mode_id}")})),
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
                error: Some(
                    json!({"code":-32000,"message":format!("unknown sessionId: {session_id}")}),
                ),
            };
        };
        session.mode_id = mode_id.to_string();
        let conversation_id = session.conversation_id.clone();
        let model_id = session.model_id.clone();
        let last_step_idx = session.last_step_idx;
        let effort = session.effort.clone();
        self.persist_session_with_controls(
            session_id,
            conversation_id.as_deref(),
            last_step_idx,
            model_id.as_deref(),
            mode_id,
            effort.as_deref(),
        );
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "modes": Self::session_modes_json(Some(mode_id)) })),
            error: None,
        }
    }

    pub(crate) fn child_args(&self, session_id: &str, clean_prompt: &str) -> Vec<String> {
        let mut args = vec!["--add-dir".to_string(), self.working_dir.clone()];
        if let Ok(extra) = std::env::var("AGY_EXTRA_ARGS") {
            args.extend(
                extra
                    .split_whitespace()
                    .filter(|arg| {
                        !(*arg == "--dangerously-skip-permissions"
                            && self.dangerously_skip_permissions)
                            && !(*arg == "--sandbox" && self.sandbox)
                    })
                    .map(String::from),
            );
        }
        if self.dangerously_skip_permissions {
            args.push("--dangerously-skip-permissions".to_string());
        }
        if self.sandbox {
            args.push("--sandbox".to_string());
        }
        args.extend(["--output-format".to_string(), "stream-json".to_string()]);
        if let Some(session) = self.sessions.get(session_id) {
            if let Some(conv_id) = &session.conversation_id {
                args.extend(["--conversation".to_string(), conv_id.clone()]);
            }
            if let Some(model_id) = &session.model_id {
                args.extend(["--model".to_string(), model_id.clone()]);
            }
            args.extend(["--mode".to_string(), session.mode_id.clone()]);
            if let Some(effort) = &session.effort {
                args.extend(["--effort".to_string(), effort.clone()]);
            }
        }
        args.extend(["-p".to_string(), clean_prompt.to_string()]);
        args
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

        let args = self.child_args(session_id, clean_prompt);

        let spawn_result = Command::new("agy")
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

        let stdout = child.stdout.take();
        let skip_naration = self.skip_naration;
        let poll_session_id = session_id.to_string();
        let stdout_reader = tokio::spawn(async move {
            let mut processor = StreamProcessor::new(skip_naration);
            if let Some(stdout) = stdout {
                let mut lines = BufReader::new(stdout).lines();
                let mut out = io::stdout();
                while let Ok(Some(line)) = lines.next_line().await {
                    for notification in processor.process_line(&line, &poll_session_id) {
                        let _ = writeln!(out, "{}", notification);
                    }
                    let _ = out.flush();
                }
            }
            processor
        });

        let mut stderr = child.stderr.take();
        let stderr_reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut stderr) = stderr.take() {
                let _ = stderr.read_to_end(&mut buf).await;
            }
            buf
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
        let processor = stdout_reader
            .await
            .unwrap_or_else(|_| StreamProcessor::new(skip_naration));
        let stderr_bytes = stderr_reader.await.unwrap_or_default();

        let bound_conv_id = processor.conversation_id.clone();
        let new_step_idx = processor.last_step_idx;
        let had_updates = processor.had_updates;
        let result_failed = processor
            .result_status
            .as_deref()
            .is_some_and(|status| status == "ERROR");
        let result_error = processor.result_error.clone();

        if let Some(session) = self.sessions.get_mut(session_id) {
            if session.conversation_id.is_none() {
                session.conversation_id = bound_conv_id.clone();
            }
            if bound_conv_id.is_some() {
                session.last_step_idx = new_step_idx;
            }
        }
        if bound_conv_id.is_some() {
            let model_id = self
                .sessions
                .get(session_id)
                .and_then(|s| s.model_id.clone());
            let session = self.sessions.get(session_id);
            self.persist_session_with_controls(
                session_id,
                bound_conv_id.as_deref(),
                new_step_idx,
                model_id.as_deref(),
                session
                    .map(|s| s.mode_id.as_str())
                    .unwrap_or(Self::DEFAULT_MODE_ID),
                session.and_then(|s| s.effort.as_deref()),
            );
        }

        let stop_reason = if was_cancelled {
            "cancelled"
        } else {
            "end_turn"
        };
        let output_lines = vec![serde_json::to_string(&JsonRpcResponse {
            jsonrpc: "2.0",
            id: id.clone(),
            result: Some(json!({ "stopReason": stop_reason })),
            error: None,
        })
        .unwrap()];

        match result {
            Ok(status) => {
                let stderr_text = String::from_utf8_lossy(&stderr_bytes);
                if !stderr_text.is_empty() {
                    eprintln!("[agy-acp] agy stderr: {}", stderr_text.trim_end());
                }

                if !was_cancelled && (!status.success() || result_failed) {
                    eprintln!("[agy-acp] WARN: agy exited with status: {}", status);
                    if !had_updates {
                        let msg = if let Some(error) = result_error.filter(|s| !s.is_empty()) {
                            format!("agy failed: {}", error.trim_end())
                        } else if stderr_text.is_empty() {
                            format!("agy exited with status: {}", status)
                        } else {
                            format!("agy failed: {}", stderr_text.trim_end())
                        };
                        return vec![serde_json::to_string(&JsonRpcResponse {
                            jsonrpc: "2.0",
                            id,
                            result: None,
                            error: Some(json!({"code":-32000,"message":msg})),
                        })
                        .unwrap()];
                    }
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

        output_lines
    }
}

/// Parse `agy models` stdout into display names.
///
/// Each model line is `slug<TAB>Display Name`. ACP clients show `modelId` and
/// `name` side by side, so we keep only the display name and skip status lines
/// like "Fetching available models...".
pub fn parse_available_models(stdout: &str) -> Vec<String> {
    stdout.lines().filter_map(parse_model_line).collect()
}

fn parse_model_line(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    if let Some((_, name)) = line.split_once('\t') {
        let name = name.trim();
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }
    if line.ends_with("...") {
        return None;
    }
    Some(line.to_string())
}

/// Filter out leading narration ("I will ...", "I'll ...") from response parts.
#[cfg(test)]
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
