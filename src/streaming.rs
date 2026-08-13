use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use crate::adapter::is_narration;
use crate::tools::{
    message_chunk_update, thought_text_from_value, tool_content, tool_kind, tool_locations,
};
use crate::types::JsonRpcNotification;

#[derive(Debug, Deserialize)]
struct StreamEvent {
    event: String,
    conversation_id: Option<String>,
    step_update: Option<StepUpdate>,
    result: Option<ResultEvent>,
}

#[derive(Debug, Deserialize)]
struct StepUpdate {
    conversation_id: Option<String>,
    step_index: Option<i64>,
    state: Option<String>,
    step_type: Option<String>,
    tool_name: Option<String>,
    text_delta: Option<String>,
    thought_delta: Option<String>,
    tool_info: Option<Value>,
    subagent_info: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ResultEvent {
    conversation_id: Option<String>,
    status: Option<String>,
    response: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
pub struct StreamProcessor {
    pub conversation_id: Option<String>,
    pub last_step_idx: i64,
    pub had_updates: bool,
    pub result_status: Option<String>,
    pub result_error: Option<String>,
    skip_naration: bool,
    agent_text: HashMap<i64, String>,
    thought_text: HashMap<i64, String>,
    agent_emitted: HashMap<i64, usize>,
    thought_emitted: HashMap<i64, usize>,
    started_tools: HashSet<i64>,
    emitted_agent_text: bool,
}

impl StreamProcessor {
    pub fn new(skip_naration: bool) -> Self {
        Self {
            conversation_id: None,
            last_step_idx: -1,
            had_updates: false,
            result_status: None,
            result_error: None,
            skip_naration,
            agent_text: HashMap::new(),
            thought_text: HashMap::new(),
            agent_emitted: HashMap::new(),
            thought_emitted: HashMap::new(),
            started_tools: HashSet::new(),
            emitted_agent_text: false,
        }
    }

    pub fn process_line(&mut self, line: &str, session_id: &str) -> Vec<String> {
        let line = line.trim();
        if line.is_empty() {
            return Vec::new();
        }
        let Ok(event) = serde_json::from_str::<StreamEvent>(line) else {
            return Vec::new();
        };
        self.process_event(event, session_id)
    }

    fn bind_conversation_id(&mut self, conversation_id: Option<&str>) {
        if self.conversation_id.is_some() {
            return;
        }
        if let Some(id) = conversation_id.filter(|id| !id.is_empty()) {
            self.conversation_id = Some(id.to_string());
        }
    }

    fn process_event(&mut self, event: StreamEvent, session_id: &str) -> Vec<String> {
        self.bind_conversation_id(event.conversation_id.as_deref());
        match event.event.as_str() {
            "init" => Vec::new(),
            "step_update" => event
                .step_update
                .map(|step| self.process_step(step, session_id))
                .unwrap_or_default(),
            "result" => event
                .result
                .map(|result| self.process_result(result, session_id))
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn process_step(&mut self, step: StepUpdate, session_id: &str) -> Vec<String> {
        self.bind_conversation_id(step.conversation_id.as_deref());
        let step_index = step.step_index.unwrap_or(-1);
        self.last_step_idx = self.last_step_idx.max(step_index);

        let step_type = step.step_type.as_deref().unwrap_or("");
        let mut notifications = Vec::new();

        if let Some(delta) = step.text_delta.as_deref().filter(|s| !s.is_empty()) {
            if is_thought_step(step_type) {
                notifications.extend(self.emit_thought_delta(session_id, step_index, delta));
            } else if should_emit_agent_text(step_type) {
                notifications.extend(self.emit_agent_delta(session_id, step_index, delta));
            }
        }

        if let Some(delta) = step.thought_delta.as_deref().filter(|s| !s.is_empty()) {
            notifications.extend(self.emit_thought_delta(session_id, step_index, delta));
        }

        if step_type == "tool" || step.tool_info.is_some() || step.tool_name.is_some() {
            let done = step.state.as_deref() == Some("DONE");
            notifications.extend(self.emit_tool(
                session_id,
                step_index,
                step.tool_name.as_deref(),
                step.tool_info.as_ref(),
                done,
            ));
        }

        if let Some(info) = step.subagent_info.as_ref() {
            notifications.extend(self.emit_subagent(session_id, step_index, info));
        }

        notifications
    }

    fn process_result(&mut self, result: ResultEvent, session_id: &str) -> Vec<String> {
        self.bind_conversation_id(result.conversation_id.as_deref());
        self.result_status = result.status;
        self.result_error = result.error.filter(|s| !s.is_empty());

        let Some(response) = result.response.filter(|s| !s.is_empty()) else {
            return Vec::new();
        };
        if self.emitted_agent_text {
            return Vec::new();
        }
        self.emit_agent_delta(session_id, self.last_step_idx.max(0), &response)
    }

    fn emit_agent_delta(&mut self, session_id: &str, step_index: i64, delta: &str) -> Vec<String> {
        let accumulated = self.agent_text.entry(step_index).or_default();
        accumulated.push_str(delta);
        let emitted = self.agent_emitted.entry(step_index).or_insert(0);
        if self.skip_naration && is_narration(accumulated) {
            *emitted = accumulated.len();
            return Vec::new();
        }
        if *emitted >= accumulated.len() {
            return Vec::new();
        }
        let new_text = accumulated[*emitted..].to_string();
        *emitted = accumulated.len();
        if new_text.is_empty() {
            return Vec::new();
        }
        self.emitted_agent_text = true;
        self.had_updates = true;
        vec![notification(
            session_id,
            message_chunk_update("agent_message_chunk", new_text),
        )]
    }

    fn emit_thought_snapshot(
        &mut self,
        session_id: &str,
        step_index: i64,
        text: &str,
    ) -> Vec<String> {
        let already = self
            .thought_text
            .get(&step_index)
            .cloned()
            .unwrap_or_default();
        if text == already || already.starts_with(text) {
            return Vec::new();
        }
        let delta = if text.starts_with(&already) {
            &text[already.len()..]
        } else if already.is_empty() {
            text
        } else {
            return Vec::new();
        };
        self.emit_thought_delta(session_id, step_index, delta)
    }

    fn emit_thought_delta(
        &mut self,
        session_id: &str,
        step_index: i64,
        delta: &str,
    ) -> Vec<String> {
        let accumulated = self.thought_text.entry(step_index).or_default();
        accumulated.push_str(delta);
        let emitted = self.thought_emitted.entry(step_index).or_insert(0);
        if *emitted >= accumulated.len() {
            return Vec::new();
        }
        let new_text = accumulated[*emitted..].to_string();
        *emitted = accumulated.len();
        if new_text.is_empty() {
            return Vec::new();
        }
        self.had_updates = true;
        vec![notification(
            session_id,
            message_chunk_update("agent_thought_chunk", new_text),
        )]
    }

    fn emit_tool(
        &mut self,
        session_id: &str,
        step_index: i64,
        tool_name: Option<&str>,
        tool_info: Option<&Value>,
        done: bool,
    ) -> Vec<String> {
        let name = tool_info
            .and_then(|info| info.get("name"))
            .and_then(|v| v.as_str())
            .or(tool_name)
            .unwrap_or("tool");
        let parameters = tool_info.and_then(|info| info.get("parameters"));
        let raw_output = tool_info.and_then(tool_output_value);
        let error = tool_info.and_then(|info| info.get("error"));

        if tool_kind(name) == "think" {
            let thought = parameters
                .and_then(thought_text_from_value)
                .or_else(|| raw_output.as_ref().and_then(thought_text_from_value));
            if let Some(text) = thought.filter(|text| !text.trim().is_empty()) {
                return self.emit_thought_snapshot(session_id, step_index, &text);
            }
            if !done {
                return Vec::new();
            }
        }

        let already_started = !self.started_tools.insert(step_index);
        if already_started && !done {
            return Vec::new();
        }

        let title = parameters
            .and_then(|v| v.get("toolSummary").or_else(|| v.get("toolAction")))
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| name.to_string());
        let kind = tool_kind(name);
        let status = if error.is_some() {
            "failed"
        } else if done {
            "completed"
        } else {
            "in_progress"
        };

        let session_update = if already_started {
            "tool_call_update"
        } else {
            "tool_call"
        };
        let mut update = json!({
            "sessionUpdate": session_update,
            "toolCallId": format!("agy-{step_index}"),
            "title": title,
            "kind": kind,
            "status": status,
        });
        if let Some(input) = parameters.cloned() {
            update["rawInput"] = input;
        }
        if let Some(output) = raw_output.clone() {
            update["rawOutput"] = output;
        }
        if let Some(error) = error.cloned() {
            update["rawOutput"] = match update.get("rawOutput") {
                Some(Value::Object(_)) => {
                    let mut out = update["rawOutput"].clone();
                    out["error"] = error;
                    out
                }
                _ => json!({ "error": error }),
            };
        }

        let mut locations = parameters.map(tool_locations).unwrap_or_default();
        if let Some(output_locations) = raw_output.as_ref().map(tool_locations) {
            locations.extend(output_locations);
        }
        if !locations.is_empty() {
            update["locations"] = Value::Array(locations);
        }

        let content = raw_output
            .as_ref()
            .and_then(|output| tool_content(output, true))
            .or_else(|| parameters.and_then(|input| tool_content(input, false)));
        if let Some(content) = content {
            update["content"] = Value::Array(vec![content]);
        }

        self.had_updates = true;
        vec![notification(session_id, update)]
    }

    fn emit_subagent(&mut self, session_id: &str, step_index: i64, info: &Value) -> Vec<String> {
        if !self.started_tools.insert(step_index) {
            return Vec::new();
        }
        let title = info
            .get("subagents")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|agent| {
                agent
                    .get("role")
                    .or_else(|| agent.get("type_name"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("subagent");
        self.had_updates = true;
        vec![notification(
            session_id,
            json!({
                "sessionUpdate": "tool_call",
                "toolCallId": format!("agy-{step_index}"),
                "title": title,
                "kind": "other",
                "status": "completed",
                "rawOutput": info,
            }),
        )]
    }
}

fn is_thought_step(step_type: &str) -> bool {
    matches!(
        step_type,
        "thinking" | "thought" | "agent_thought" | "reasoning"
    )
}

fn should_emit_agent_text(step_type: &str) -> bool {
    !matches!(
        step_type,
        "user_input"
            | "checkpoint"
            | "unknown"
            | "tool"
            | "thinking"
            | "thought"
            | "agent_thought"
            | "reasoning"
    )
}

fn tool_output_value(info: &Value) -> Option<Value> {
    match info.get("output") {
        Some(Value::String(s)) if !s.is_empty() => Some(json!({ "output": s })),
        Some(value) if !value.is_null() => Some(value.clone()),
        _ => None,
    }
}

fn notification(session_id: &str, update: Value) -> String {
    serde_json::to_string(&JsonRpcNotification {
        jsonrpc: "2.0",
        method: "session/update".to_string(),
        params: json!({
            "sessionId": session_id,
            "update": update,
        }),
    })
    .unwrap()
}
