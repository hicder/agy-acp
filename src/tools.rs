use serde_json::{json, Value};

pub fn tool_kind(tool_name: &str) -> &'static str {
    let lower = tool_name.to_lowercase();
    if lower.contains("write") || lower.contains("edit") || lower.contains("patch") {
        "edit"
    } else if lower.contains("delete") || lower.contains("remove") {
        "delete"
    } else if lower.contains("move") || lower.contains("rename") {
        "move"
    } else if lower.contains("read") || lower.contains("view") || lower.contains("list") {
        "read"
    } else if lower.contains("grep") || lower.contains("search") || lower.contains("find") {
        "search"
    } else if lower.contains("command") || lower.contains("execute") || lower.contains("terminal") {
        "execute"
    } else if lower.contains("think")
        || lower.contains("thought")
        || lower.contains("reason")
        || lower.contains("plan")
    {
        "think"
    } else if lower.contains("url") || lower.contains("fetch") {
        "fetch"
    } else {
        "other"
    }
}

fn fenced_code_block(text: &str) -> String {
    let mut fence_len = 3;
    let mut run_len = 0;
    for ch in text.chars() {
        if ch == '`' {
            run_len += 1;
            fence_len = fence_len.max(run_len + 1);
        } else {
            run_len = 0;
        }
    }

    let fence = "`".repeat(fence_len);
    format!("{fence}\n{text}\n{fence}")
}

pub fn tool_content(input: &Value, code_block: bool) -> Option<Value> {
    if let Some(text) = format_structured_tool_output(input) {
        return Some(json!({
            "type": "content",
            "content": { "type": "text", "text": fenced_code_block(&text) },
        }));
    }

    for key in [
        "thought",
        "thinking",
        "reasoning",
        "analysis",
        "plan",
        "text",
        "result",
        "output",
        "textOutput",
        "content",
        "summary",
        "message",
    ] {
        if let Some(text) = input.get(key).and_then(|v| v.as_str()) {
            if !text.trim().is_empty() {
                let text = if code_block {
                    fenced_code_block(text)
                } else {
                    text.to_string()
                };
                return Some(json!({
                    "type": "content",
                    "content": { "type": "text", "text": text },
                }));
            }
        }
    }
    None
}

fn format_structured_tool_output(input: &Value) -> Option<String> {
    match input.get("resultType").and_then(|v| v.as_str()) {
        Some("grepSearch") => format_grep_output(input),
        Some("listDirectory") => format_list_output(input),
        _ => None,
    }
}

fn format_grep_output(input: &Value) -> Option<String> {
    if input
        .get("textOutput")
        .and_then(|v| v.as_str())
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
    {
        return None;
    }

    let Some(hits) = input.get("hits").and_then(|v| v.as_array()) else {
        return Some("No matches".to_string());
    };
    if hits.is_empty() {
        return Some("No matches".to_string());
    }

    let lines: Vec<String> = hits
        .iter()
        .filter_map(|hit| {
            hit.as_object().map(|fields| {
                let mut parts: Vec<String> = fields
                    .iter()
                    .filter_map(|(key, value)| {
                        value
                            .as_str()
                            .map(String::from)
                            .or_else(|| value.as_i64().map(|n| n.to_string()))
                            .map(|value| (key, value))
                    })
                    .filter(|(_, value)| !value.trim().is_empty())
                    .map(|(key, value)| format!("{key}: {value}"))
                    .collect();
                parts.sort();
                parts.join(" | ")
            })
        })
        .filter(|line| !line.is_empty())
        .collect();

    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn format_list_output(input: &Value) -> Option<String> {
    let Some(entries) = input.get("entries").and_then(|v| v.as_array()) else {
        return Some("(empty directory)".to_string());
    };
    if entries.is_empty() {
        return Some("(empty directory)".to_string());
    }

    let lines: Vec<String> = entries
        .iter()
        .filter_map(|entry| {
            let name = entry.get("name").and_then(|v| v.as_str())?;
            if name.trim().is_empty() {
                return None;
            }
            let suffix = if entry
                .get("isDirectory")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                "/"
            } else {
                ""
            };
            Some(format!("{name}{suffix}"))
        })
        .collect();

    (!lines.is_empty()).then(|| lines.join("\n"))
}

pub fn tool_locations(input: &Value) -> Vec<Value> {
    let mut locations = Vec::new();
    for key in [
        "AbsolutePath",
        "DirectoryPath",
        "SearchPath",
        "path",
        "file",
        "FilePath",
        "fileUri",
        "dirUri",
        "cwdUri",
        "Cwd",
    ] {
        if let Some(path) = input.get(key).and_then(|v| v.as_str()) {
            let mut loc = json!({ "path": path });
            if let Some(line) = input
                .get("StartLine")
                .or_else(|| input.get("startLine"))
                .or_else(|| input.get("line"))
                .and_then(|v| v.as_i64())
            {
                loc["line"] = json!(line);
            }
            locations.push(loc);
        }
    }
    locations
}

pub fn message_chunk_update(session_update: &str, text: String) -> Value {
    json!({
        "sessionUpdate": session_update,
        "content": { "type": "text", "text": text },
    })
}

pub fn thought_text_from_value(input: &Value) -> Option<String> {
    tool_content(input, false).and_then(|content| {
        let inner = content.get("content")?;
        let content_type = inner.get("type").and_then(|value| value.as_str());
        let text = inner.get("text").and_then(|value| value.as_str());
        (content_type == Some("text")).then(|| text.map(str::to_string))?
    })
}
