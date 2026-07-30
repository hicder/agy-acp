use serde_json::json;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::adapter::is_narration;
use crate::db::{find_conversation_id_by_pid, new_conversation_id_in_dir, read_rows_from_db};
use crate::protobuf::{
    extract_text_from_step_payload, extract_thought_from_step_payload,
    extract_title_from_step_payload, extract_tool_update_from_step_payload, is_tool_step_type,
};
use crate::types::{JsonRpcNotification, StreamingState};

pub fn poll_streaming_delta(
    conversations_dir: &Path,
    snapshot: Option<&HashSet<String>>,
    session_id: &str,
    state: &Arc<Mutex<StreamingState>>,
) -> Vec<String> {
    let (conversation_id, base_step_idx) = {
        let mut guard = state.lock().unwrap();
        if guard.conversation_id.is_none() {
            // Prefer binding via the child's open conversation DB FD when available;
            // fall back to directory snapshot diff for newly created conversations.
            if let Some(pid) = guard.child_pid {
                guard.conversation_id = find_conversation_id_by_pid(pid, conversations_dir);
            }
            if guard.conversation_id.is_none() {
                if let Some(before) = snapshot {
                    guard.conversation_id = new_conversation_id_in_dir(conversations_dir, before);
                }
            }
        }
        (guard.conversation_id.clone(), guard.base_step_idx)
    };

    let Some(conversation_id) = conversation_id else {
        return Vec::new();
    };

    let Some(rows) = read_rows_from_db(conversations_dir, &conversation_id, base_step_idx) else {
        return Vec::new();
    };

    let mut guard = state.lock().unwrap();
    let mut notifications = Vec::new();

    for (idx, step_type, payload) in rows {
        guard.last_step_idx = guard.last_step_idx.max(idx);

        if step_type == 15 {
            if let Some(text) = extract_text_from_step_payload(&payload) {
                let written_len = guard.agent_text_lengths.get(&idx).copied().unwrap_or(0);
                if text.len() > written_len {
                    if guard.skip_naration && is_narration(&text) {
                        guard.agent_text_lengths.insert(idx, text.len());
                    } else if let Some(new_text) = text.get(written_len..) {
                        guard.agent_text_lengths.insert(idx, text.len());
                        if !new_text.is_empty() {
                            notifications.push(
                                serde_json::to_string(&JsonRpcNotification {
                                    jsonrpc: "2.0",
                                    method: "session/update".to_string(),
                                    params: json!({
                                        "sessionId": session_id,
                                        "update": {
                                            "sessionUpdate": "agent_message_chunk",
                                            "content": { "type": "text", "text": new_text },
                                        },
                                    }),
                                })
                                .unwrap(),
                            );
                        }
                    }
                }
            }

            if let Some(thought) = extract_thought_from_step_payload(&payload) {
                let written_len = guard.thought_text_lengths.get(&idx).copied().unwrap_or(0);
                if thought.len() > written_len {
                    let Some(new_text) = thought.get(written_len..) else {
                        continue;
                    };
                    guard.thought_text_lengths.insert(idx, thought.len());
                    if !new_text.is_empty() {
                        notifications.push(
                            serde_json::to_string(&JsonRpcNotification {
                                jsonrpc: "2.0",
                                method: "session/update".to_string(),
                                params: json!({
                                    "sessionId": session_id,
                                    "update": {
                                        "sessionUpdate": "agent_thought_chunk",
                                        "content": { "type": "text", "text": new_text },
                                    },
                                }),
                            })
                            .unwrap(),
                        );
                    }
                }
            }
        } else if step_type == 23 {
            let Some(title) = extract_title_from_step_payload(&payload) else {
                continue;
            };
            if guard.last_title.as_deref() == Some(title.as_str()) {
                continue;
            }
            guard.last_title = Some(title.clone());
            notifications.push(
                serde_json::to_string(&JsonRpcNotification {
                    jsonrpc: "2.0",
                    method: "session/update".to_string(),
                    params: json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "session_info_update",
                            "title": title,
                        },
                    }),
                })
                .unwrap(),
            );
        } else if is_tool_step_type(step_type) && !guard.emitted_tool_steps.contains(&idx) {
            if let Some(update) = extract_tool_update_from_step_payload(idx, step_type, &payload) {
                guard.emitted_tool_steps.insert(idx);
                notifications.push(
                    serde_json::to_string(&JsonRpcNotification {
                        jsonrpc: "2.0",
                        method: "session/update".to_string(),
                        params: json!({
                            "sessionId": session_id,
                            "update": update,
                        }),
                    })
                    .unwrap(),
                );
            }
        }
    }

    guard.had_updates = guard.had_updates || !notifications.is_empty();
    notifications
}
