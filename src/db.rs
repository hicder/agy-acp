use rusqlite::Connection;
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;

use crate::adapter::filter_narration;
use crate::protobuf::{
    extract_text_from_step_payload, extract_thought_from_step_payload,
    extract_tool_update_from_step_payload, extract_user_text_from_step_payload, is_tool_step_type,
    message_chunk_update,
};

#[cfg(test)]
use crate::types::ConversationDelta;

pub fn new_conversation_id_in_dir(
    conversations_dir: &Path,
    before: &HashSet<String>,
) -> Option<String> {
    let Ok(entries) = std::fs::read_dir(conversations_dir) else {
        return None;
    };
    let after: HashSet<String> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if path.extension().map(|x| x == "db").unwrap_or(false) {
                path.file_stem().map(|s| s.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();
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

pub fn read_rows_from_db(
    conversations_dir: &Path,
    conversation_id: &str,
    after_step_idx: i64,
) -> Option<Vec<(i64, i64, Vec<u8>)>> {
    let db_path = conversations_dir.join(format!("{}.db", conversation_id));
    let conn = Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;

    let table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='steps'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(false);
    if !table_exists {
        eprintln!(
            "[agy-acp] WARN: steps table not found in {}.db — schema changed?",
            conversation_id
        );
        return None;
    }

    let mut stmt = conn
        .prepare("SELECT idx, step_type, step_payload FROM steps WHERE idx > ?1 ORDER BY idx")
        .ok()?;
    let rows: Vec<(i64, i64, Vec<u8>)> = stmt
        .query_map([after_step_idx], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .ok()?
        .filter_map(|r| r.ok())
        .collect();
    Some(rows)
}

pub fn read_replay_updates_from_db(
    conversations_dir: &Path,
    conversation_id: &str,
    skip_naration: bool,
) -> Option<(Vec<Value>, i64)> {
    let rows = read_rows_from_db(conversations_dir, conversation_id, -1)?;
    let mut max_idx = -1;
    let mut updates = Vec::new();
    let mut pending_agent_parts = Vec::new();
    let mut pending_thought_parts = Vec::new();

    for (idx, step_type, payload) in &rows {
        max_idx = max_idx.max(*idx);
        if *step_type == 14 {
            flush_agent_message(&mut pending_agent_parts, &mut updates, skip_naration);
            flush_thought_message(&mut pending_thought_parts, &mut updates);
            if let Some(text) = extract_user_text_from_step_payload(payload) {
                updates.push(message_chunk_update("user_message_chunk", text));
            }
        } else if *step_type == 15 {
            if let Some(text) = extract_text_from_step_payload(payload) {
                if !text.is_empty() {
                    pending_agent_parts.push(text);
                }
            }
            if let Some(text) = extract_thought_from_step_payload(payload) {
                pending_thought_parts.push(text);
            }
        } else if is_tool_step_type(*step_type) {
            flush_agent_message(&mut pending_agent_parts, &mut updates, skip_naration);
            flush_thought_message(&mut pending_thought_parts, &mut updates);
            if let Some(update) = extract_tool_update_from_step_payload(*idx, *step_type, payload) {
                updates.push(update);
            }
        }
    }
    flush_agent_message(&mut pending_agent_parts, &mut updates, skip_naration);
    flush_thought_message(&mut pending_thought_parts, &mut updates);

    if updates.is_empty() {
        return None;
    }
    Some((updates, max_idx))
}

#[cfg(test)]
pub fn read_delta_from_db(
    conversations_dir: &Path,
    conversation_id: &str,
    after_step_idx: i64,
) -> Option<ConversationDelta> {
    let rows = read_rows_from_db(conversations_dir, conversation_id, after_step_idx)?;

    let mut max_idx = after_step_idx;
    let mut response_parts: Vec<String> = Vec::new();
    for (idx, step_type, payload) in &rows {
        if *step_type == 15 {
            max_idx = max_idx.max(*idx);
            if let Some(text) = extract_text_from_step_payload(payload) {
                if !text.is_empty() {
                    response_parts.push(text);
                }
            }
        }
    }
    if response_parts.is_empty() {
        let response_rows: Vec<_> = rows
            .iter()
            .filter(|(_, step_type, _)| *step_type == 15)
            .collect();
        if !response_rows.is_empty() {
            let payload_sizes: Vec<usize> = response_rows.iter().map(|(_, _, p)| p.len()).collect();
            eprintln!(
                "[agy-acp] WARN: {} new response steps found (payload sizes: {:?}) but none had extractable text \
                 (field 20.1 missing — schema change?)",
                response_rows.len(), payload_sizes
            );
        }
        return None;
    }
    let text = if response_parts.is_empty() {
        None
    } else {
        Some(response_parts.join("\n"))
    };
    Some(ConversationDelta {
        text,
        max_step_idx: max_idx,
    })
}

fn flush_agent_message(parts: &mut Vec<String>, updates: &mut Vec<Value>, skip_naration: bool) {
    if parts.is_empty() {
        return;
    }
    let text = if skip_naration {
        filter_narration(parts)
    } else {
        Some(parts.join("\n"))
    };
    parts.clear();
    if let Some(text) = text {
        if !text.is_empty() {
            updates.push(message_chunk_update("agent_message_chunk", text));
        }
    }
}

fn flush_thought_message(parts: &mut Vec<String>, updates: &mut Vec<Value>) {
    if parts.is_empty() {
        return;
    }
    let text = parts.join("\n");
    parts.clear();
    if !text.is_empty() {
        updates.push(message_chunk_update("agent_thought_chunk", text));
    }
}
