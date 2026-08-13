use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

use crate::adapter::{filter_narration, parse_available_models, Adapter};
use crate::streaming::StreamProcessor;
use crate::tools::tool_kind;
use crate::Cli;
use clap::Parser;

fn process_lines(
    skip_naration: bool,
    session_id: &str,
    lines: &[&str],
) -> (StreamProcessor, Vec<Value>) {
    let mut processor = StreamProcessor::new(skip_naration);
    let mut updates = Vec::new();
    for line in lines {
        for notification in processor.process_line(line, session_id) {
            let parsed: Value = serde_json::from_str(&notification).unwrap();
            updates.push(parsed["params"]["update"].clone());
        }
    }
    (processor, updates)
}

#[test]
fn test_parse_skip_naration_flag() {
    assert!(
        Cli::try_parse_from(["agy-acp", "--skip-naration"])
            .unwrap()
            .skip_naration
    );
    assert!(!Cli::try_parse_from(["agy-acp"]).unwrap().skip_naration);
    assert!(Cli::try_parse_from(["agy-acp", "--skip-narration"]).is_err());
}

#[test]
fn test_stream_json_binds_conversation_and_emits_text_deltas() {
    let (processor, updates) = process_lines(
        false,
        "sess-1",
        &[
            r#"{"event":"init","conversation_id":"conv-abc","init":{"cwd":"/tmp"}}"#,
            r#"{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":0,"state":"DONE","step_type":"user_input"}}"#,
            r#"{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":1,"state":"DONE","step_type":"unknown"}}"#,
            r#"{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"OK"}}"#,
            r#"{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":2,"state":"DONE","step_type":"agent_response","text_delta":"\n"}}"#,
            r#"{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":3,"state":"DONE","step_type":"checkpoint"}}"#,
            r#"{"event":"result","result":{"conversation_id":"conv-abc","status":"SUCCESS","response":"OK\n"}}"#,
        ],
    );
    assert_eq!(processor.conversation_id.as_deref(), Some("conv-abc"));
    assert_eq!(processor.last_step_idx, 3);
    assert!(processor.had_updates);
    assert_eq!(
        updates
            .iter()
            .map(|u| u["sessionUpdate"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["agent_message_chunk", "agent_message_chunk"]
    );
    assert_eq!(updates[0]["content"]["text"], "OK");
    assert_eq!(updates[1]["content"]["text"], "\n");
}

#[test]
fn test_stream_json_skips_narration_prefix() {
    let (_processor, updates) = process_lines(
        true,
        "sess-1",
        &[
            r#"{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"I will inspect the file."}}"#,
            r#"{"event":"step_update","step_update":{"step_index":2,"state":"DONE","step_type":"agent_response","text_delta":"\nHere is the result."}}"#,
        ],
    );
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["sessionUpdate"], "agent_message_chunk");
    assert_eq!(updates[0]["content"]["text"], "\nHere is the result.");
}

#[test]
fn test_stream_json_emits_tool_call_then_update() {
    let (_processor, updates) = process_lines(
        false,
        "sess-1",
        &[
            r#"{"event":"step_update","step_update":{"step_index":3,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hello"}}}}"#,
            r#"{"event":"step_update","step_update":{"step_index":3,"state":"DONE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hello"},"output":"hello\n"}}}"#,
        ],
    );
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0]["sessionUpdate"], "tool_call");
    assert_eq!(updates[0]["status"], "in_progress");
    assert_eq!(updates[0]["toolCallId"], "agy-3");
    assert_eq!(updates[0]["kind"], "execute");
    assert_eq!(updates[0]["rawInput"]["CommandLine"], "echo hello");
    assert_eq!(updates[1]["sessionUpdate"], "tool_call_update");
    assert_eq!(updates[1]["status"], "completed");
    assert_eq!(updates[1]["rawOutput"]["output"], "hello\n");
}

#[test]
fn test_stream_json_thinking_tool_emits_thought_chunk() {
    let (_processor, updates) = process_lines(
        false,
        "sess-1",
        &[
            r#"{"event":"step_update","step_update":{"step_index":4,"state":"DONE","step_type":"tool","tool_name":"thinking","tool_info":{"name":"thinking","parameters":{"thought":"Need to inspect the protocol.","toolSummary":"Reasoning"}}}}"#,
        ],
    );
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["sessionUpdate"], "agent_thought_chunk");
    assert_eq!(
        updates[0]["content"]["text"],
        "Need to inspect the protocol."
    );
}

#[test]
fn test_stream_json_result_fallback_when_no_text_delta() {
    let (_processor, updates) = process_lines(
        false,
        "sess-1",
        &[
            r#"{"event":"init","conversation_id":"conv-x","init":{}}"#,
            r#"{"event":"result","result":{"conversation_id":"conv-x","status":"SUCCESS","response":"PONG\n"}}"#,
        ],
    );
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["sessionUpdate"], "agent_message_chunk");
    assert_eq!(updates[0]["content"]["text"], "PONG\n");
}

#[test]
fn test_tool_kind_mapping() {
    assert_eq!(tool_kind("run_command"), "execute");
    assert_eq!(tool_kind("view_file"), "read");
    assert_eq!(tool_kind("write_to_file"), "edit");
    assert_eq!(tool_kind("grep_search"), "search");
    assert_eq!(tool_kind("thinking"), "think");
}

#[test]
fn test_initialize_advertises_load_session_support() {
    let adapter = Adapter::new();
    let response = adapter.handle_initialize(json!(1));
    assert_eq!(
        response
            .result
            .as_ref()
            .and_then(|r| r.get("agentCapabilities"))
            .and_then(|c| c.get("loadSession"))
            .and_then(|v| v.as_bool()),
        Some(true)
    );
}

#[test]
fn test_initialize_advertises_resume_capability() {
    let adapter = Adapter::new();
    let response = adapter.handle_initialize(json!(1));
    assert!(
        response
            .result
            .as_ref()
            .and_then(|r| r.get("agentCapabilities"))
            .and_then(|c| c.get("sessionCapabilities"))
            .and_then(|sc| sc.get("resume"))
            .is_some(),
        "sessionCapabilities.resume should be present"
    );
}

#[test]
#[ignore]
fn test_session_load_restores_persisted_session() {
    let root = std::env::temp_dir().join(format!("agy-acp-load-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    adapter.persist_session("sess-1", Some("conv-abc"), 5, None);

    let output = adapter.handle_session_load(json!(7), &json!({"sessionId": "sess-1"}));
    let response: Value = serde_json::from_str(output.last().unwrap()).unwrap();
    assert!(response["error"].is_null());
    assert_eq!(
        adapter
            .sessions
            .get("sess-1")
            .and_then(|s| s.conversation_id.as_deref()),
        Some("conv-abc")
    );
    assert_eq!(
        adapter.sessions.get("sess-1").map(|s| s.last_step_idx),
        Some(5)
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
#[ignore]
fn test_session_load_rejects_unknown_session() {
    let root = std::env::temp_dir().join(format!("agy-acp-missing-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };

    let output = adapter.handle_session_load(json!(9), &json!({"sessionId": "missing"}));
    let response: Value = serde_json::from_str(output.last().unwrap()).unwrap();
    assert!(response["result"].is_null());
    assert_eq!(
        response["error"]["message"].as_str(),
        Some("unknown sessionId: missing")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
#[ignore]
fn test_session_resume_restores_persisted_session() {
    let root = std::env::temp_dir().join(format!("agy-acp-resume-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    adapter.persist_session("sess-r1", Some("conv-xyz"), 3, None);

    let response = adapter.handle_session_resume(json!(10), &json!({"sessionId": "sess-r1"}));
    assert!(response.error.is_none());
    assert_eq!(
        response
            .result
            .as_ref()
            .and_then(|r| r.get("sessionId"))
            .and_then(|s| s.as_str()),
        Some("sess-r1")
    );
    assert_eq!(
        adapter
            .sessions
            .get("sess-r1")
            .and_then(|s| s.conversation_id.as_deref()),
        Some("conv-xyz")
    );
    assert_eq!(
        adapter.sessions.get("sess-r1").map(|s| s.last_step_idx),
        Some(3)
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
#[ignore]
fn test_session_resume_rejects_unknown_session() {
    let root = std::env::temp_dir().join(format!("agy-acp-resume-miss-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };

    let response = adapter.handle_session_resume(json!(11), &json!({"sessionId": "nope"}));
    assert!(response.result.is_none());
    assert_eq!(
        response
            .error
            .as_ref()
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str()),
        Some("unknown sessionId: nope")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn test_session_resume_rejects_empty_session_id() {
    let mut adapter = Adapter::new();
    let response = adapter.handle_session_resume(json!(12), &json!({}));
    assert!(response.result.is_none());
    assert_eq!(
        response
            .error
            .as_ref()
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_i64()),
        Some(-32602)
    );
}

#[test]
fn test_session_resume_accepts_in_memory_session() {
    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: "/tmp".to_string(),
        state_file: PathBuf::from("/tmp/nonexistent-agy-acp-sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    adapter.sessions.insert(
        "sess-memory".to_string(),
        crate::types::Session {
            conversation_id: None,
            last_step_idx: -1,
            model_id: None,
        },
    );

    let response = adapter.handle_session_resume(json!(12), &json!({"sessionId": "sess-memory"}));

    assert!(response.error.is_none());
    assert_eq!(
        response
            .result
            .as_ref()
            .and_then(|r| r.get("sessionId"))
            .and_then(|s| s.as_str()),
        Some("sess-memory")
    );
}

#[test]
fn test_session_load_accepts_in_memory_session_without_replay() {
    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: "/tmp".to_string(),
        state_file: PathBuf::from("/tmp/nonexistent-agy-acp-sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    adapter.sessions.insert(
        "sess-memory-load".to_string(),
        crate::types::Session {
            conversation_id: None,
            last_step_idx: -1,
            model_id: None,
        },
    );

    let output = adapter.handle_session_load(json!(13), &json!({"sessionId": "sess-memory-load"}));

    assert_eq!(output.len(), 1);
    let response: Value = serde_json::from_str(&output[0]).unwrap();
    assert!(response["error"].is_null());
    assert_eq!(response["result"]["sessionId"], "sess-memory-load");
}

#[test]
#[ignore]
fn test_session_resume_does_not_replay_history() {
    let root = std::env::temp_dir().join(format!("agy-acp-resume-noreplay-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    adapter.persist_session("sess-nr", Some("conv-nr"), 10, None);

    let response = adapter.handle_session_resume(json!(13), &json!({"sessionId": "sess-nr"}));
    assert!(response.error.is_none());
    assert_eq!(
        response
            .result
            .as_ref()
            .and_then(|r| r.get("sessionId"))
            .and_then(|s| s.as_str()),
        Some("sess-nr")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
#[ignore]
fn test_persist_and_restore_session() {
    let root = std::env::temp_dir().join(format!("agy-acp-state-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };

    adapter.persist_session("sess-1", Some("conv-abc"), 7, None);
    let restored = adapter.restore_session("sess-1");
    assert_eq!(restored, Some(("conv-abc".to_string(), 7, None)));

    let missing = adapter.restore_session("sess-unknown");
    assert_eq!(missing, None);

    let _ = fs::remove_dir_all(root);
}

fn prepare_auth() -> bool {
    if std::env::var("GEMINI_API_KEY")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        eprintln!("[e2e] Using GEMINI_API_KEY");
        return true;
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let settings = format!("{}/.gemini/antigravity-cli/settings.json", home);
    if std::path::Path::new(&settings).exists() {
        eprintln!("[e2e] Using local auth (keyring)");
        return true;
    }
    eprintln!("SKIP: No GEMINI_API_KEY and no local auth found");
    false
}

#[test]
#[ignore]
fn test_e2e_agy_acp_full_round_trip() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};
    use std::time::Duration;

    if !prepare_auth() {
        return;
    }

    let agy_check = Command::new("agy").arg("--help").output();
    if agy_check.is_err() || !agy_check.unwrap().status.success() {
        eprintln!("SKIP: agy not found in PATH");
        return;
    }

    let binary = std::env::current_dir()
        .unwrap()
        .join("target/release/agy-acp");
    if !binary.exists() {
        panic!("Run `cargo build --release` first");
    }

    let mut child = Command::new(&binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn agy-acp");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    let mut send_and_recv = |msg: &str| -> String {
        writeln!(stdin, "{}", msg).unwrap();
        stdin.flush().unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line
    };

    let resp = send_and_recv(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientName":"e2e","clientVersion":"0.1"}}"#,
    );
    let init: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(init["result"]["protocolVersion"], 1);

    let resp = send_and_recv(r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}"#);
    let session: Value = serde_json::from_str(&resp).unwrap();
    let session_id = session["result"]["sessionId"].as_str().unwrap();
    assert!(!session_id.is_empty());

    let prompt_msg = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{{"sessionId":"{}","prompt":[{{"type":"text","text":"Reply with exactly one word: PONG"}}]}}}}"#,
        session_id
    );
    writeln!(stdin, "{}", prompt_msg).unwrap();
    stdin.flush().unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let mut got_notification = false;
    let mut response_text = String::new();
    loop {
        if std::time::Instant::now() > deadline {
            panic!("Timed out waiting for agy-acp response");
        }
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line.is_empty() {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        let msg: Value = serde_json::from_str(line.trim()).unwrap();
        if msg.get("method") == Some(&json!("session/update")) {
            got_notification = true;
            response_text = msg["params"]["update"]["content"]["text"]
                .as_str()
                .unwrap_or("")
                .to_string();
        }
        if msg.get("id") == Some(&json!(3)) {
            assert!(msg["error"].is_null(), "Got error: {}", msg["error"]);
            assert_eq!(msg["result"]["stopReason"], "end_turn");
            break;
        }
    }

    drop(stdin);
    let _ = child.wait();

    assert!(got_notification, "Expected session/update notification");
    let lower = response_text.to_lowercase();
    assert!(
        lower.contains("pong"),
        "Expected 'PONG' in response, got: '{}'",
        response_text
    );
}

fn spawn_agy_acp() -> Option<(
    std::process::ChildStdin,
    std::io::BufReader<std::process::ChildStdout>,
    std::process::Child,
)> {
    use std::io::BufReader;
    use std::process::{Command, Stdio};

    if !prepare_auth() {
        return None;
    }
    let agy_check = Command::new("agy").arg("--help").output();
    if agy_check.is_err() || !agy_check.unwrap().status.success() {
        eprintln!("SKIP: agy not found in PATH");
        return None;
    }
    let binary = std::env::current_dir()
        .unwrap()
        .join("target/release/agy-acp");
    if !binary.exists() {
        panic!("Run `cargo build --release` first");
    }

    let mut child = Command::new(&binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn agy-acp");
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    Some((stdin, BufReader::new(stdout), child))
}

fn send_recv(
    stdin: &mut std::process::ChildStdin,
    reader: &mut std::io::BufReader<std::process::ChildStdout>,
    msg: &str,
) -> String {
    use std::io::{BufRead, Write};
    writeln!(stdin, "{}", msg).unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

fn send_prompt_wait(
    stdin: &mut std::process::ChildStdin,
    reader: &mut std::io::BufReader<std::process::ChildStdout>,
    id: u64,
    session_id: &str,
    text: &str,
) -> (Option<String>, Value) {
    use std::io::{BufRead, Write};
    use std::time::Duration;

    let msg = format!(
        r#"{{"jsonrpc":"2.0","id":{},"method":"session/prompt","params":{{"sessionId":"{}","prompt":[{{"type":"text","text":"{}"}}]}}}}"#,
        id, session_id, text
    );
    writeln!(stdin, "{}", msg).unwrap();
    stdin.flush().unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let mut notification_text: Option<String> = None;
    loop {
        if std::time::Instant::now() > deadline {
            panic!("Timed out");
        }
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line.is_empty() {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        let msg: Value = serde_json::from_str(line.trim()).unwrap();
        if msg.get("method") == Some(&json!("session/update")) {
            notification_text = msg["params"]["update"]["content"]["text"]
                .as_str()
                .map(String::from);
        }
        if msg.get("id") == Some(&json!(id)) {
            return (notification_text, msg);
        }
    }
}

#[test]
#[ignore]
fn test_e2e_multi_turn() {
    let Some((mut stdin, mut reader, mut child)) = spawn_agy_acp() else {
        return;
    };

    send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientName":"e2e","clientVersion":"0.1"}}"#,
    );

    let resp = send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}"#,
    );
    let session_id = serde_json::from_str::<Value>(&resp).unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let (text1, resp1) = send_prompt_wait(
        &mut stdin,
        &mut reader,
        3,
        &session_id,
        "Remember this word: BANANA. Reply OK.",
    );
    assert!(resp1["error"].is_null(), "Turn 1 error: {}", resp1["error"]);
    assert!(text1.is_some());

    let (text2, resp2) = send_prompt_wait(
        &mut stdin,
        &mut reader,
        4,
        &session_id,
        "What word did I ask you to remember? Reply with just that word.",
    );
    assert!(resp2["error"].is_null(), "Turn 2 error: {}", resp2["error"]);
    let reply = text2.unwrap_or_default().to_lowercase();
    assert!(
        reply.contains("banana"),
        "Expected 'BANANA' in multi-turn reply, got: '{}'",
        reply
    );

    drop(stdin);
    let _ = child.wait();
}

#[test]
#[ignore]
fn test_e2e_session_load() {
    let Some((mut stdin, mut reader, mut child)) = spawn_agy_acp() else {
        return;
    };

    send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientName":"e2e","clientVersion":"0.1"}}"#,
    );
    let resp = send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}"#,
    );
    let session_id = serde_json::from_str::<Value>(&resp).unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let (_text, resp1) = send_prompt_wait(
        &mut stdin,
        &mut reader,
        3,
        &session_id,
        "Reply with exactly: FIRST_TURN",
    );
    assert!(
        resp1["error"].is_null(),
        "First turn error: {}",
        resp1["error"]
    );

    let (text2, resp2) = send_prompt_wait(
        &mut stdin,
        &mut reader,
        4,
        &session_id,
        "Reply with exactly one word: SECOND",
    );
    assert!(
        resp2["error"].is_null(),
        "Second turn error: {}",
        resp2["error"]
    );
    assert!(text2.is_some(), "Expected response on continued session");

    drop(stdin);
    let _ = child.wait();
}

#[test]
#[ignore]
fn test_e2e_error_paths() {
    let Some((mut stdin, mut reader, mut child)) = spawn_agy_acp() else {
        return;
    };

    send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientName":"e2e","clientVersion":"0.1"}}"#,
    );

    let resp = send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"non-existent-session"}}"#,
    );
    let val: Value = serde_json::from_str(&resp).unwrap();
    assert!(
        !val["error"].is_null(),
        "Expected error for unknown session"
    );

    let resp = send_recv(
        &mut stdin,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":3,"method":"bogus/method","params":{}}"#,
    );
    let val: Value = serde_json::from_str(&resp).unwrap();
    assert!(!val["error"].is_null(), "Expected error for unknown method");

    drop(stdin);
    let _ = child.wait();
}

#[test]
fn test_is_narration_true() {
    assert!(Adapter::is_narration("I will fetch the latest commits."));
    assert!(Adapter::is_narration("I'll fetch the latest commits."));
    assert!(Adapter::is_narration("I’ll fetch the latest commits."));
    assert!(Adapter::is_narration(
        "I will fetch the latest commits.\nI'll check the diff."
    ));
    assert!(Adapter::is_narration(
        "I will read the file.\n\nI will analyze the output."
    ));
}

#[test]
fn test_is_narration_false() {
    assert!(!Adapter::is_narration("Here is the result."));
    assert!(!Adapter::is_narration(
        "I will fetch the commits.\nHere is the result."
    ));
    assert!(!Adapter::is_narration(""));
}

#[test]
fn test_filter_narration_drops_all_narration() {
    let parts = vec![
        "I will fetch the latest commits.\nI will check the diff.".to_string(),
        "I will read the file.".to_string(),
        "The fix is confirmed! LGTM ✅".to_string(),
    ];
    let result = filter_narration(&parts);
    assert_eq!(result.as_deref(), Some("The fix is confirmed! LGTM ✅"));
}

#[test]
fn test_filter_narration_preserves_content_after_first_non_narration() {
    let parts = vec![
        "I will check things.".to_string(),
        "Here is my analysis.".to_string(),
        "I will also note this is fine.".to_string(),
    ];
    let result = filter_narration(&parts);
    assert_eq!(result.as_deref(), Some("Here is my analysis."));
}

#[test]
fn test_filter_narration_single_part_unchanged() {
    let parts = vec!["I will do something.".to_string()];
    let result = Adapter::filter_narration(&parts);
    assert_eq!(result, None);
}

#[test]
fn test_filter_narration_all_narration_drops_all() {
    let parts = vec![
        "I will fetch the file.".to_string(),
        "I'll check the output.".to_string(),
        "I will verify the fix.".to_string(),
    ];
    let result = filter_narration(&parts);
    assert_eq!(result, None);
}

#[test]
fn test_session_new_returns_models() {
    let mut adapter = Adapter::new();
    let response = adapter.handle_session_new(json!(1));
    let result = response.result.as_ref().unwrap();
    assert!(result.get("sessionId").is_some());
    let models = result.get("models").unwrap();
    assert!(models.get("currentModelId").is_some());
    assert!(models.get("availableModels").is_some());
    let config_options = result.get("configOptions").unwrap().as_array().unwrap();
    assert_eq!(config_options.len(), 1);
    assert_eq!(config_options[0]["id"].as_str(), Some("model"));
    assert_eq!(config_options[0]["category"].as_str(), Some("model"));
    assert_eq!(config_options[0]["type"].as_str(), Some("select"));
    assert!(config_options[0].get("currentValue").is_some());
    assert!(config_options[0].get("options").is_some());
}

#[test]
fn test_session_set_model() {
    let mut adapter = Adapter::new();
    let new_resp = adapter.handle_session_new(json!(1));
    let session_id = new_resp.result.as_ref().unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let set_resp = adapter.handle_session_set_model(
        json!(2),
        &json!({"sessionId": session_id, "modelId": "Gemini 3.5 Flash (High)"}),
    );
    assert!(set_resp.error.is_none());
    assert_eq!(
        adapter
            .sessions
            .get(&session_id)
            .unwrap()
            .model_id
            .as_deref(),
        Some("Gemini 3.5 Flash (High)")
    );
}

#[test]
fn test_session_set_model_missing_params() {
    let mut adapter = Adapter::new();
    let resp = adapter.handle_session_set_model(json!(1), &json!({}));
    assert!(resp.error.is_some());
    assert_eq!(resp.error.as_ref().unwrap()["code"].as_i64(), Some(-32602));
}

#[test]
fn test_session_set_model_unknown_session() {
    let mut adapter = Adapter::new();
    let resp = adapter.handle_session_set_model(
        json!(1),
        &json!({"sessionId": "nonexistent", "modelId": "some-model"}),
    );
    assert!(resp.error.is_some());
    assert_eq!(resp.error.as_ref().unwrap()["code"].as_i64(), Some(-32000));
}

#[test]
fn test_session_set_config_option_sets_model() {
    let mut adapter = Adapter::new();
    adapter.available_models = vec!["Model A".to_string(), "Model B".to_string()];
    let new_resp = adapter.handle_session_new(json!(1));
    let session_id = new_resp.result.as_ref().unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let set_resp = adapter.handle_session_set_config_option(
        json!(2),
        &json!({"sessionId": session_id, "configId": "model", "value": "Model B"}),
    );

    assert!(set_resp.error.is_none(), "error: {:?}", set_resp.error);
    assert_eq!(
        adapter
            .sessions
            .get(&session_id)
            .unwrap()
            .model_id
            .as_deref(),
        Some("Model B")
    );
    let config_options = set_resp.result.as_ref().unwrap()["configOptions"]
        .as_array()
        .unwrap();
    assert_eq!(config_options[0]["currentValue"].as_str(), Some("Model B"));
}

#[test]
fn test_session_set_config_option_rejects_unknown_config() {
    let mut adapter = Adapter::new();
    let new_resp = adapter.handle_session_new(json!(1));
    let session_id = new_resp.result.as_ref().unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = adapter.handle_session_set_config_option(
        json!(2),
        &json!({"sessionId": session_id, "configId": "not-model", "value": "Model B"}),
    );

    assert!(resp.error.is_some());
    assert_eq!(resp.error.as_ref().unwrap()["code"].as_i64(), Some(-32602));
}

#[test]
#[ignore]
fn test_session_set_model_persists() {
    let root = std::env::temp_dir().join(format!("agy-acp-model-persist-{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&root);

    let mut adapter = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };

    adapter.persist_session("sess-m1", Some("conv-m1"), 0, None);

    adapter.restore_session_state("sess-m1");
    adapter.handle_session_set_model(
        json!(1),
        &json!({"sessionId": "sess-m1", "modelId": "Claude Opus 4.6 (Thinking)"}),
    );

    let adapter2 = Adapter {
        sessions: HashMap::new(),
        working_dir: root.to_string_lossy().to_string(),
        state_file: root.join("sessions.json"),
        available_models: vec![],
        skip_naration: false,
    };
    let restored = adapter2.restore_session("sess-m1");
    assert_eq!(
        restored,
        Some((
            "conv-m1".to_string(),
            0,
            Some("Claude Opus 4.6 (Thinking)".to_string())
        ))
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn test_session_load_returns_models() {
    let mut adapter = Adapter::new();
    adapter.sessions.insert(
        "test-load".to_string(),
        crate::types::Session {
            conversation_id: None,
            last_step_idx: -1,
            model_id: Some("Gemini 3.1 Pro (High)".to_string()),
        },
    );
    adapter.persist_session(
        "test-load",
        Some("conv-load"),
        -1,
        Some("Gemini 3.1 Pro (High)"),
    );
    adapter.sessions.clear();

    let output = adapter.handle_session_load(json!(1), &json!({"sessionId": "test-load"}));
    let response: Value = serde_json::from_str(output.last().unwrap()).unwrap();
    assert!(
        response["error"].is_null(),
        "error: {:?}",
        response["error"]
    );
    let models = response["result"]["models"].as_object().unwrap();
    assert_eq!(
        models["currentModelId"].as_str(),
        Some("Gemini 3.1 Pro (High)")
    );
    assert_eq!(
        response["result"]["configOptions"][0]["currentValue"].as_str(),
        Some("Gemini 3.1 Pro (High)")
    );
}

#[test]
fn test_session_resume_returns_models() {
    let mut adapter = Adapter::new();
    adapter.persist_session(
        "test-resume",
        Some("conv-resume"),
        -1,
        Some("GPT-OSS 120B (Medium)"),
    );
    adapter.sessions.clear();

    let response = adapter.handle_session_resume(json!(1), &json!({"sessionId": "test-resume"}));
    assert!(response.error.is_none(), "error: {:?}", response.error);
    let models = response.result.as_ref().unwrap()["models"]
        .as_object()
        .unwrap();
    assert_eq!(
        models["currentModelId"].as_str(),
        Some("GPT-OSS 120B (Medium)")
    );
    assert_eq!(
        response.result.as_ref().unwrap()["configOptions"][0]["currentValue"].as_str(),
        Some("GPT-OSS 120B (Medium)")
    );
}

#[test]
fn test_session_models_json_default() {
    let mut adapter = Adapter::new();
    let models = adapter.session_models_json(None);
    let current = models["currentModelId"].as_str().unwrap();
    if adapter.available_models.is_empty() {
        assert_eq!(current, "");
    } else {
        assert_eq!(current, adapter.available_models[0]);
    }
}

#[test]
fn test_session_models_json_with_model() {
    let mut adapter = Adapter::new();
    adapter.available_models = vec!["Model A".to_string(), "Model B".to_string()];
    let models = adapter.session_models_json(Some("Model B"));
    assert_eq!(models["currentModelId"].as_str(), Some("Model B"));
    let available = models["availableModels"].as_array().unwrap();
    assert_eq!(available.len(), 2);
    assert_eq!(available[0]["modelId"].as_str(), Some("Model A"));
    assert_eq!(available[1]["modelId"].as_str(), Some("Model B"));
}

#[test]
fn test_session_config_options_json_with_model() {
    let mut adapter = Adapter::new();
    adapter.available_models = vec!["Model A".to_string(), "Model B".to_string()];
    let config_options = adapter.session_config_options_json(Some("Model B"));
    assert_eq!(config_options[0]["id"].as_str(), Some("model"));
    assert_eq!(config_options[0]["category"].as_str(), Some("model"));
    assert_eq!(config_options[0]["type"].as_str(), Some("select"));
    assert_eq!(config_options[0]["currentValue"].as_str(), Some("Model B"));
    let options = config_options[0]["options"].as_array().unwrap();
    assert_eq!(options.len(), 2);
    assert_eq!(options[0]["value"].as_str(), Some("Model A"));
    assert_eq!(options[1]["value"].as_str(), Some("Model B"));
}

#[test]
fn test_parse_available_models_keeps_display_names_only() {
    let stdout = "\
Fetching available models...
gemini-3.6-flash-high\tGemini 3.6 Flash (High)
gemini-3.6-flash-medium\tGemini 3.6 Flash (Medium)
gemini-3.6-flash-low\tGemini 3.6 Flash (Low)
claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)

";
    assert_eq!(
        parse_available_models(stdout),
        vec![
            "Gemini 3.6 Flash (High)",
            "Gemini 3.6 Flash (Medium)",
            "Gemini 3.6 Flash (Low)",
            "Claude Sonnet 4.6 (Thinking)",
        ]
    );
}

#[test]
fn test_parse_available_models_skips_blank_and_status_lines() {
    assert!(parse_available_models("").is_empty());
    assert!(parse_available_models("Fetching available models...\n\n").is_empty());
    assert_eq!(
        parse_available_models("Gemini 3.1 Pro (High)\n"),
        vec!["Gemini 3.1 Pro (High)"]
    );
}
