mod adapter;
mod db;
mod protobuf;
mod streaming;
mod types;

#[cfg(test)]
mod tests;

use serde_json::json;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc;

use adapter::Adapter;
use clap::Parser;
use types::{JsonRpcRequest, JsonRpcResponse};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Skip pure narration messages from agy, such as "I will ...".
    #[arg(long = "skip-naration", default_value_t = false)]
    skip_naration: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let adapter = if cli.skip_naration {
        Adapter::new_with_skip_naration(true)
    } else {
        Adapter::new()
    };
    let adapter = Arc::new(tokio::sync::Mutex::new(adapter));
    let active_cancellations: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Option<String>>();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) if !l.trim().is_empty() => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    let mut stdout = io::stdout();
    let mut stdin_open = true;
    let mut pending_prompts = 0usize;

    loop {
        if !stdin_open && pending_prompts == 0 {
            break;
        }

        let line = if stdin_open {
            tokio::select! {
                output = out_rx.recv() => {
                    match output {
                        Some(Some(line)) => {
                            let _ = writeln!(stdout, "{}", line);
                            let _ = stdout.flush();
                        }
                        Some(None) => pending_prompts = pending_prompts.saturating_sub(1),
                        None => {}
                    }
                    continue;
                }
                input = rx.recv() => {
                    match input {
                        Some(line) => line,
                        None => {
                            stdin_open = false;
                            continue;
                        }
                    }
                }
            }
        } else {
            match out_rx.recv().await {
                Some(Some(line)) => {
                    let _ = writeln!(stdout, "{}", line);
                    let _ = stdout.flush();
                }
                Some(None) => pending_prompts = pending_prompts.saturating_sub(1),
                None => break,
            }
            continue;
        };

        while let Ok(output) = out_rx.try_recv() {
            match output {
                Some(line) => {
                    let _ = writeln!(stdout, "{}", line);
                    let _ = stdout.flush();
                }
                None => pending_prompts = pending_prompts.saturating_sub(1),
            }
        }

        let req: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let id = match req.id {
            Some(id) => id,
            None => {
                if req.method.as_deref() == Some("session/cancel") {
                    let params = req.params.unwrap_or(json!({}));
                    if let Some(session_id) = params.get("sessionId").and_then(|v| v.as_str()) {
                        if let Some(cancelled) = active_cancellations
                            .lock()
                            .unwrap()
                            .get(session_id)
                            .cloned()
                        {
                            cancelled.store(true, Ordering::SeqCst);
                        }
                    }
                }
                continue;
            }
        };

        let output = match req.method.as_deref() {
            Some("initialize") => {
                let adapter = adapter.lock().await;
                vec![serde_json::to_string(&adapter.handle_initialize(id)).unwrap()]
            }
            Some("session/new") => {
                let params = req.params.unwrap_or(json!({}));
                let mut adapter = adapter.lock().await;
                vec![serde_json::to_string(&adapter.handle_session_new(id, &params)).unwrap()]
            }
            Some("session/load") => {
                let params = req.params.unwrap_or(json!({}));
                let mut adapter = adapter.lock().await;
                adapter.handle_session_load(id, &params)
            }
            Some("session/resume") => {
                let params = req.params.unwrap_or(json!({}));
                let mut adapter = adapter.lock().await;
                vec![serde_json::to_string(&adapter.handle_session_resume(id, &params)).unwrap()]
            }
            Some("session/prompt") => {
                let params = req.params.unwrap_or(json!({}));
                let session_id = params
                    .get("sessionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let cancelled = Arc::new(AtomicBool::new(false));
                if !session_id.is_empty() {
                    active_cancellations
                        .lock()
                        .unwrap()
                        .insert(session_id.clone(), Arc::clone(&cancelled));
                }
                let adapter = Arc::clone(&adapter);
                let active_cancellations = Arc::clone(&active_cancellations);
                let out_tx = out_tx.clone();
                pending_prompts += 1;
                tokio::spawn(async move {
                    let output = {
                        let mut adapter = adapter.lock().await;
                        adapter.handle_session_prompt(id, &params, cancelled).await
                    };
                    if !session_id.is_empty() {
                        active_cancellations.lock().unwrap().remove(&session_id);
                    }
                    for line in output {
                        let _ = out_tx.send(Some(line));
                    }
                    let _ = out_tx.send(None);
                });
                Vec::new()
            }
            Some("session/cancel") => {
                let params = req.params.unwrap_or(json!({}));
                if let Some(session_id) = params.get("sessionId").and_then(|v| v.as_str()) {
                    if let Some(cancelled) = active_cancellations
                        .lock()
                        .unwrap()
                        .get(session_id)
                        .cloned()
                    {
                        cancelled.store(true, Ordering::SeqCst);
                    }
                }
                let r = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: Some(json!({})),
                    error: None,
                };
                vec![serde_json::to_string(&r).unwrap()]
            }
            Some("session/set_model") | Some("session/setModel") => {
                let params = req.params.unwrap_or(json!({}));
                let mut adapter = adapter.lock().await;
                vec![serde_json::to_string(&adapter.handle_session_set_model(id, &params)).unwrap()]
            }
            Some("session/set_config_option") | Some("session/setConfigOption") => {
                let params = req.params.unwrap_or(json!({}));
                let mut adapter = adapter.lock().await;
                vec![
                    serde_json::to_string(&adapter.handle_session_set_config_option(id, &params))
                        .unwrap(),
                ]
            }
            Some(method) => {
                let r = JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(
                        json!({"code":-32601,"message":format!("method not found: {method}")}),
                    ),
                };
                vec![serde_json::to_string(&r).unwrap()]
            }
            None => continue,
        };

        for line in output {
            let _ = writeln!(stdout, "{}", line);
        }
        let _ = stdout.flush();
    }
}
