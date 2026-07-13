use std::collections::HashMap;

/// Cap on how many bytes to scan from a single log's appended region. agy logs
/// errors at the tail, so if a turn wrote a very large burst (debug logging) we
/// scan only the newest tail slice, bounding the allocation instead of reading
/// the whole file with `read_to_string`.
const MAX_LOG_SCAN_BYTES: u64 = 256 * 1024;

/// Decide whether this turn's outcome should be surfaced as a JSON-RPC error
/// instead of falling through to the normal `end_turn`/`error` `stopReason`
/// result. `status_display` is the exit status already formatted as a string
/// (e.g. `status.to_string()`); `swallowed_error` is the result of
/// `detect_swallowed_agy_error` for the zero-exit-but-empty-output case.
/// Returns `Some((code, message))` when an error should be surfaced.
pub fn decide_turn_error(
    was_cancelled: bool,
    status_success: bool,
    had_updates: bool,
    status_display: &str,
    stderr_text: &str,
    swallowed_error: Option<&str>,
) -> Option<(i32, String)> {
    if was_cancelled || had_updates {
        return None;
    }
    if !status_success {
        let msg = if stderr_text.is_empty() {
            format!("agy exited with status: {status_display}")
        } else {
            format!("agy failed: {}", stderr_text.trim_end())
        };
        return Some((-32000, msg));
    }
    swallowed_error.map(|details| (-32603, details.to_string()))
}

/// Match predicate for agy's `cli-*.log` files, shared by `snapshot_agy_logs` and
/// `detect_swallowed_agy_error` so the naming convention lives in one place.
fn is_agy_cli_log(name: &str) -> bool {
    name.starts_with("cli-") && name.ends_with(".log")
}

/// Snapshot agy's current `cli-*.log` files as `name -> byte length` under
/// `<conversations_dir>/../log`. Recording each file's pre-turn size (not just
/// its name) lets `detect_swallowed_agy_error` scan only the bytes appended
/// *during* this turn. This narrows, but does not fully eliminate, the risk of
/// attributing another turn's or a concurrent session's error to this turn —
/// see `detect_swallowed_agy_error`'s doc for the residual limitation.
pub fn snapshot_agy_logs(conversations_dir: &std::path::Path) -> HashMap<String, u64> {
    let Some(log_dir) = conversations_dir.parent().map(|p| p.join("log")) else {
        return HashMap::new();
    };
    let entries = match std::fs::read_dir(&log_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(e) => {
            eprintln!(
                "[agy-acp] cannot read agy log dir {}: {e}; swallowed-error detection disabled this turn",
                log_dir.display()
            );
            return HashMap::new();
        }
    };
    let mut dir_entry_errors = 0u32;
    let snapshot: HashMap<String, u64> = entries
        .filter_map(|e| {
            e.inspect_err(|_| dir_entry_errors += 1).ok()
        })
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            if !is_agy_cli_log(&name) {
                return None;
            }
            match e.metadata() {
                Ok(meta) => Some((name, meta.len())),
                Err(e) => {
                    eprintln!("[agy-acp] cannot stat agy log {name}: {e}; it will be treated as new next turn");
                    None
                }
            }
        })
        .collect();
    if dir_entry_errors > 0 {
        eprintln!("[agy-acp] {dir_entry_errors} entr(y/ies) in agy log dir {} could not be read while snapshotting", log_dir.display());
    }
    snapshot
}

/// Scan the `cli-*.log` files agy appended to during this turn for a backend
/// error it swallowed. `agy --print` exits 0 with empty stdout/stderr when the
/// model backend fails (e.g. quota 429 / RESOURCE_EXHAUSTED), recording the
/// cause only in its own cli.log. A candidate must have grown past its
/// `pre_snapshot` size *and* been modified no more than 1s before `spawn_time`
/// (when this turn's own agy child was spawned) — the 1s tolerance absorbs
/// filesystems that truncate mtime to whole seconds, which could otherwise
/// make this turn's own log look stale by a few hundred ms and be wrongly
/// excluded. Every candidate that grew is scanned (newest first, no arbitrary
/// cap) so a genuinely empty turn's own log is never skipped in favor of
/// another candidate.
///
/// This narrows the window for a *reused* log file (bytes written before this
/// turn's snapshot are excluded) but does **not** fully isolate a *concurrent*
/// `agy-acp` session: the log directory is shared by every running session,
/// and agy's log filenames carry no PID/session correlation we can key on. A
/// concurrent session's own agy child writing a brand-new log at or after this
/// turn's `spawn_time` can still be scanned and, if it matches a known anchor,
/// misattributed to this turn. Fully closing this would require per-invocation
/// log isolation from agy itself, which is not available (agy is closed-source).
pub fn detect_swallowed_agy_error(
    conversations_dir: &std::path::Path,
    pre_snapshot: &HashMap<String, u64>,
    spawn_time: std::time::SystemTime,
) -> Option<String> {
    let log_dir = conversations_dir.parent()?.join("log");
    let entries = match std::fs::read_dir(&log_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            eprintln!(
                "[agy-acp] cannot read agy log dir {}: {e}; swallowed-error detection skipped this turn",
                log_dir.display()
            );
            return None;
        }
    };

    // Only logs that grew this turn (new file, or larger than the pre-turn
    // snapshot) and were modified at/after this turn's own agy child was
    // spawned; `offset` is where this turn's bytes begin, `len` its current size.
    let mut dir_entry_errors = 0u32;
    let mut candidates: Vec<(std::time::SystemTime, std::path::PathBuf, u64, u64)> = entries
        .filter_map(|e| {
            e.inspect_err(|_| dir_entry_errors += 1).ok()
        })
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            if !is_agy_cli_log(&name) {
                return None;
            }
            let meta = match e.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    eprintln!("[agy-acp] cannot stat agy log {name}: {err}; excluded from this turn's swallowed-error scan");
                    return None;
                }
            };
            let offset = pre_snapshot.get(&name).copied().unwrap_or(0);
            if meta.len() <= offset {
                return None; // nothing appended this turn
            }
            let mtime = match meta.modified() {
                Ok(mtime) => mtime,
                Err(err) => {
                    eprintln!("[agy-acp] cannot read mtime of agy log {name}: {err}; excluded from this turn's swallowed-error scan");
                    return None;
                }
            };
            // Tolerate up to 1s of clock/filesystem imprecision: some filesystems
            // truncate mtime to whole seconds, which can make this turn's own
            // log (written a few hundred ms after spawn_time) appear to predate
            // it. A false negative here (excluding this turn's own error) is
            // worse than the tradeoff of a slightly wider window for the
            // already-acknowledged concurrent-session risk above, so we only
            // exclude a candidate that is unambiguously more than 1s stale.
            if mtime + std::time::Duration::from_secs(1) < spawn_time {
                return None; // grew well before this turn's own agy child was spawned
            }
            Some((mtime, e.path(), offset, meta.len()))
        })
        .collect();
    if dir_entry_errors > 0 {
        eprintln!("[agy-acp] {dir_entry_errors} entr(y/ies) in agy log dir {} could not be read during scan", log_dir.display());
    }

    candidates.sort_by_key(|(mtime, _, _, _)| std::cmp::Reverse(*mtime)); // newest first

    let scanned = candidates.len();
    let mut read_failures = 0u32;
    let found = candidates.iter().find_map(|(_, path, offset, len)| {
        match read_log_tail(path, *offset, *len) {
            Some(content) => extract_agy_error_message(&content),
            None => {
                read_failures += 1;
                None
            }
        }
    });

    if found.is_none() && read_failures > 0 {
        eprintln!(
            "[agy-acp] swallowed-error scan: {read_failures}/{scanned} grown log(s) could not be read; detection may have missed this turn's error"
        );
    } else if found.is_none() && scanned > 0 {
        eprintln!(
            "[agy-acp] swallowed-error scan: no known error anchor in {scanned} grown log(s)"
        );
    }
    found
}

fn read_log_tail(path: &std::path::Path, offset: u64, len: u64) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[agy-acp] cannot open agy log {}: {e}", path.display());
            return None;
        }
    };
    let start = offset.max(len.saturating_sub(MAX_LOG_SCAN_BYTES));
    if let Err(e) = file.seek(SeekFrom::Start(start)) {
        eprintln!("[agy-acp] cannot seek agy log {}: {e}", path.display());
        return None;
    }
    let mut buf = Vec::new();
    if let Err(e) = file.take(MAX_LOG_SCAN_BYTES).read_to_end(&mut buf) {
        eprintln!("[agy-acp] cannot read agy log {}: {e}", path.display());
        return None;
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// Truncate `s` to at most `max` bytes, snapping back to the nearest char
/// boundary so a multi-byte UTF-8 char is never split (which would panic).
fn truncate_to_byte_boundary(s: &mut String, max: usize) {
    if s.len() <= max {
        return;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

/// Extract a clean, single-line error message from agy's cli.log content. agy
/// logs errors via glog (`E0707 08:34:23.910604  84 log.go:398] <msg>`) and
/// self-wraps them (`<msg>.: <msg>`); this returns the most specific terminal
/// error, de-wrapped and byte-length-capped on a char boundary.
fn extract_agy_error_message(content: &str) -> Option<String> {
    // Most specific terminal error first.
    const ANCHORS: [&str; 3] = [
        "agent executor error:",
        "model unreachable:",
        "RESOURCE_EXHAUSTED",
    ];
    for anchor in ANCHORS {
        // The last matching line is the terminal failure (retries log the same anchor).
        if let Some(line) = content.lines().rev().find(|l| l.contains(anchor)) {
            let start = line.find(anchor)?;
            let mut msg = line[start..].trim().to_string();
            // Drop glog's self-wrapped duplicate tail ("<msg>.: <msg>").
            if let Some((first, _)) = msg.split_once(".: ") {
                msg = format!("{}.", first);
            }
            truncate_to_byte_boundary(&mut msg, 500);
            return Some(msg);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use uuid::Uuid;

    /// RAII guard for a temp-dir test fixture: removes the directory on drop.
    struct TempDirGuard(PathBuf);
    impl AsRef<Path> for TempDirGuard {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const QUOTA_LOG: &str = "\
I0707 08:34:18.847769  84 http_helpers.go:208] URL: .../streamGenerateContent?alt=sse\n\
I0707 08:34:20.615268  84 log.go:398] RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 40h52m50s.: RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 40h52m50s.\n\
E0707 08:34:23.910604  84 log.go:398] agent executor error: model unreachable: RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 40h52m46s.: RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 40h52m46s.\n";

    #[test]
    fn test_extract_agy_error_message_dewraps_quota_error() {
        let msg = extract_agy_error_message(QUOTA_LOG).expect("should detect quota error");
        assert!(msg.starts_with("agent executor error:"), "got: {msg}");
        assert!(msg.contains("Individual quota reached"), "got: {msg}");
        assert!(msg.contains("Resets in 40h52m46s"), "got: {msg}");
        assert!(!msg.contains(".: "), "duplicate tail not stripped: {msg}");
    }

    #[test]
    fn test_extract_agy_error_message_none_for_clean_log() {
        let clean =
            "I0707 08:34:15.727406  84 printmode.go:225] Print mode: silent auth succeeded\n\
                     I0707 08:34:15.871543  84 server.go:825] Created conversation abc\n";
        assert_eq!(extract_agy_error_message(clean), None);
    }

    #[test]
    fn test_extract_agy_error_message_truncates_on_char_boundary() {
        let line = format!(
            "E0707 08:34:23.910604  84 log.go:398] RESOURCE_EXHAUSTED {}",
            "é".repeat(400)
        );
        let msg = extract_agy_error_message(&line).expect("should detect anchored error");
        assert!(msg.starts_with("RESOURCE_EXHAUSTED"), "got: {msg}");
        assert!(msg.len() <= 500, "not capped: {} bytes", msg.len());
        assert!(std::str::from_utf8(msg.as_bytes()).is_ok());
    }

    #[test]
    fn test_detect_swallowed_agy_error_reads_new_turn_log() {
        let root =
            TempDirGuard(std::env::temp_dir().join(format!("agy-acp-logscan-{}-", Uuid::new_v4())));
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        let spawn_time = std::time::SystemTime::now();
        fs::write(log_dir.join("cli-20260707_083407.log"), QUOTA_LOG).unwrap();

        let empty = HashMap::new();
        let detected = detect_swallowed_agy_error(&conversations, &empty, spawn_time);
        assert!(detected.is_some(), "should detect error in fresh log");
        assert!(detected.unwrap().contains("Individual quota reached"));
    }

    #[test]
    fn test_detect_swallowed_agy_error_none_when_no_logs() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-empty-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        fs::create_dir_all(&conversations).unwrap();
        let empty = HashMap::new();
        assert_eq!(
            detect_swallowed_agy_error(&conversations, &empty, std::time::SystemTime::now()),
            None
        );
    }

    #[test]
    fn test_detect_swallowed_agy_error_ignores_pre_existing_error() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-stale-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        let log_path = log_dir.join("cli-20260707_083407.log");
        fs::write(&log_path, QUOTA_LOG).unwrap();

        let snapshot = snapshot_agy_logs(&conversations);
        let spawn_time = std::time::SystemTime::now();

        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        use std::io::Write as _;
        f.write_all(b"I0707 09:00:00.000000  84 server.go:825] turn ok\n")
            .unwrap();
        drop(f);

        assert_eq!(
            detect_swallowed_agy_error(&conversations, &snapshot, spawn_time),
            None,
            "pre-existing error before the snapshot offset must not be surfaced"
        );
    }

    #[test]
    fn test_detect_swallowed_agy_error_reads_only_appended_bytes() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-append-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        let log_path = log_dir.join("cli-20260707_083407.log");
        fs::write(
            &log_path,
            "I0707 08:00:00.000000  84 server.go:825] Created conversation abc\n",
        )
        .unwrap();

        let snapshot = snapshot_agy_logs(&conversations);
        let spawn_time = std::time::SystemTime::now();

        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        use std::io::Write as _;
        f.write_all(QUOTA_LOG.as_bytes()).unwrap();
        drop(f);

        let detected = detect_swallowed_agy_error(&conversations, &snapshot, spawn_time);
        assert!(detected.is_some(), "should detect error appended this turn");
        assert!(detected.unwrap().contains("Individual quota reached"));
    }

    #[test]
    fn test_detect_swallowed_agy_error_excludes_log_grown_before_spawn_time() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-concurrent-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();

        let empty_snapshot = snapshot_agy_logs(&conversations);

        fs::write(log_dir.join("cli-20260707_083000.log"), QUOTA_LOG).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let spawn_time = std::time::SystemTime::now();
        std::thread::sleep(std::time::Duration::from_millis(1100));

        fs::write(
            log_dir.join("cli-20260707_083500.log"),
            "I0707 08:35:00.000000  84 server.go:825] turn ok\n",
        )
        .unwrap();

        let detected = detect_swallowed_agy_error(&conversations, &empty_snapshot, spawn_time);
        assert_eq!(
            detected, None,
            "a log that finished growing before this turn's spawn_time must not be surfaced"
        );
    }

    #[test]
    fn test_detect_swallowed_agy_error_tolerates_mtime_just_before_spawn_time() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-tolerance-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        let empty = HashMap::new();

        fs::write(log_dir.join("cli-20260707_083407.log"), QUOTA_LOG).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        let spawn_time = std::time::SystemTime::now();

        let detected = detect_swallowed_agy_error(&conversations, &empty, spawn_time);
        assert!(
            detected.is_some(),
            "a log within the 1s tolerance window must still be surfaced, not excluded as stale"
        );
    }

    #[test]
    fn test_detect_swallowed_agy_error_scans_all_grown_candidates_regardless_of_position() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logscan-multi-{}-", Uuid::new_v4())),
        );
        let conversations = root.as_ref().join("conversations");
        let log_dir = root.as_ref().join("log");
        fs::create_dir_all(&conversations).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        let empty = HashMap::new();
        let spawn_time = std::time::SystemTime::now();

        fs::write(log_dir.join("cli-a-oldest.log"), QUOTA_LOG).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        for (i, name) in ["cli-b.log", "cli-c.log", "cli-d-newest.log"]
            .iter()
            .enumerate()
        {
            fs::write(
                log_dir.join(name),
                format!("I0707 08:3{i}:00.000000  84 server.go:825] turn ok\n"),
            )
            .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(1100));
        }

        let detected = detect_swallowed_agy_error(&conversations, &empty, spawn_time);
        assert!(
            detected.is_some(),
            "error in the oldest of 4 grown logs must still be found; there is no cap on candidates scanned"
        );
    }

    #[test]
    fn test_read_log_tail_starts_at_offset_when_offset_is_more_restrictive() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logtail-offset-{}-", Uuid::new_v4())),
        );
        fs::create_dir_all(root.as_ref()).unwrap();
        let path = root.as_ref().join("small.log");
        let earlier_turn_prefix = "I0707 08:00:00.000000  84 server.go:825] earlier turn content\n";
        let content = format!("{earlier_turn_prefix}{QUOTA_LOG}");
        fs::write(&path, &content).unwrap();
        let len = content.len() as u64;
        let offset = earlier_turn_prefix.len() as u64;
        assert!(len < MAX_LOG_SCAN_BYTES, "fixture must stay under the cap");

        let tail = read_log_tail(&path, offset, len).expect("should read tail");
        assert!(
            !tail.contains("earlier turn content"),
            "must not include bytes before offset: {tail}"
        );
        assert!(
            tail.starts_with("I0707 08:34"),
            "must start exactly at offset, got: {tail}"
        );
    }

    #[test]
    fn test_read_log_tail_caps_read_when_offset_is_less_restrictive() {
        let root = TempDirGuard(
            std::env::temp_dir().join(format!("agy-acp-logtail-cap-{}-", Uuid::new_v4())),
        );
        fs::create_dir_all(root.as_ref()).unwrap();
        let path = root.as_ref().join("big.log");
        let prefix = "x".repeat((MAX_LOG_SCAN_BYTES as usize) + 1000);
        let content = format!("{prefix}{QUOTA_LOG}");
        fs::write(&path, &content).unwrap();
        let len = content.len() as u64;

        let tail = read_log_tail(&path, 0, len).expect("should read tail");
        assert!(
            tail.contains("Individual quota reached"),
            "should reach the error past the filler prefix"
        );
        assert!(
            tail.len() as u64 <= MAX_LOG_SCAN_BYTES,
            "must not exceed the cap"
        );
    }

    #[test]
    fn test_decide_turn_error_cancelled_never_surfaces() {
        assert_eq!(
            decide_turn_error(true, false, false, "exit 1", "boom", Some("swallowed")),
            None
        );
    }

    #[test]
    fn test_decide_turn_error_had_updates_never_surfaces() {
        assert_eq!(
            decide_turn_error(false, false, true, "exit 1", "boom", Some("swallowed")),
            None
        );
    }

    #[test]
    fn test_decide_turn_error_nonzero_exit_uses_stderr() {
        let (code, msg) =
            decide_turn_error(false, false, false, "exit status: 1", "boom", None).unwrap();
        assert_eq!(code, -32000);
        assert!(msg.contains("boom"));
    }

    #[test]
    fn test_decide_turn_error_nonzero_exit_falls_back_to_status_when_stderr_empty() {
        let (code, msg) =
            decide_turn_error(false, false, false, "exit status: 1", "", None).unwrap();
        assert_eq!(code, -32000);
        assert!(msg.contains("exit status: 1"));
    }

    #[test]
    fn test_decide_turn_error_success_with_swallowed_error_surfaces_32603() {
        let (code, msg) = decide_turn_error(
            false,
            true,
            false,
            "exit status: 0",
            "",
            Some("quota exhausted"),
        )
        .unwrap();
        assert_eq!(code, -32603);
        assert_eq!(msg, "quota exhausted");
    }

    #[test]
    fn test_decide_turn_error_success_no_swallowed_error_falls_through() {
        assert_eq!(
            decide_turn_error(false, true, false, "exit status: 0", "", None),
            None
        );
    }
}
