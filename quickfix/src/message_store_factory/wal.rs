use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Sender},
    thread,
};

use crate::SessionId;

use super::custom::{MessageStoreCallback, MessageStoreFactoryCallback};

// ---------------------------------------------------------------------------
// WAL operations sent to the background writer thread
// ---------------------------------------------------------------------------

enum WalOp {
    /// Append a raw message to the body+header files.
    SetMsg {
        seq_num: u64,
        /// Byte offset in the body file where `msg` should land.
        file_offset: u64,
        msg: Vec<u8>,
    },
    /// Overwrite the seqnums file with the latest sender/target numbers.
    SetSeqNums { sender: u64, target: u64 },
    /// Truncate all WAL files and reinitialise with a new creation time.
    Reset { now: i64 },
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

fn open_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn open_readwrite(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).read(true).write(true).open(path)
}

/// Build per-session file paths from a base directory and session ID.
///
/// Naming mirrors the C++ FileStore:
/// `{base}/{BeginString}-{SenderCompID}-{TargetCompID}[.{qualifier}].{ext}`
fn build_paths(base: &Path, session: &SessionId) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let begin = session.get_begin_string().unwrap_or_default();
    let sender = session.get_sender_comp_id().unwrap_or_default();
    let target = session.get_target_comp_id().unwrap_or_default();
    let qualifier = session.get_session_qualifier().unwrap_or_default();

    let mut stem = format!("{}-{}-{}", begin, sender, target);
    if !qualifier.is_empty() {
        stem.push('-');
        stem.push_str(&qualifier);
    }

    (
        base.join(format!("{}.body", stem)),
        base.join(format!("{}.header", stem)),
        base.join(format!("{}.seqnums", stem)),
        base.join(format!("{}.session", stem)),
    )
}

// ---------------------------------------------------------------------------
// populate_cache — mirrors FileStore::populateCache()
// ---------------------------------------------------------------------------

struct CacheState {
    msgs: HashMap<u64, String>,
    next_sender: u64,
    next_target: u64,
    creation_time: i64,
    /// Current end-of-file offset in the body file (used to compute new offsets
    /// for subsequent `set` operations without re-reading disk).
    body_end: u64,
}

/// Restore in-memory state from the four WAL files.
///
/// File formats (same as C++ FileStore):
/// - `.body`    — raw message bytes, concatenated
/// - `.header`  — space-separated entries `seqnum,offset,size`
/// - `.seqnums` — `sender : target` (u64)
/// - `.session` — Unix seconds as ASCII (creation time)
fn populate_cache(body: &Path, header: &Path, seqnums: &Path, session: &Path) -> CacheState {
    // 1. Read header → build offset index
    let mut offsets: HashMap<u64, (u64, usize)> = HashMap::new();
    if let Ok(content) = fs::read_to_string(header) {
        for entry in content.split_whitespace() {
            let mut parts = entry.splitn(3, ',');
            let parsed = (
                parts.next().and_then(|s| s.parse::<u64>().ok()),
                parts.next().and_then(|s| s.parse::<u64>().ok()),
                parts.next().and_then(|s| s.parse::<usize>().ok()),
            );
            if let (Some(seq), Some(off), Some(sz)) = parsed {
                offsets.insert(seq, (off, sz));
            }
        }
    }

    // 2. Read body → reconstruct messages from offsets
    let mut msgs: HashMap<u64, String> = HashMap::new();
    let body_end = match File::open(body) {
        Ok(mut f) => {
            let mut data = Vec::new();
            let _ = f.read_to_end(&mut data);
            let end = data.len() as u64;
            for (&seq, &(off, sz)) in &offsets {
                let start = off as usize;
                let stop = start + sz;
                if stop <= data.len() {
                    if let Ok(msg) = String::from_utf8(data[start..stop].to_vec()) {
                        msgs.insert(seq, msg);
                    }
                }
            }
            end
        }
        Err(_) => 0,
    };

    // 3. Read seqnums — format: "sender : target"
    let mut next_sender = 1u64;
    let mut next_target = 1u64;
    if let Ok(content) = fs::read_to_string(seqnums) {
        if let Some(pos) = content.find(" : ") {
            let s = content[..pos].trim();
            let t = content[pos + 3..].trim();
            if let (Ok(s), Ok(t)) = (s.parse::<u64>(), t.parse::<u64>()) {
                next_sender = s;
                next_target = t;
            }
        }
    }

    // 4. Read session — format: Unix seconds as ASCII
    let mut creation_time = 0i64;
    if let Ok(content) = fs::read_to_string(session) {
        if let Ok(t) = content.trim().parse::<i64>() {
            creation_time = t;
        }
    }

    CacheState { msgs, next_sender, next_target, creation_time, body_end }
}

// ---------------------------------------------------------------------------
// Background writer thread
// ---------------------------------------------------------------------------

fn run_writer(
    body_path: PathBuf,
    header_path: PathBuf,
    seqnums_path: PathBuf,
    session_path: PathBuf,
    rx: mpsc::Receiver<WalOp>,
) {
    macro_rules! try_open {
        ($fn:expr, $path:expr, $label:expr) => {
            match $fn($path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("WalWriter: {} open error: {}", $label, e);
                    return;
                }
            }
        };
    }

    let mut body_file = try_open!(open_append, &body_path, "body");
    let mut header_file = try_open!(open_append, &header_path, "header");
    let mut seqnums_file = try_open!(open_readwrite, &seqnums_path, "seqnums");
    let mut session_file = try_open!(open_readwrite, &session_path, "session");

    for op in rx {
        match op {
            WalOp::SetMsg { seq_num, file_offset, msg } => {
                if let Err(e) = body_file.write_all(&msg).and_then(|_| body_file.flush()) {
                    eprintln!("WalWriter: body write/flush error: {}", e);
                }
                let header_entry = format!("{},{},{} ", seq_num, file_offset, msg.len());
                if let Err(e) = header_file
                    .write_all(header_entry.as_bytes())
                    .and_then(|_| header_file.flush())
                {
                    eprintln!("WalWriter: header write/flush error: {}", e);
                }
            }

            WalOp::SetSeqNums { sender, target } => {
                let content = format!("{} : {}", sender, target);
                let _ = seqnums_file.seek(SeekFrom::Start(0));
                if let Err(e) = seqnums_file
                    .write_all(content.as_bytes())
                    .and_then(|_| seqnums_file.set_len(content.len() as u64))
                    .and_then(|_| seqnums_file.flush())
                {
                    eprintln!("WalWriter: seqnums write error: {}", e);
                }
            }

            WalOp::Reset { now } => {
                // Drop handles so truncation succeeds on all platforms
                drop(body_file);
                drop(header_file);

                if let Err(e) = File::create(&body_path).and(File::create(&header_path)) {
                    eprintln!("WalWriter: reset truncate error: {}", e);
                    return;
                }

                body_file = try_open!(open_append, &body_path, "body (reset)");
                header_file = try_open!(open_append, &header_path, "header (reset)");

                // Reset seqnums to "1 : 1"
                let init = "1 : 1";
                let _ = seqnums_file.seek(SeekFrom::Start(0));
                let _ = seqnums_file.write_all(init.as_bytes());
                let _ = seqnums_file.set_len(init.len() as u64);
                let _ = seqnums_file.flush();

                // Rewrite session file
                let ts = now.to_string();
                let _ = session_file.seek(SeekFrom::Start(0));
                let _ = session_file.write_all(ts.as_bytes());
                let _ = session_file.set_len(ts.len() as u64);
                let _ = session_file.flush();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WalStore
// ---------------------------------------------------------------------------

/// Per-session WAL message store.
///
/// Reads are served from an in-memory cache. Writes are applied to the cache
/// immediately and dispatched asynchronously to a background writer thread
/// that flushes them to disk in the same format as the C++ `FileStore`.
///
/// Call [`refresh`][WalStore::refresh] to reload the cache from the WAL files
/// (e.g. after a process restart).
pub struct WalStore {
    msgs: HashMap<u64, String>,
    next_sender: u64,
    next_target: u64,
    creation_time: i64,
    /// Tracks the next write offset in the body file without a syscall.
    body_end: u64,

    body_path: PathBuf,
    header_path: PathBuf,
    seqnums_path: PathBuf,
    session_path: PathBuf,

    /// None after Drop begins (channel closed, writer exits).
    tx: Option<Sender<WalOp>>,
    writer_handle: Option<thread::JoinHandle<()>>,
}

impl WalStore {
    fn init(
        body_path: PathBuf,
        header_path: PathBuf,
        seqnums_path: PathBuf,
        session_path: PathBuf,
        now: i64,
    ) -> Self {
        let state = populate_cache(&body_path, &header_path, &seqnums_path, &session_path);

        // If no session file existed yet, seed it with the provided timestamp
        let creation_time = if state.creation_time != 0 {
            state.creation_time
        } else {
            let _ = fs::write(&session_path, now.to_string());
            now
        };

        // If no seqnums file existed, seed with "1 : 1"
        if !seqnums_path.exists() {
            let _ = fs::write(&seqnums_path, "1 : 1");
        }

        let (tx, rx) = mpsc::channel();
        let (bp, hp, sp, ssp) = (
            body_path.clone(),
            header_path.clone(),
            seqnums_path.clone(),
            session_path.clone(),
        );
        let handle = thread::Builder::new()
            .name("wal-writer".to_owned())
            .spawn(move || run_writer(bp, hp, sp, ssp, rx))
            .expect("WalStore: failed to spawn writer thread");

        Self {
            msgs: state.msgs,
            next_sender: state.next_sender,
            next_target: state.next_target,
            creation_time,
            body_end: state.body_end,
            body_path,
            header_path,
            seqnums_path,
            session_path,
            tx: Some(tx),
            writer_handle: Some(handle),
        }
    }

    fn send(&self, op: WalOp) {
        if let Some(tx) = &self.tx {
            if let Err(e) = tx.send(op) {
                eprintln!("WalStore: channel send error: {}", e);
            }
        }
    }
}

impl Drop for WalStore {
    fn drop(&mut self) {
        // Closing the Sender causes the writer's for-loop to exit.
        self.tx = None;
        // Join to ensure all pending writes are flushed before the store is gone.
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.join();
        }
    }
}

impl MessageStoreCallback for WalStore {
    fn set(&mut self, seq_num: u64, msg: &str) -> bool {
        let msg_bytes = msg.as_bytes().to_vec();
        let offset = self.body_end;
        self.body_end += msg_bytes.len() as u64;
        self.msgs.insert(seq_num, msg.to_owned());
        self.send(WalOp::SetMsg { seq_num, file_offset: offset, msg: msg_bytes });
        true
    }

    fn get(&self, begin_seq_num: u64, end_seq_num: u64, msgs: &mut Vec<String>) {
        for seq in begin_seq_num..=end_seq_num {
            if let Some(msg) = self.msgs.get(&seq) {
                msgs.push(msg.clone());
            }
        }
    }

    fn next_sender_seq_num(&self) -> u64 {
        self.next_sender
    }

    fn next_target_seq_num(&self) -> u64 {
        self.next_target
    }

    fn set_next_sender_seq_num(&mut self, value: u64) {
        self.next_sender = value;
        self.send(WalOp::SetSeqNums { sender: self.next_sender, target: self.next_target });
    }

    fn set_next_target_seq_num(&mut self, value: u64) {
        self.next_target = value;
        self.send(WalOp::SetSeqNums { sender: self.next_sender, target: self.next_target });
    }

    fn incr_next_sender_seq_num(&mut self) {
        self.next_sender += 1;
        self.send(WalOp::SetSeqNums { sender: self.next_sender, target: self.next_target });
    }

    fn incr_next_target_seq_num(&mut self) {
        self.next_target += 1;
        self.send(WalOp::SetSeqNums { sender: self.next_sender, target: self.next_target });
    }

    fn creation_time(&self) -> i64 {
        self.creation_time
    }

    fn reset(&mut self, now: i64) {
        self.msgs.clear();
        self.next_sender = 1;
        self.next_target = 1;
        self.creation_time = now;
        self.body_end = 0;
        self.send(WalOp::Reset { now });
    }

    /// Reload in-memory cache from the WAL files on disk.
    ///
    /// Mirrors `FileStore::populateCache()`: reads the header index, body
    /// messages, sequence numbers, and creation timestamp from the four WAL
    /// files. Any pending async writes that have not yet been flushed to disk
    /// are not visible until the writer thread catches up.
    fn refresh(&mut self) {
        let state = populate_cache(
            &self.body_path,
            &self.header_path,
            &self.seqnums_path,
            &self.session_path,
        );
        self.msgs = state.msgs;
        self.next_sender = state.next_sender;
        self.next_target = state.next_target;
        self.creation_time = state.creation_time;
        self.body_end = state.body_end;
    }
}

// ---------------------------------------------------------------------------
// WalStoreFactory
// ---------------------------------------------------------------------------

/// Factory that creates [`WalStore`] instances for each session.
///
/// Each session gets its own set of four WAL files under `base_path`,
/// named after the session's BeginString, SenderCompID, and TargetCompID.
pub struct WalStoreFactory {
    base_path: PathBuf,
}

impl WalStoreFactory {
    /// Create a new factory that stores WAL files under `base_path`.
    /// The directory is created if it does not exist.
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self { base_path: base_path.into() }
    }
}

impl MessageStoreFactoryCallback for WalStoreFactory {
    type Store = WalStore;

    fn create(&self, session: &SessionId, now: i64) -> WalStore {
        if let Err(e) = fs::create_dir_all(&self.base_path) {
            eprintln!("WalStoreFactory: cannot create base_path: {}", e);
        }
        let (body, header, seqnums, session_file) = build_paths(&self.base_path, session);
        WalStore::init(body, header, seqnums, session_file, now)
    }
}
