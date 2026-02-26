//! Append-Only File (AOF) persistence implementation
//!
//! The AOF module provides write-ahead logging for durability.
//! Commands are serialized in RESP format and written to a file.
//! On startup, commands are replayed to rebuild the in-memory state.

use crate::error::{PersistenceError, PersistenceResult};
use crate::fsync::fsync_file;
use bytes::{Bytes, BytesMut};
use ferris_protocol::RespValue;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// AOF fsync modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// Fsync after every write (safest, slowest)
    Always,
    /// Fsync every second (good balance)
    EverySecond,
    /// Never fsync, let OS handle it (fastest, least safe)
    No,
}

/// AOF configuration
#[derive(Debug, Clone)]
pub struct AofConfig {
    /// Path to the AOF file
    pub file_path: PathBuf,
    /// Fsync mode
    pub fsync_mode: FsyncMode,
    /// Buffer size for writing (bytes)
    pub buffer_size: usize,
    /// Channel capacity for async writes
    pub channel_capacity: usize,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("ferris.aof"),
            fsync_mode: FsyncMode::EverySecond,
            buffer_size: 64 * 1024, // 64KB
            channel_capacity: 1024,
        }
    }
}

/// Entry to be written to AOF
#[derive(Debug, Clone)]
pub struct AofEntry {
    /// The command as a RESP array
    pub command: Vec<RespValue>,
    /// Database number (for SELECT commands)
    pub db: usize,
}

/// AOF writer with async channel and background task
#[derive(Debug)]
pub struct AofWriter {
    /// Sender for AOF entries
    tx: mpsc::Sender<AofEntry>,
    /// Background writer task handle
    task_handle: Option<JoinHandle<PersistenceResult<()>>>,
    /// Configuration
    config: Arc<AofConfig>,
}

impl AofWriter {
    /// Create a new AOF writer
    ///
    /// This starts a background task that writes entries to the AOF file.
    pub fn new(config: AofConfig) -> PersistenceResult<Self> {
        let config = Arc::new(config);
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        // Start background writer task
        let task_config = Arc::clone(&config);
        let task_handle = tokio::spawn(async move { aof_writer_task(rx, task_config).await });

        info!(path = %config.file_path.display(), "AOF writer started");

        Ok(Self {
            tx,
            task_handle: Some(task_handle),
            config,
        })
    }

    /// Append a command to the AOF
    ///
    /// This is async and non-blocking. The command is sent to a channel
    /// and written by the background task.
    pub async fn append(&self, entry: AofEntry) -> PersistenceResult<()> {
        self.tx
            .send(entry)
            .await
            .map_err(|_| PersistenceError::ChannelClosed)
    }

    /// Shutdown the AOF writer
    ///
    /// Waits for all pending writes to complete.
    pub async fn shutdown(mut self) -> PersistenceResult<()> {
        // Close the sender, signaling the task to finish
        drop(self.tx);

        // Wait for the task to complete
        if let Some(handle) = self.task_handle.take() {
            handle.await.map_err(|_| PersistenceError::TaskPanicked)??;
        }

        info!("AOF writer shutdown complete");
        Ok(())
    }

    /// Get the path to the AOF file
    pub fn file_path(&self) -> &Path {
        &self.config.file_path
    }
}

/// Background task that writes AOF entries to disk
///
/// This task batches writes and performs blocking I/O (flush/fsync) in a
/// separate blocking thread pool to avoid blocking the async runtime.
#[allow(clippy::too_many_lines)]
async fn aof_writer_task(
    mut rx: mpsc::Receiver<AofEntry>,
    config: Arc<AofConfig>,
) -> PersistenceResult<()> {
    // Open AOF file in append mode
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.file_path)?;

    // Wrap writer in Arc<Mutex> so we can move it into spawn_blocking
    let writer = Arc::new(std::sync::Mutex::new(BufWriter::with_capacity(
        config.buffer_size,
        file,
    )));
    let mut last_fsync = Instant::now();
    let mut current_db: Option<usize> = None;
    let mut write_count: u64 = 0;

    // For everysec mode, we need a timer
    let fsync_interval = Duration::from_secs(1);

    loop {
        tokio::select! {
            // Receive new entry
            entry = rx.recv() => {
                match entry {
                    Some(entry) => {
                        // Serialize commands to bytes first (non-blocking)
                        let mut data_to_write = Vec::new();

                        // Write SELECT command if database changed
                        if current_db != Some(entry.db) {
                            let select_cmd = vec![
                                RespValue::BulkString(Bytes::from("SELECT")),
                                RespValue::BulkString(Bytes::from(entry.db.to_string())),
                            ];
                            if let Ok(bytes) = serialize_resp(&RespValue::Array(select_cmd)) {
                                data_to_write.extend(bytes);
                            }
                            current_db = Some(entry.db);
                        }

                        // Serialize command
                        let array = RespValue::Array(entry.command);
                        if let Ok(bytes) = serialize_resp(&array) {
                            data_to_write.extend(bytes);
                        }

                        // Write to buffer (this is fast, just memcpy to BufWriter)
                        // Note: unwrap is safe here - poisoned mutex means another thread panicked
                        // and we should propagate that failure
                        #[allow(clippy::unwrap_used)]
                        {
                            let mut w = writer.lock().unwrap();
                            if let Err(e) = w.write_all(&data_to_write) {
                                warn!(error = %e, "Failed to write to AOF buffer");
                            }
                        }
                        write_count += 1;

                        // Fsync based on mode - use spawn_blocking to avoid blocking async runtime
                        let needs_fsync = match config.fsync_mode {
                            FsyncMode::Always => true,
                            FsyncMode::EverySecond => last_fsync.elapsed() >= fsync_interval,
                            FsyncMode::No => false,
                        };

                        if needs_fsync {
                            let writer_clone = Arc::clone(&writer);
                            #[allow(clippy::unwrap_used)]
                            let fsync_result = tokio::task::spawn_blocking(move || {
                                let mut w = writer_clone.lock().unwrap();
                                w.flush()?;
                                fsync_file(w.get_ref())
                            }).await;

                            match fsync_result {
                                Ok(Ok(())) => {
                                    last_fsync = Instant::now();
                                    debug!(count = write_count, "AOF fsync completed");
                                }
                                Ok(Err(e)) => {
                                    warn!(error = %e, "AOF fsync failed");
                                }
                                Err(e) => {
                                    warn!(error = %e, "AOF fsync task panicked");
                                }
                            }
                        }
                    }
                    None => {
                        // Channel closed, flush and exit using spawn_blocking
                        let writer_clone = Arc::clone(&writer);
                        let do_fsync = config.fsync_mode != FsyncMode::No;
                        #[allow(clippy::unwrap_used)]
                        let _ = tokio::task::spawn_blocking(move || {
                            let mut w = writer_clone.lock().unwrap();
                            let _ = w.flush();
                            if do_fsync {
                                let _ = fsync_file(w.get_ref());
                            }
                        }).await;
                        info!(total_writes = write_count, "AOF writer task exiting");
                        break;
                    }
                }
            }

            // Periodic fsync for everysec mode - use spawn_blocking
            _ = tokio::time::sleep(fsync_interval), if config.fsync_mode == FsyncMode::EverySecond => {
                if last_fsync.elapsed() >= fsync_interval {
                    let writer_clone = Arc::clone(&writer);
                    #[allow(clippy::unwrap_used)]
                    let fsync_result = tokio::task::spawn_blocking(move || {
                        let mut w = writer_clone.lock().unwrap();
                        w.flush()?;
                        fsync_file(w.get_ref())
                    }).await;

                    match fsync_result {
                        Ok(Ok(())) => {
                            last_fsync = Instant::now();
                            debug!(count = write_count, "AOF periodic fsync");
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "AOF periodic fsync failed");
                        }
                        Err(e) => {
                            warn!(error = %e, "AOF periodic fsync task panicked");
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Serialize a RespValue to bytes (RESP2 format)
fn serialize_resp(value: &RespValue) -> PersistenceResult<Vec<u8>> {
    let mut buf = Vec::new();
    serialize_resp_inner(value, &mut buf)?;
    Ok(buf)
}

fn serialize_resp_inner(value: &RespValue, buf: &mut Vec<u8>) -> PersistenceResult<()> {
    match value {
        RespValue::SimpleString(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            buf.push(b'-');
            buf.extend_from_slice(e.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            buf.push(b':');
            buf.extend_from_slice(i.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(bytes) => {
            buf.push(b'$');
            buf.extend_from_slice(bytes.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(bytes);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Array(arr) => {
            buf.push(b'*');
            buf.extend_from_slice(arr.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in arr {
                serialize_resp_inner(item, buf)?;
            }
        }
        RespValue::Null => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        // RESP3 types - serialize as RESP2 equivalents for compatibility
        RespValue::Double(d) => {
            // Serialize as bulk string
            let s = d.to_string();
            buf.push(b'$');
            buf.extend_from_slice(s.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Boolean(b) => {
            // Serialize as integer (0 or 1)
            buf.push(b':');
            buf.push(if *b { b'1' } else { b'0' });
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BigNumber(s) => {
            // Serialize as bulk string
            buf.push(b'$');
            buf.extend_from_slice(s.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::VerbatimString { data, .. } => {
            // Serialize as bulk string (ignore encoding for AOF)
            buf.push(b'$');
            buf.extend_from_slice(data.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Map(map) => {
            // Serialize as array of key-value pairs
            buf.push(b'*');
            buf.extend_from_slice((map.len() * 2).to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for (key, value) in map {
                // Serialize key as bulk string
                buf.push(b'$');
                buf.extend_from_slice(key.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b"\r\n");
                // Serialize value
                serialize_resp_inner(value, buf)?;
            }
        }
        RespValue::Set(items) => {
            // Serialize as array
            buf.push(b'*');
            buf.extend_from_slice(items.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in items {
                serialize_resp_inner(item, buf)?;
            }
        }
        RespValue::Push(items) => {
            // Serialize as array
            buf.push(b'*');
            buf.extend_from_slice(items.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in items {
                serialize_resp_inner(item, buf)?;
            }
        }
    }
    Ok(())
}

/// AOF reader for replaying commands on startup
pub struct AofReader {
    file_path: PathBuf,
}

impl AofReader {
    /// Create a new AOF reader
    pub fn new<P: Into<PathBuf>>(file_path: P) -> Self {
        Self {
            file_path: file_path.into(),
        }
    }

    /// Check if the AOF file exists
    pub fn exists(&self) -> bool {
        self.file_path.exists()
    }

    /// Replay all commands from the AOF file
    ///
    /// Calls the provided callback for each command.
    /// The callback receives the database number and the command.
    pub fn replay<F>(&self, mut callback: F) -> PersistenceResult<usize>
    where
        F: FnMut(usize, Vec<RespValue>) -> PersistenceResult<()>,
    {
        if !self.exists() {
            info!("AOF file does not exist, starting with empty database");
            return Ok(0);
        }

        info!(path = %self.file_path.display(), "Replaying AOF file");

        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);

        let mut current_db = 0;
        let mut command_count = 0;
        let mut buffer = BytesMut::with_capacity(8192);

        loop {
            // Read more data into buffer
            let mut temp_buf = vec![0u8; 8192];
            match std::io::Read::read(&mut reader, &mut temp_buf) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[..n]);
                }
                Err(e) => return Err(PersistenceError::Io(e)),
            }

            // Try to parse RESP values from buffer
            loop {
                match try_parse_resp(&buffer) {
                    Ok(Some((value, consumed))) => {
                        // Successfully parsed a value
                        buffer.advance(consumed);

                        // Process the command
                        if let RespValue::Array(command) = value {
                            // Check if this is a SELECT command
                            if is_select_command(&command) {
                                if let Some(db) = extract_db_from_select(&command) {
                                    current_db = db;
                                    continue;
                                }
                            }

                            // Execute the command
                            if let Err(e) = callback(current_db, command.clone()) {
                                warn!(error = %e, command = ?command, "Failed to replay command, skipping");
                            } else {
                                command_count += 1;

                                // Log progress every 10k commands
                                if command_count % 10_000 == 0 {
                                    info!(count = command_count, "AOF replay progress");
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // Need more data, break inner loop to read more
                        break;
                    }
                    Err(e) => {
                        // Parse error, skip this byte and try again
                        warn!(error = %e, "Failed to parse AOF entry, skipping byte");
                        if !buffer.is_empty() {
                            buffer.advance(1);
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        info!(count = command_count, "AOF replay complete");
        Ok(command_count)
    }
}

/// Try to parse a RESP value from the buffer
///
/// Returns Ok(Some((value, bytes_consumed))) if successful,
/// Ok(None) if more data is needed,
/// Err if the data is invalid.
fn try_parse_resp(buf: &[u8]) -> PersistenceResult<Option<(RespValue, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut pos = 0;

    match buf[0] {
        b'+' => parse_simple_string(buf, &mut pos),
        b'-' => parse_error(buf, &mut pos),
        b':' => parse_integer(buf, &mut pos),
        b'$' => parse_bulk_string(buf, &mut pos),
        b'*' => parse_array(buf, &mut pos),
        _ => Err(PersistenceError::Parse(format!(
            "Invalid RESP type byte: {}",
            buf[0]
        ))),
    }
}

fn parse_simple_string(
    buf: &[u8],
    pos: &mut usize,
) -> PersistenceResult<Option<(RespValue, usize)>> {
    *pos += 1; // Skip '+'
    if let Some(line) = read_line(buf, pos)? {
        Ok(Some((
            RespValue::SimpleString(String::from_utf8_lossy(&line).to_string()),
            *pos,
        )))
    } else {
        Ok(None)
    }
}

fn parse_error(buf: &[u8], pos: &mut usize) -> PersistenceResult<Option<(RespValue, usize)>> {
    *pos += 1; // Skip '-'
    if let Some(line) = read_line(buf, pos)? {
        Ok(Some((
            RespValue::Error(String::from_utf8_lossy(&line).to_string()),
            *pos,
        )))
    } else {
        Ok(None)
    }
}

fn parse_integer(buf: &[u8], pos: &mut usize) -> PersistenceResult<Option<(RespValue, usize)>> {
    *pos += 1; // Skip ':'
    if let Some(line) = read_line(buf, pos)? {
        let s = String::from_utf8_lossy(&line);
        let i = s
            .parse::<i64>()
            .map_err(|e| PersistenceError::Parse(format!("Invalid integer: {}", e)))?;
        Ok(Some((RespValue::Integer(i), *pos)))
    } else {
        Ok(None)
    }
}

fn parse_bulk_string(buf: &[u8], pos: &mut usize) -> PersistenceResult<Option<(RespValue, usize)>> {
    *pos += 1; // Skip '$'

    // Read length
    let len_line = match read_line(buf, pos)? {
        Some(line) => line,
        None => return Ok(None),
    };

    let len_str = String::from_utf8_lossy(&len_line);
    let len = len_str
        .parse::<i64>()
        .map_err(|e| PersistenceError::Parse(format!("Invalid bulk string length: {}", e)))?;

    if len == -1 {
        return Ok(Some((RespValue::Null, *pos)));
    }

    let len = len as usize;

    // Read data
    if *pos + len + 2 > buf.len() {
        return Ok(None); // Need more data
    }

    let data = &buf[*pos..*pos + len];
    *pos += len;

    // Skip \r\n
    if *pos + 2 > buf.len() || &buf[*pos..*pos + 2] != b"\r\n" {
        return Err(PersistenceError::Parse(
            "Missing \\r\\n after bulk string".to_string(),
        ));
    }
    *pos += 2;

    Ok(Some((
        RespValue::BulkString(Bytes::copy_from_slice(data)),
        *pos,
    )))
}

fn parse_array(buf: &[u8], pos: &mut usize) -> PersistenceResult<Option<(RespValue, usize)>> {
    *pos += 1; // Skip '*'

    // Read count
    let count_line = match read_line(buf, pos)? {
        Some(line) => line,
        None => return Ok(None),
    };

    let count_str = String::from_utf8_lossy(&count_line);
    let count = count_str
        .parse::<i64>()
        .map_err(|e| PersistenceError::Parse(format!("Invalid array count: {}", e)))?;

    if count == -1 {
        return Ok(Some((RespValue::Null, *pos)));
    }

    let count = count as usize;
    let mut elements = Vec::with_capacity(count);

    for _ in 0..count {
        match try_parse_resp(&buf[*pos..])? {
            Some((value, consumed)) => {
                elements.push(value);
                *pos += consumed;
            }
            None => return Ok(None), // Need more data
        }
    }

    Ok(Some((RespValue::Array(elements), *pos)))
}

fn read_line(buf: &[u8], pos: &mut usize) -> PersistenceResult<Option<Vec<u8>>> {
    // Find \r\n
    for i in *pos..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            let line = buf[*pos..i].to_vec();
            *pos = i + 2;
            return Ok(Some(line));
        }
    }
    Ok(None) // Need more data
}

fn is_select_command(command: &[RespValue]) -> bool {
    if command.is_empty() {
        return false;
    }
    if let RespValue::BulkString(cmd) = &command[0] {
        cmd.to_ascii_uppercase() == b"SELECT"
    } else {
        false
    }
}

fn extract_db_from_select(command: &[RespValue]) -> Option<usize> {
    if command.len() != 2 {
        return None;
    }
    if let RespValue::BulkString(db_bytes) = &command[1] {
        let db_str = String::from_utf8_lossy(db_bytes);
        db_str.parse().ok()
    } else {
        None
    }
}

trait BytesMutExt {
    fn advance(&mut self, cnt: usize);
}

impl BytesMutExt for BytesMut {
    fn advance(&mut self, cnt: usize) {
        let _ = self.split_to(cnt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_aof_writer_basic() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write a command
        let entry = AofEntry {
            command: vec![
                RespValue::BulkString(Bytes::from("SET")),
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("value")),
            ],
            db: 0,
        };

        writer.append(entry).await.unwrap();

        // Shutdown and verify file exists
        writer.shutdown().await.unwrap();

        assert!(file_path.exists());
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("SET"));
        assert!(content.contains("key"));
        assert!(content.contains("value"));
    }

    #[tokio::test]
    async fn test_aof_writer_multiple_commands() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::No,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write multiple commands
        for i in 0..10 {
            let entry = AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from(format!("key{}", i))),
                    RespValue::BulkString(Bytes::from(format!("value{}", i))),
                ],
                db: 0,
            };
            writer.append(entry).await.unwrap();
        }

        writer.shutdown().await.unwrap();

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("key0"));
        assert!(content.contains("key9"));
    }

    #[tokio::test]
    async fn test_aof_reader_replay() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        // Write some commands
        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key1")),
                    RespValue::BulkString(Bytes::from("value1")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key2")),
                    RespValue::BulkString(Bytes::from("value2")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Read and replay
        let reader = AofReader::new(&file_path);

        let mut replayed_commands = Vec::new();
        let count = reader
            .replay(|db, command| {
                replayed_commands.push((db, command));
                Ok(())
            })
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(replayed_commands.len(), 2);
        assert_eq!(replayed_commands[0].0, 0);
        assert_eq!(replayed_commands[1].0, 0);
    }

    #[tokio::test]
    async fn test_aof_database_switching() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write to DB 0
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key1")),
                    RespValue::BulkString(Bytes::from("value1")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // Write to DB 1
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key2")),
                    RespValue::BulkString(Bytes::from("value2")),
                ],
                db: 1,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Replay and verify DB switching
        let reader = AofReader::new(&file_path);

        let mut replayed = Vec::new();
        reader
            .replay(|db, command| {
                replayed.push((db, command));
                Ok(())
            })
            .unwrap();

        assert_eq!(replayed[0].0, 0);
        assert_eq!(replayed[1].0, 1);
    }

    #[test]
    fn test_serialize_resp() {
        let value = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
        ]);

        let bytes = serialize_resp(&value).unwrap();
        let s = String::from_utf8(bytes).unwrap();

        assert!(s.contains("*3"));
        assert!(s.contains("$3\r\nSET"));
        assert!(s.contains("$3\r\nkey"));
        assert!(s.contains("$5\r\nvalue"));
    }

    #[test]
    fn test_aof_reader_nonexistent_file() {
        let reader = AofReader::new("/nonexistent/path/to/file.aof");
        assert!(!reader.exists());

        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_aof_writer_fsync_everysec() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::EverySecond,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key")),
                    RespValue::BulkString(Bytes::from("value")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // Wait a bit to allow fsync to happen
        tokio::time::sleep(Duration::from_millis(1100)).await;

        writer.shutdown().await.unwrap();
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_aof_concurrent_writes() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::No,
            ..Default::default()
        };

        let writer = Arc::new(AofWriter::new(config).unwrap());

        // Spawn multiple tasks writing concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let writer_clone = Arc::clone(&writer);
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    writer_clone
                        .append(AofEntry {
                            command: vec![
                                RespValue::BulkString(Bytes::from("SET")),
                                RespValue::BulkString(Bytes::from(format!("key_{}_{}", i, j))),
                                RespValue::BulkString(Bytes::from(format!("value_{}_{}", i, j))),
                            ],
                            db: 0,
                        })
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Get the writer back from Arc
        let writer = Arc::try_unwrap(writer).unwrap();
        writer.shutdown().await.unwrap();

        // Verify all writes made it
        let reader = AofReader::new(&file_path);
        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 100);
    }

    #[tokio::test]
    async fn test_aof_large_values() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write a large value (1MB)
        let large_value = vec![b'x'; 1024 * 1024];
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("large_key")),
                    RespValue::BulkString(Bytes::from(large_value.clone())),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Replay and verify
        let reader = AofReader::new(&file_path);
        let mut replayed_value = None;
        reader
            .replay(|_, command| {
                if let Some(RespValue::BulkString(value)) = command.get(2) {
                    replayed_value = Some(value.to_vec());
                }
                Ok(())
            })
            .unwrap();

        assert_eq!(replayed_value.unwrap(), large_value);
    }

    #[tokio::test]
    async fn test_aof_empty_commands() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write command with empty string
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("")),
                    RespValue::BulkString(Bytes::from("value")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Replay
        let reader = AofReader::new(&file_path);
        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_aof_multiple_databases() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write to different databases
        for db in 0..5 {
            writer
                .append(AofEntry {
                    command: vec![
                        RespValue::BulkString(Bytes::from("SET")),
                        RespValue::BulkString(Bytes::from(format!("key_db{}", db))),
                        RespValue::BulkString(Bytes::from(format!("value_db{}", db))),
                    ],
                    db,
                })
                .await
                .unwrap();
        }

        writer.shutdown().await.unwrap();

        // Replay and verify database switching
        let reader = AofReader::new(&file_path);
        let mut db_commands: Vec<(usize, Vec<RespValue>)> = Vec::new();
        reader
            .replay(|db, command| {
                db_commands.push((db, command));
                Ok(())
            })
            .unwrap();

        assert_eq!(db_commands.len(), 5);
        for (i, (db, _)) in db_commands.iter().enumerate() {
            assert_eq!(*db, i);
        }
    }

    #[tokio::test]
    async fn test_aof_replay_error_continues() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write multiple commands
        for i in 0..5 {
            writer
                .append(AofEntry {
                    command: vec![
                        RespValue::BulkString(Bytes::from("SET")),
                        RespValue::BulkString(Bytes::from(format!("key{}", i))),
                        RespValue::BulkString(Bytes::from(format!("value{}", i))),
                    ],
                    db: 0,
                })
                .await
                .unwrap();
        }

        writer.shutdown().await.unwrap();

        // Replay with errors on some commands
        let reader = AofReader::new(&file_path);
        let mut success_count = 0;
        let count = reader
            .replay(|_, command| {
                // Fail on key2
                if let Some(RespValue::BulkString(key)) = command.get(1) {
                    if key.as_ref() == b"key2" {
                        return Err(PersistenceError::Parse("simulated error".to_string()));
                    }
                }
                success_count += 1;
                Ok(())
            })
            .unwrap();

        // Should complete 4 commands (skip key2)
        assert_eq!(count, 4);
        assert_eq!(success_count, 4);
    }

    #[tokio::test]
    async fn test_aof_binary_data() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write binary data (not valid UTF-8)
        let binary_data = vec![0u8, 1, 2, 255, 254, 253];
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("binary_key")),
                    RespValue::BulkString(Bytes::from(binary_data.clone())),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Replay and verify binary data
        let reader = AofReader::new(&file_path);
        let mut replayed_data = None;
        reader
            .replay(|_, command| {
                if let Some(RespValue::BulkString(value)) = command.get(2) {
                    replayed_data = Some(value.to_vec());
                }
                Ok(())
            })
            .unwrap();

        assert_eq!(replayed_data.unwrap(), binary_data);
    }

    #[tokio::test]
    async fn test_aof_different_command_types() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::Always,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // SET command
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key1")),
                    RespValue::BulkString(Bytes::from("value1")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // DEL command
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("DEL")),
                    RespValue::BulkString(Bytes::from("key1")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // LPUSH command
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("LPUSH")),
                    RespValue::BulkString(Bytes::from("list")),
                    RespValue::BulkString(Bytes::from("item1")),
                    RespValue::BulkString(Bytes::from("item2")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // SADD command
        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SADD")),
                    RespValue::BulkString(Bytes::from("set")),
                    RespValue::BulkString(Bytes::from("member1")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        writer.shutdown().await.unwrap();

        // Replay all commands
        let reader = AofReader::new(&file_path);
        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn test_aof_high_throughput() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::No,
            buffer_size: 256 * 1024, // Larger buffer
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Write 1000 commands quickly
        for i in 0..1000 {
            writer
                .append(AofEntry {
                    command: vec![
                        RespValue::BulkString(Bytes::from("SET")),
                        RespValue::BulkString(Bytes::from(format!("key{}", i))),
                        RespValue::BulkString(Bytes::from(format!("value{}", i))),
                    ],
                    db: 0,
                })
                .await
                .unwrap();
        }

        writer.shutdown().await.unwrap();

        // Verify all commands
        let reader = AofReader::new(&file_path);
        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 1000);
    }

    #[test]
    fn test_serialize_resp_types() {
        // Test SimpleString
        let value = RespValue::SimpleString("OK".to_string());
        let bytes = serialize_resp(&value).unwrap();
        assert_eq!(bytes, b"+OK\r\n");

        // Test Error
        let value = RespValue::Error("ERR".to_string());
        let bytes = serialize_resp(&value).unwrap();
        assert_eq!(bytes, b"-ERR\r\n");

        // Test Integer
        let value = RespValue::Integer(42);
        let bytes = serialize_resp(&value).unwrap();
        assert_eq!(bytes, b":42\r\n");

        // Test Null
        let value = RespValue::Null;
        let bytes = serialize_resp(&value).unwrap();
        assert_eq!(bytes, b"$-1\r\n");
    }

    #[tokio::test]
    async fn test_aof_file_path() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("custom.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();
        assert_eq!(writer.file_path(), &file_path);
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_aof_channel_capacity() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path,
            channel_capacity: 10,
            fsync_mode: FsyncMode::No,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        // Should be able to write many commands even with small capacity
        for i in 0..100 {
            writer
                .append(AofEntry {
                    command: vec![
                        RespValue::BulkString(Bytes::from("SET")),
                        RespValue::BulkString(Bytes::from(format!("key{}", i))),
                        RespValue::BulkString(Bytes::from("value")),
                    ],
                    db: 0,
                })
                .await
                .unwrap();
        }

        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_aof_shutdown_flushes() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.aof");

        let config = AofConfig {
            file_path: file_path.clone(),
            fsync_mode: FsyncMode::No,
            ..Default::default()
        };

        let writer = AofWriter::new(config).unwrap();

        writer
            .append(AofEntry {
                command: vec![
                    RespValue::BulkString(Bytes::from("SET")),
                    RespValue::BulkString(Bytes::from("key")),
                    RespValue::BulkString(Bytes::from("value")),
                ],
                db: 0,
            })
            .await
            .unwrap();

        // Shutdown should flush
        writer.shutdown().await.unwrap();

        // Verify data is on disk
        let reader = AofReader::new(&file_path);
        let count = reader.replay(|_, _| Ok(())).unwrap();
        assert_eq!(count, 1);
    }
}
