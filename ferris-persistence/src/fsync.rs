//! Platform-specific fsync implementations
//!
//! Note: This module uses safe abstractions where possible.
//! For advanced fsync behavior, we rely on std::fs::File::sync_all()
//! which provides cross-platform durability guarantees.

use std::fs::File;
use std::io;

/// Perform platform-specific fsync on a file
///
/// This ensures data is written to persistent storage.
///
/// # Implementation
///
/// We use `File::sync_all()` which provides cross-platform durability:
/// - **Linux**: Calls `fsync()` system call
/// - **macOS**: Calls `fsync()` system call  
/// - **Windows**: Calls `FlushFileBuffers()`
///
/// Note: On macOS, `fsync()` may not guarantee immediate disk write due to
/// disk drive caching. For production, consider using `nix` crate with F_FULLFSYNC.
/// For now, we prioritize safe code over platform-specific optimizations.
pub fn fsync_file(file: &File) -> io::Result<()> {
    // Use standard library sync_all for cross-platform safety
    // This is guaranteed to work correctly on all platforms
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_fsync_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        file.write_all(b"test data").unwrap();

        // Should not panic or error
        fsync_file(&file).unwrap();

        // Verify data was written
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "test data");
    }
}
