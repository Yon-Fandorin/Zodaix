use rusqlite::Connection;
use zodaix_core::{VfsError, VfsResult};

/// Current schema version.
pub const SCHEMA_VERSION: &str = "1";

/// Default chunk size for file data (4KB, matching FUSE/NFS block size).
pub const CHUNK_SIZE: u32 = 4096;

/// Initialize the database schema (idempotent).
pub fn init_schema(conn: &Connection) -> VfsResult<()> {
    conn.execute_batch(DDL).map_err(|e| VfsError::Other(format!("schema init: {e}")))?;

    // Insert default config if not present.
    conn.execute(
        "INSERT OR IGNORE INTO fs_config (key, value) VALUES ('schema_version', ?1)",
        [SCHEMA_VERSION],
    )
    .map_err(|e| VfsError::Other(format!("config insert: {e}")))?;

    conn.execute(
        "INSERT OR IGNORE INTO fs_config (key, value) VALUES ('chunk_size', ?1)",
        [&CHUNK_SIZE.to_string()],
    )
    .map_err(|e| VfsError::Other(format!("config insert: {e}")))?;

    Ok(())
}

const DDL: &str = r#"
-- File metadata (inode)
CREATE TABLE IF NOT EXISTS fs_inode (
    ino        INTEGER PRIMARY KEY,
    mode       INTEGER NOT NULL,
    nlink      INTEGER NOT NULL DEFAULT 1,
    uid        INTEGER NOT NULL,
    gid        INTEGER NOT NULL,
    size       INTEGER NOT NULL DEFAULT 0,
    rdev       INTEGER NOT NULL DEFAULT 0,
    atime_s    INTEGER NOT NULL,
    atime_ns   INTEGER NOT NULL,
    mtime_s    INTEGER NOT NULL,
    mtime_ns   INTEGER NOT NULL,
    ctime_s    INTEGER NOT NULL,
    ctime_ns   INTEGER NOT NULL,
    crtime_s   INTEGER NOT NULL,
    crtime_ns  INTEGER NOT NULL
);

-- Directory entries (dentry) — separated from inode for natural hardlink support
CREATE TABLE IF NOT EXISTS fs_dentry (
    parent_ino INTEGER NOT NULL,
    name       TEXT NOT NULL,
    ino        INTEGER NOT NULL REFERENCES fs_inode(ino),
    PRIMARY KEY (parent_ino, name)
);
CREATE INDEX IF NOT EXISTS idx_dentry_ino ON fs_dentry(ino);

-- File content — 4KB chunks (AgentFS pattern)
CREATE TABLE IF NOT EXISTS fs_data (
    ino        INTEGER NOT NULL,
    chunk_idx  INTEGER NOT NULL,
    data       BLOB NOT NULL,
    PRIMARY KEY (ino, chunk_idx)
);

-- Symlink targets
CREATE TABLE IF NOT EXISTS fs_symlink (
    ino    INTEGER PRIMARY KEY,
    target TEXT NOT NULL
);

-- Extended attributes
CREATE TABLE IF NOT EXISTS fs_xattr (
    ino   INTEGER NOT NULL,
    name  TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (ino, name)
);

-- Full-text search (FTS5) — stores content for retrieval
CREATE VIRTUAL TABLE IF NOT EXISTS fs_fts USING fts5(
    path, tags, description
);

-- Configuration / metadata
CREATE TABLE IF NOT EXISTS fs_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;
