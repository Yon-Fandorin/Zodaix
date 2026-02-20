use crate::schema::{self, CHUNK_SIZE};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zodaix_core::*;

/// SQLite-backed persistent VFS backend.
///
/// Uses inode/dentry/chunk separation (AgentFS pattern).
/// WAL mode for concurrent reads with single writer.
/// Separate read/write connections: reads never block writes and vice versa.
pub struct SqliteBackend {
    write_conn: Mutex<Connection>,
    /// Separate read connection for WAL-mode concurrent reads.
    /// `None` for in-memory databases (`:memory:` can't share across connections).
    read_conn: Option<Mutex<Connection>>,
    next_ino: AtomicU64,
    chunk_size: u32,
}

// ── Helpers ──────────────────────────────────────────────────────

fn systime_to_secs_nanos(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(_) => (0, 0),
    }
}

fn secs_nanos_to_systime(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH
    }
}

fn name_to_str(name: &OsStr) -> VfsResult<&str> {
    name.to_str().ok_or(VfsError::InvalidArgument)
}

/// Extract FileType from a mode integer.
fn mode_to_kind(mode: u32) -> FileType {
    let fmt = mode & (libc::S_IFMT as u32);
    if fmt == libc::S_IFDIR as u32 {
        FileType::Directory
    } else if fmt == libc::S_IFLNK as u32 {
        FileType::Symlink
    } else {
        FileType::RegularFile
    }
}

fn kind_mode_bits(kind: FileType) -> u32 {
    match kind {
        FileType::RegularFile => libc::S_IFREG as u32,
        FileType::Directory => libc::S_IFDIR as u32,
        FileType::Symlink => libc::S_IFLNK as u32,
    }
}

impl SqliteBackend {
    /// Open (or create) a SQLite-backed VFS at the given path.
    pub fn open(path: &str) -> VfsResult<Self> {
        let is_memory = path == ":memory:" || path.is_empty();

        // Ensure parent directory exists for file-based DBs.
        if !is_memory {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| VfsError::Other(format!("mkdir: {e}")))?;
                }
            }
        }

        let write_conn =
            Connection::open(path).map_err(|e| VfsError::Other(format!("sqlite open: {e}")))?;

        // Enable WAL mode for concurrent reads.
        write_conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| VfsError::Other(format!("pragma: {e}")))?;

        schema::init_schema(&write_conn)?;

        // Seed root inode if it doesn't exist.
        let root_exists: bool = write_conn
            .query_row("SELECT COUNT(*) FROM fs_inode WHERE ino = 1", [], |row| {
                row.get::<_, i64>(0).map(|c| c > 0)
            })
            .map_err(|e| VfsError::Other(format!("root check: {e}")))?;

        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();

        if !root_exists {
            let now = SystemTime::now();
            let (s, ns) = systime_to_secs_nanos(now);
            let mode = (libc::S_IFDIR as u32) | 0o755;
            write_conn.execute(
                "INSERT INTO fs_inode (ino, mode, nlink, uid, gid, size, rdev, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns, crtime_s, crtime_ns) VALUES (?1,?2,?3,?4,?5,0,0,?6,?7,?6,?7,?6,?7,?6,?7)",
                params![1i64, mode, 2u32, uid, gid, s, ns],
            )
            .map_err(|e| VfsError::Other(format!("root insert: {e}")))?;
        }

        // Determine next inode number.
        let max_ino: i64 = write_conn
            .query_row("SELECT COALESCE(MAX(ino), 1) FROM fs_inode", [], |row| {
                row.get(0)
            })
            .map_err(|e| VfsError::Other(format!("max ino: {e}")))?;

        // Open separate read connection for file-based DBs.
        // In WAL mode, the read connection never blocks the writer and vice versa.
        let read_conn = if !is_memory {
            let rc = Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|e| VfsError::Other(format!("read conn open: {e}")))?;
            rc.execute_batch("PRAGMA journal_mode=WAL;")
                .map_err(|e| VfsError::Other(format!("read conn pragma: {e}")))?;
            Some(Mutex::new(rc))
        } else {
            None
        };

        Ok(Self {
            write_conn: Mutex::new(write_conn),
            read_conn,
            next_ino: AtomicU64::new((max_ino + 1) as u64),
            chunk_size: CHUNK_SIZE,
        })
    }

    /// Lock a connection suitable for read-only operations.
    /// Uses the dedicated read connection (WAL concurrent reads) if available,
    /// otherwise falls back to the write connection (in-memory DBs).
    fn read_lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        match &self.read_conn {
            Some(rc) => rc.lock(),
            None => self.write_conn.lock(),
        }
    }

    fn alloc_ino(&self) -> u64 {
        self.next_ino.fetch_add(1, Ordering::Relaxed)
    }

    /// Read inode row into VfsAttr.
    fn row_to_attr(row: &rusqlite::Row) -> rusqlite::Result<VfsAttr> {
        let ino: i64 = row.get("ino")?;
        let mode: u32 = row.get("mode")?;
        let nlink: u32 = row.get("nlink")?;
        let uid: u32 = row.get("uid")?;
        let gid: u32 = row.get("gid")?;
        let size: i64 = row.get("size")?;
        let rdev: u32 = row.get("rdev")?;
        let atime_s: i64 = row.get("atime_s")?;
        let atime_ns: u32 = row.get("atime_ns")?;
        let mtime_s: i64 = row.get("mtime_s")?;
        let mtime_ns: u32 = row.get("mtime_ns")?;
        let ctime_s: i64 = row.get("ctime_s")?;
        let ctime_ns: u32 = row.get("ctime_ns")?;
        let crtime_s: i64 = row.get("crtime_s")?;
        let crtime_ns: u32 = row.get("crtime_ns")?;

        Ok(VfsAttr {
            ino: ino as u64,
            size: size as u64,
            blocks: (size as u64).div_ceil(512),
            atime: secs_nanos_to_systime(atime_s, atime_ns),
            mtime: secs_nanos_to_systime(mtime_s, mtime_ns),
            ctime: secs_nanos_to_systime(ctime_s, ctime_ns),
            crtime: secs_nanos_to_systime(crtime_s, crtime_ns),
            kind: mode_to_kind(mode),
            perm: (mode & 0o7777) as u16,
            nlink,
            uid,
            gid,
            rdev,
            blksize: 4096,
        })
    }

    fn get_inode(conn: &Connection, ino: u64) -> VfsResult<VfsAttr> {
        conn.query_row(
            "SELECT * FROM fs_inode WHERE ino = ?1",
            [ino as i64],
            Self::row_to_attr,
        )
        .optional()
        .map_err(|e| VfsError::Other(format!("getattr: {e}")))?
        .ok_or(VfsError::NotFound)
    }

    fn update_times(conn: &Connection, ino: u64, atime: bool, mtime: bool, ctime: bool) -> VfsResult<()> {
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);
        let mut parts = Vec::new();
        if atime { parts.push("atime_s=?2, atime_ns=?3"); }
        if mtime { parts.push("mtime_s=?2, mtime_ns=?3"); }
        if ctime { parts.push("ctime_s=?2, ctime_ns=?3"); }
        if parts.is_empty() { return Ok(()); }
        let sql = format!("UPDATE fs_inode SET {} WHERE ino=?1", parts.join(", "));
        conn.execute(&sql, params![ino as i64, s, ns])
            .map_err(|e| VfsError::Other(format!("update_times: {e}")))?;
        Ok(())
    }

    /// Build the full path of an inode by walking dentries up to root.
    /// Depth limited to prevent infinite loops on corrupted dentry cycles.
    fn build_path(conn: &Connection, ino: u64) -> String {
        const MAX_DEPTH: usize = 1024;
        let mut parts = Vec::new();
        let mut cur = ino;
        for _ in 0..MAX_DEPTH {
            if cur == ROOT_INO {
                break;
            }
            let name: Option<String> = conn
                .query_row(
                    "SELECT name, parent_ino FROM fs_dentry WHERE ino = ?1 LIMIT 1",
                    [cur as i64],
                    |row| {
                        let name: String = row.get(0)?;
                        let parent: i64 = row.get(1)?;
                        cur = parent as u64;
                        Ok(name)
                    },
                )
                .optional()
                .unwrap_or(None);
            match name {
                Some(n) => parts.push(n),
                None => break,
            }
        }
        parts.reverse();
        format!("/{}", parts.join("/"))
    }
}

impl VfsBackend for SqliteBackend {
    fn name(&self) -> &str {
        "sqlite"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::HARDLINKS
            | BackendCapabilities::SYMLINKS
            | BackendCapabilities::XATTRS
            | BackendCapabilities::SEARCH
            | BackendCapabilities::PERSISTENT
    }

    fn shutdown(&self) -> VfsResult<()> {
        // Flush WAL to main DB.
        let conn = self.write_conn.lock();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| VfsError::Other(format!("shutdown checkpoint: {e}")))?;
        Ok(())
    }

    // ── Lookup & attributes ──────────────────────────────────────

    fn lookup(&self, parent: InodeId, name: &OsStr) -> VfsResult<VfsAttr> {
        let name_str = name_to_str(name)?;
        let conn = self.read_lock();
        let child_ino: i64 = conn
            .query_row(
                "SELECT ino FROM fs_dentry WHERE parent_ino = ?1 AND name = ?2",
                params![parent as i64, name_str],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("lookup: {e}")))?
            .ok_or(VfsError::NotFound)?;
        Self::get_inode(&conn, child_ino as u64)
    }

    fn getattr(&self, ino: InodeId) -> VfsResult<VfsAttr> {
        let conn = self.read_lock();
        Self::get_inode(&conn, ino)
    }

    fn setattr(&self, ino: InodeId, params: SetAttrParams) -> VfsResult<VfsAttr> {
        let conn = self.write_conn.lock();
        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("setattr begin: {e}")))?;
        let mut attr = Self::get_inode(&tx, ino)?;

        // Collect SET clauses and params. Params start at ?2 (ino is ?1).
        let mut sets = Vec::new();
        let mut sql_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        // Helper: push a param and return its placeholder index.
        macro_rules! push_param {
            ($val:expr) => {{
                sql_params.push(Box::new($val));
                sql_params.len() + 1 // +1 because ?1 is ino
            }};
        }

        if let Some(mode) = params.mode {
            let new_perm = (mode & 0o7777) as u16;
            let new_mode = kind_mode_bits(attr.kind) | new_perm as u32;
            let i = push_param!(new_mode);
            sets.push(format!("mode=?{i}"));
            attr.perm = new_perm;
        }
        if let Some(uid) = params.uid {
            let i = push_param!(uid);
            sets.push(format!("uid=?{i}"));
            attr.uid = uid;
        }
        if let Some(gid) = params.gid {
            let i = push_param!(gid);
            sets.push(format!("gid=?{i}"));
            attr.gid = gid;
        }
        if let Some(size) = params.size {
            let i = push_param!(size as i64);
            sets.push(format!("size=?{i}"));
            attr.size = size;

            // Truncate data chunks.
            let chunk_size = self.chunk_size as u64;
            let last_chunk = if size == 0 { 0 } else { (size - 1) / chunk_size };
            if size == 0 {
                tx.execute("DELETE FROM fs_data WHERE ino=?1", [ino as i64])
                    .map_err(|e| VfsError::Other(format!("truncate data: {e}")))?;
            } else {
                tx.execute(
                    "DELETE FROM fs_data WHERE ino=?1 AND chunk_idx > ?2",
                    params![ino as i64, last_chunk as i64],
                )
                .map_err(|e| VfsError::Other(format!("truncate data: {e}")))?;

                // Trim last chunk if partial.
                let offset_in_chunk = (size % chunk_size) as usize;
                if offset_in_chunk > 0 {
                    let existing: Option<Vec<u8>> = tx
                        .query_row(
                            "SELECT data FROM fs_data WHERE ino=?1 AND chunk_idx=?2",
                            params![ino as i64, last_chunk as i64],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|e| VfsError::Other(format!("trim chunk read: {e}")))?;
                    if let Some(mut chunk_data) = existing {
                        chunk_data.truncate(offset_in_chunk);
                        tx.execute(
                            "UPDATE fs_data SET data=?3 WHERE ino=?1 AND chunk_idx=?2",
                            params![ino as i64, last_chunk as i64, chunk_data],
                        )
                        .map_err(|e| VfsError::Other(format!("trim chunk write: {e}")))?;
                    }
                }
            }
        }
        if let Some(atime) = params.atime {
            let (s, ns) = systime_to_secs_nanos(atime);
            let si = push_param!(s);
            let ni = push_param!(ns);
            sets.push(format!("atime_s=?{si}, atime_ns=?{ni}"));
            attr.atime = atime;
        }
        if let Some(mtime) = params.mtime {
            let (s, ns) = systime_to_secs_nanos(mtime);
            let si = push_param!(s);
            let ni = push_param!(ns);
            sets.push(format!("mtime_s=?{si}, mtime_ns=?{ni}"));
            attr.mtime = mtime;
        }

        // Always update ctime.
        let now = SystemTime::now();
        let (cs, cns) = systime_to_secs_nanos(now);
        let csi = push_param!(cs);
        let cni = push_param!(cns);
        sets.push(format!("ctime_s=?{csi}, ctime_ns=?{cni}"));
        attr.ctime = now;

        if !sets.is_empty() {
            let sql = format!("UPDATE fs_inode SET {} WHERE ino=?1", sets.join(", "));
            let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            all_params.push(Box::new(ino as i64));
            all_params.extend(sql_params);
            let refs: Vec<&dyn rusqlite::types::ToSql> = all_params.iter().map(|p| p.as_ref()).collect();
            tx.execute(&sql, refs.as_slice())
                .map_err(|e| VfsError::Other(format!("setattr: {e}")))?;
        }

        tx.commit().map_err(|e| VfsError::Other(format!("setattr commit: {e}")))?;
        Ok(attr)
    }

    // ── File I/O ─────────────────────────────────────────────────

    fn open(&self, ino: InodeId, _flags: i32) -> VfsResult<FileHandle> {
        let conn = self.read_lock();
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM fs_inode WHERE ino=?1",
                [ino as i64],
                |row| row.get::<_, i64>(0).map(|c| c > 0),
            )
            .map_err(|e| VfsError::Other(format!("open: {e}")))?;
        if !exists {
            return Err(VfsError::NotFound);
        }
        // We don't track file handles for SQLite — just return a dummy.
        Ok(0)
    }

    fn read(&self, ino: InodeId, _fh: FileHandle, offset: i64, size: u32) -> VfsResult<Vec<u8>> {
        if offset < 0 {
            return Err(VfsError::InvalidArgument);
        }
        let conn = self.read_lock();
        let attr = Self::get_inode(&conn, ino)?;
        let file_size = attr.size;

        if offset as u64 >= file_size {
            return Ok(Vec::new());
        }

        let end = ((offset as u64) + size as u64).min(file_size);
        let cs = self.chunk_size as u64;
        let first_chunk = offset as u64 / cs;
        let last_chunk = (end - 1) / cs;

        let mut result = Vec::with_capacity((end - offset as u64) as usize);

        let mut stmt = conn
            .prepare_cached("SELECT chunk_idx, data FROM fs_data WHERE ino=?1 AND chunk_idx BETWEEN ?2 AND ?3 ORDER BY chunk_idx")
            .map_err(|e| VfsError::Other(format!("read prepare: {e}")))?;

        let rows = stmt
            .query_map(
                params![ino as i64, first_chunk as i64, last_chunk as i64],
                |row| {
                    let idx: i64 = row.get(0)?;
                    let data: Vec<u8> = row.get(1)?;
                    Ok((idx as u64, data))
                },
            )
            .map_err(|e| VfsError::Other(format!("read query: {e}")))?;

        // Collect into a map for easy access (chunks may be sparse).
        let mut chunk_map = std::collections::BTreeMap::new();
        for row in rows {
            let (idx, data) = row.map_err(|e| VfsError::Other(format!("read row: {e}")))?;
            chunk_map.insert(idx, data);
        }

        let mut pos = offset as u64;
        while pos < end {
            let chunk_idx = pos / cs;
            let offset_in_chunk = (pos % cs) as usize;
            let remaining_in_chunk = (cs as usize) - offset_in_chunk;
            let to_read = remaining_in_chunk.min((end - pos) as usize);

            if let Some(chunk_data) = chunk_map.get(&chunk_idx) {
                if offset_in_chunk < chunk_data.len() {
                    let avail = chunk_data.len() - offset_in_chunk;
                    let n = to_read.min(avail);
                    result.extend_from_slice(&chunk_data[offset_in_chunk..offset_in_chunk + n]);
                    // If chunk is shorter than expected, pad with zeros.
                    if n < to_read {
                        result.extend(std::iter::repeat_n(0u8,to_read - n));
                    }
                } else {
                    // Offset beyond chunk data → zeros.
                    result.extend(std::iter::repeat_n(0u8,to_read));
                }
            } else {
                // Missing chunk → zeros (sparse file).
                result.extend(std::iter::repeat_n(0u8,to_read));
            }

            pos += to_read as u64;
        }

        Ok(result)
    }

    fn write(
        &self,
        ino: InodeId,
        _fh: FileHandle,
        offset: i64,
        data: &[u8],
        _flags: i32,
    ) -> VfsResult<u32> {
        if offset < 0 {
            return Err(VfsError::InvalidArgument);
        }
        let conn = self.write_conn.lock();
        let cs = self.chunk_size as u64;
        let offset = offset as u64;
        let end = offset + data.len() as u64;

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("write begin: {e}")))?;

        let mut written = 0usize;
        let mut pos = offset;

        while pos < end {
            let chunk_idx = pos / cs;
            let offset_in_chunk = (pos % cs) as usize;
            let remaining_in_chunk = cs as usize - offset_in_chunk;
            let to_write = remaining_in_chunk.min((end - pos) as usize);

            // Read existing chunk (for read-modify-write on boundary chunks).
            let existing: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT data FROM fs_data WHERE ino=?1 AND chunk_idx=?2",
                    params![ino as i64, chunk_idx as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| VfsError::Other(format!("write read chunk: {e}")))?;

            let mut chunk_data = existing.unwrap_or_default();

            // Extend chunk if necessary.
            let needed = offset_in_chunk + to_write;
            if chunk_data.len() < needed {
                chunk_data.resize(needed, 0);
            }

            chunk_data[offset_in_chunk..offset_in_chunk + to_write]
                .copy_from_slice(&data[written..written + to_write]);

            tx.execute(
                "INSERT OR REPLACE INTO fs_data (ino, chunk_idx, data) VALUES (?1, ?2, ?3)",
                params![ino as i64, chunk_idx as i64, chunk_data],
            )
            .map_err(|e| VfsError::Other(format!("write chunk: {e}")))?;

            written += to_write;
            pos += to_write as u64;
        }

        // Update size if we extended the file.
        let current_size: i64 = tx
            .query_row(
                "SELECT size FROM fs_inode WHERE ino=?1",
                [ino as i64],
                |row| row.get(0),
            )
            .map_err(|e| VfsError::Other(format!("write size check: {e}")))?;

        let new_size = (current_size as u64).max(end);
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);
        tx.execute(
            "UPDATE fs_inode SET size=?2, mtime_s=?3, mtime_ns=?4, ctime_s=?3, ctime_ns=?4 WHERE ino=?1",
            params![ino as i64, new_size as i64, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("write update size: {e}")))?;

        tx.commit().map_err(|e| VfsError::Other(format!("write commit: {e}")))?;
        Ok(data.len() as u32)
    }

    fn flush(&self, _ino: InodeId, _fh: FileHandle) -> VfsResult<()> {
        Ok(())
    }

    fn release(&self, _ino: InodeId, _fh: FileHandle, _flags: i32, _flush: bool) -> VfsResult<()> {
        Ok(())
    }

    fn fsync(&self, _ino: InodeId, _fh: FileHandle, _datasync: bool) -> VfsResult<()> {
        // Force WAL checkpoint.
        let conn = self.write_conn.lock();
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(|e| VfsError::Other(format!("fsync: {e}")))?;
        Ok(())
    }

    // ── Directory operations ─────────────────────────────────────

    fn mknod(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> VfsResult<VfsAttr> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("mknod begin: {e}")))?;

        // Check parent is a directory.
        let parent_attr = Self::get_inode(&tx, parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        // Check name doesn't already exist.
        let exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, name_str],
                |row| row.get::<_, i64>(0).map(|c| c > 0),
            )
            .map_err(|e| VfsError::Other(format!("mknod check: {e}")))?;
        if exists {
            return Err(VfsError::AlreadyExists);
        }

        let ino = self.alloc_ino();
        let perm = mode & !umask & 0o7777;
        let full_mode = (libc::S_IFREG as u32) | perm;
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);

        tx.execute(
            "INSERT INTO fs_inode (ino, mode, nlink, uid, gid, size, rdev, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns, crtime_s, crtime_ns) VALUES (?1,?2,1,?3,?4,0,?5,?6,?7,?6,?7,?6,?7,?6,?7)",
            params![ino as i64, full_mode, uid, gid, rdev, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("mknod insert inode: {e}")))?;

        tx.execute(
            "INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?1, ?2, ?3)",
            params![parent as i64, name_str, ino as i64],
        )
        .map_err(|e| VfsError::Other(format!("mknod insert dentry: {e}")))?;

        // Update parent mtime/ctime.
        Self::update_times(&tx, parent, false, true, true)?;

        let attr = Self::get_inode(&tx, ino)?;
        tx.commit().map_err(|e| VfsError::Other(format!("mknod commit: {e}")))?;
        Ok(attr)
    }

    fn mkdir(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
    ) -> VfsResult<VfsAttr> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("mkdir begin: {e}")))?;

        let parent_attr = Self::get_inode(&tx, parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        let exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, name_str],
                |row| row.get::<_, i64>(0).map(|c| c > 0),
            )
            .map_err(|e| VfsError::Other(format!("mkdir check: {e}")))?;
        if exists {
            return Err(VfsError::AlreadyExists);
        }

        let ino = self.alloc_ino();
        let perm = mode & !umask & 0o7777;
        let full_mode = (libc::S_IFDIR as u32) | perm;
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);

        tx.execute(
            "INSERT INTO fs_inode (ino, mode, nlink, uid, gid, size, rdev, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns, crtime_s, crtime_ns) VALUES (?1,?2,2,?3,?4,0,0,?5,?6,?5,?6,?5,?6,?5,?6)",
            params![ino as i64, full_mode, uid, gid, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("mkdir insert inode: {e}")))?;

        tx.execute(
            "INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?1, ?2, ?3)",
            params![parent as i64, name_str, ino as i64],
        )
        .map_err(|e| VfsError::Other(format!("mkdir insert dentry: {e}")))?;

        // Increment parent nlink (subdir adds ".." link to parent).
        tx.execute(
            "UPDATE fs_inode SET nlink=nlink+1 WHERE ino=?1",
            [parent as i64],
        )
        .map_err(|e| VfsError::Other(format!("mkdir parent nlink: {e}")))?;

        Self::update_times(&tx, parent, false, true, true)?;

        let attr = Self::get_inode(&tx, ino)?;
        tx.commit().map_err(|e| VfsError::Other(format!("mkdir commit: {e}")))?;
        Ok(attr)
    }

    fn unlink(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("unlink begin: {e}")))?;

        // Find the child inode.
        let child_ino: i64 = tx
            .query_row(
                "SELECT ino FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, name_str],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("unlink lookup: {e}")))?
            .ok_or(VfsError::NotFound)?;

        // Check it's not a directory.
        let attr = Self::get_inode(&tx, child_ino as u64)?;
        if attr.kind == FileType::Directory {
            return Err(VfsError::IsADirectory);
        }

        // Remove dentry.
        tx.execute(
            "DELETE FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
            params![parent as i64, name_str],
        )
        .map_err(|e| VfsError::Other(format!("unlink dentry: {e}")))?;

        // Decrement nlink (saturating to prevent underflow on corruption).
        tx.execute(
            "UPDATE fs_inode SET nlink=CASE WHEN nlink>0 THEN nlink-1 ELSE 0 END WHERE ino=?1",
            [child_ino],
        )
        .map_err(|e| VfsError::Other(format!("unlink nlink: {e}")))?;

        // If nlink reaches 0, delete inode + data + xattrs + symlink.
        let new_nlink: u32 = tx
            .query_row(
                "SELECT nlink FROM fs_inode WHERE ino=?1",
                [child_ino],
                |row| row.get(0),
            )
            .map_err(|e| VfsError::Other(format!("unlink nlink check: {e}")))?;

        if new_nlink == 0 {
            tx.execute("DELETE FROM fs_data WHERE ino=?1", [child_ino])
                .map_err(|e| VfsError::Other(format!("unlink data: {e}")))?;
            tx.execute("DELETE FROM fs_xattr WHERE ino=?1", [child_ino])
                .map_err(|e| VfsError::Other(format!("unlink xattr: {e}")))?;
            tx.execute("DELETE FROM fs_symlink WHERE ino=?1", [child_ino])
                .map_err(|e| VfsError::Other(format!("unlink symlink: {e}")))?;
            tx.execute("DELETE FROM fs_inode WHERE ino=?1", [child_ino])
                .map_err(|e| VfsError::Other(format!("unlink inode: {e}")))?;
            // Clean up FTS5 index entry for deleted inode.
            tx.execute("DELETE FROM fs_fts WHERE rowid=?1", [child_ino]).ok();
        }

        Self::update_times(&tx, parent, false, true, true)?;
        tx.commit().map_err(|e| VfsError::Other(format!("unlink commit: {e}")))?;
        Ok(())
    }

    fn rmdir(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("rmdir begin: {e}")))?;

        let child_ino: i64 = tx
            .query_row(
                "SELECT ino FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, name_str],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("rmdir lookup: {e}")))?
            .ok_or(VfsError::NotFound)?;

        let attr = Self::get_inode(&tx, child_ino as u64)?;
        if attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        // Check directory is empty.
        let child_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1",
                [child_ino],
                |row| row.get(0),
            )
            .map_err(|e| VfsError::Other(format!("rmdir empty check: {e}")))?;
        if child_count > 0 {
            return Err(VfsError::NotEmpty);
        }

        // Remove dentry.
        tx.execute(
            "DELETE FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
            params![parent as i64, name_str],
        )
        .map_err(|e| VfsError::Other(format!("rmdir dentry: {e}")))?;

        // Remove inode.
        tx.execute("DELETE FROM fs_xattr WHERE ino=?1", [child_ino])
            .map_err(|e| VfsError::Other(format!("rmdir xattr: {e}")))?;
        tx.execute("DELETE FROM fs_inode WHERE ino=?1", [child_ino])
            .map_err(|e| VfsError::Other(format!("rmdir inode: {e}")))?;
        // Clean up FTS5 index entry for deleted inode.
        tx.execute("DELETE FROM fs_fts WHERE rowid=?1", [child_ino]).ok();

        // Decrement parent nlink (subdirectory's ".." link removed).
        tx.execute(
            "UPDATE fs_inode SET nlink=CASE WHEN nlink>0 THEN nlink-1 ELSE 0 END WHERE ino=?1",
            [parent as i64],
        )
        .map_err(|e| VfsError::Other(format!("rmdir parent nlink: {e}")))?;

        Self::update_times(&tx, parent, false, true, true)?;
        tx.commit().map_err(|e| VfsError::Other(format!("rmdir commit: {e}")))?;
        Ok(())
    }

    fn rename(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        _flags: u32,
    ) -> VfsResult<()> {
        let old_name = name_to_str(name)?;
        let new_name = name_to_str(newname)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("rename begin: {e}")))?;

        // Find source inode.
        let src_ino: i64 = tx
            .query_row(
                "SELECT ino FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, old_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("rename src: {e}")))?
            .ok_or(VfsError::NotFound)?;

        let src_attr = Self::get_inode(&tx, src_ino as u64)?;
        let src_is_dir = src_attr.kind == FileType::Directory;

        // Check if target exists.
        let replaced_ino: Option<i64> = tx
            .query_row(
                "SELECT ino FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![newparent as i64, new_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("rename dest: {e}")))?;

        // If target exists, handle replacement.
        if let Some(replaced) = replaced_ino {
            let replaced_attr = Self::get_inode(&tx, replaced as u64)?;
            let replaced_is_dir = replaced_attr.kind == FileType::Directory;

            // POSIX: cannot replace directory with non-directory and vice versa.
            if src_is_dir && !replaced_is_dir {
                return Err(VfsError::NotADirectory);
            }
            if !src_is_dir && replaced_is_dir {
                return Err(VfsError::IsADirectory);
            }

            // If replacing a directory, it must be empty.
            if replaced_is_dir {
                let child_count: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1",
                        [replaced],
                        |row| row.get(0),
                    )
                    .map_err(|e| VfsError::Other(format!("rename empty check: {e}")))?;
                if child_count > 0 {
                    return Err(VfsError::NotEmpty);
                }
            }

            // Decrement nlink.
            tx.execute(
                "UPDATE fs_inode SET nlink=CASE WHEN nlink>0 THEN nlink-1 ELSE 0 END WHERE ino=?1",
                [replaced],
            )
            .map_err(|e| VfsError::Other(format!("rename replace nlink: {e}")))?;

            let new_nlink: u32 = tx
                .query_row(
                    "SELECT nlink FROM fs_inode WHERE ino=?1",
                    [replaced],
                    |row| row.get(0),
                )
                .map_err(|e| VfsError::Other(format!("rename nlink check: {e}")))?;

            if new_nlink == 0 {
                // Delete child dentries (shouldn't exist if we checked empty, but be safe).
                tx.execute("DELETE FROM fs_dentry WHERE parent_ino=?1", [replaced])
                    .map_err(|e| VfsError::Other(format!("rename del children: {e}")))?;
                tx.execute("DELETE FROM fs_data WHERE ino=?1", [replaced])
                    .map_err(|e| VfsError::Other(format!("rename del data: {e}")))?;
                tx.execute("DELETE FROM fs_xattr WHERE ino=?1", [replaced])
                    .map_err(|e| VfsError::Other(format!("rename del xattr: {e}")))?;
                tx.execute("DELETE FROM fs_symlink WHERE ino=?1", [replaced])
                    .map_err(|e| VfsError::Other(format!("rename del symlink: {e}")))?;
                tx.execute("DELETE FROM fs_inode WHERE ino=?1", [replaced])
                    .map_err(|e| VfsError::Other(format!("rename del inode: {e}")))?;
                // Clean up FTS5 index entry for deleted inode.
                tx.execute("DELETE FROM fs_fts WHERE rowid=?1", [replaced]).ok();
            }

            // Remove old target dentry.
            tx.execute(
                "DELETE FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![newparent as i64, new_name],
            )
            .map_err(|e| VfsError::Other(format!("rename del target dentry: {e}")))?;

            // If replaced was a directory, newparent loses a ".." link.
            if replaced_is_dir {
                tx.execute(
                    "UPDATE fs_inode SET nlink=CASE WHEN nlink>0 THEN nlink-1 ELSE 0 END WHERE ino=?1",
                    [newparent as i64],
                )
                .map_err(|e| VfsError::Other(format!("rename replaced dir nlink: {e}")))?;
            }
        }

        // Remove old dentry.
        tx.execute(
            "DELETE FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
            params![parent as i64, old_name],
        )
        .map_err(|e| VfsError::Other(format!("rename del src dentry: {e}")))?;

        // Insert new dentry.
        tx.execute(
            "INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?1, ?2, ?3)",
            params![newparent as i64, new_name, src_ino],
        )
        .map_err(|e| VfsError::Other(format!("rename new dentry: {e}")))?;

        // Update parent nlinks when moving a directory across parents.
        if src_is_dir && parent != newparent {
            // Old parent loses ".." link from moved directory.
            tx.execute(
                "UPDATE fs_inode SET nlink=CASE WHEN nlink>0 THEN nlink-1 ELSE 0 END WHERE ino=?1",
                [parent as i64],
            )
            .map_err(|e| VfsError::Other(format!("rename src parent nlink: {e}")))?;
            // New parent gains ".." link from moved directory.
            tx.execute(
                "UPDATE fs_inode SET nlink=nlink+1 WHERE ino=?1",
                [newparent as i64],
            )
            .map_err(|e| VfsError::Other(format!("rename dst parent nlink: {e}")))?;
        }

        // Update timestamps.
        Self::update_times(&tx, parent, false, true, true)?;
        if parent != newparent {
            Self::update_times(&tx, newparent, false, true, true)?;
        }

        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);
        tx.execute(
            "UPDATE fs_inode SET ctime_s=?2, ctime_ns=?3 WHERE ino=?1",
            params![src_ino, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("rename ctime: {e}")))?;

        // Update FTS5 index path for moved inode (if it has an entry).
        let new_path = Self::build_path(&tx, src_ino as u64);
        let tags = Self::get_tags_string(&tx, src_ino as u64);
        let desc = Self::get_description(&tx, src_ino as u64);
        if !tags.is_empty() || !desc.is_empty() {
            Self::upsert_fts(&tx, src_ino as u64, &new_path, &tags, &desc)?;
        }

        tx.commit().map_err(|e| VfsError::Other(format!("rename commit: {e}")))?;
        Ok(())
    }

    fn opendir(&self, ino: InodeId, _flags: i32) -> VfsResult<FileHandle> {
        let conn = self.read_lock();
        let attr = Self::get_inode(&conn, ino)?;
        if attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }
        Ok(0)
    }

    fn readdir(&self, ino: InodeId, _fh: FileHandle, offset: i64) -> VfsResult<Vec<DirEntry>> {
        let conn = self.read_lock();
        let attr = Self::get_inode(&conn, ino)?;
        if attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        let mut entries = Vec::new();

        // Find parent for ".." — look up a dentry pointing to this ino.
        let parent_ino: u64 = conn
            .query_row(
                "SELECT parent_ino FROM fs_dentry WHERE ino=?1 LIMIT 1",
                [ino as i64],
                |row| row.get::<_, i64>(0).map(|v| v as u64),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("readdir parent: {e}")))?
            .unwrap_or(ino); // root's parent is itself

        entries.push(DirEntry {
            ino,
            name: ".".to_string(),
            kind: FileType::Directory,
        });
        entries.push(DirEntry {
            ino: parent_ino,
            name: "..".to_string(),
            kind: FileType::Directory,
        });

        // Children.
        let mut stmt = conn
            .prepare_cached("SELECT name, d.ino, i.mode FROM fs_dentry d JOIN fs_inode i ON d.ino=i.ino WHERE d.parent_ino=?1 ORDER BY d.name")
            .map_err(|e| VfsError::Other(format!("readdir prepare: {e}")))?;

        let rows = stmt
            .query_map([ino as i64], |row| {
                let name: String = row.get(0)?;
                let child_ino: i64 = row.get(1)?;
                let mode: u32 = row.get(2)?;
                Ok(DirEntry {
                    ino: child_ino as u64,
                    name,
                    kind: mode_to_kind(mode),
                })
            })
            .map_err(|e| VfsError::Other(format!("readdir query: {e}")))?;

        for row in rows {
            entries.push(row.map_err(|e| VfsError::Other(format!("readdir row: {e}")))?);
        }

        // Apply offset.
        let offset = offset.max(0) as usize;
        if offset < entries.len() {
            Ok(entries[offset..].to_vec())
        } else {
            Ok(Vec::new())
        }
    }

    fn releasedir(&self, _ino: InodeId, _fh: FileHandle, _flags: i32) -> VfsResult<()> {
        Ok(())
    }

    // ── Symlinks & hardlinks ─────────────────────────────────────

    fn symlink(
        &self,
        parent: InodeId,
        link_name: &OsStr,
        target: &Path,
        uid: u32,
        gid: u32,
    ) -> VfsResult<VfsAttr> {
        let name_str = name_to_str(link_name)?;
        let target_str = target
            .to_str()
            .ok_or(VfsError::InvalidArgument)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("symlink begin: {e}")))?;

        let parent_attr = Self::get_inode(&tx, parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        let exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![parent as i64, name_str],
                |row| row.get::<_, i64>(0).map(|c| c > 0),
            )
            .map_err(|e| VfsError::Other(format!("symlink check: {e}")))?;
        if exists {
            return Err(VfsError::AlreadyExists);
        }

        let ino = self.alloc_ino();
        let mode = (libc::S_IFLNK as u32) | 0o777;
        let target_len = target_str.len() as i64;
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);

        tx.execute(
            "INSERT INTO fs_inode (ino, mode, nlink, uid, gid, size, rdev, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns, crtime_s, crtime_ns) VALUES (?1,?2,1,?3,?4,?5,0,?6,?7,?6,?7,?6,?7,?6,?7)",
            params![ino as i64, mode, uid, gid, target_len, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("symlink insert inode: {e}")))?;

        tx.execute(
            "INSERT INTO fs_symlink (ino, target) VALUES (?1, ?2)",
            params![ino as i64, target_str],
        )
        .map_err(|e| VfsError::Other(format!("symlink insert target: {e}")))?;

        tx.execute(
            "INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?1, ?2, ?3)",
            params![parent as i64, name_str, ino as i64],
        )
        .map_err(|e| VfsError::Other(format!("symlink insert dentry: {e}")))?;

        Self::update_times(&tx, parent, false, true, true)?;

        let attr = Self::get_inode(&tx, ino)?;
        tx.commit().map_err(|e| VfsError::Other(format!("symlink commit: {e}")))?;
        Ok(attr)
    }

    fn readlink(&self, ino: InodeId) -> VfsResult<Vec<u8>> {
        let conn = self.read_lock();
        let target: String = conn
            .query_row(
                "SELECT target FROM fs_symlink WHERE ino=?1",
                [ino as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("readlink: {e}")))?
            .ok_or_else(|| VfsError::Other(format!("readlink: no target for ino {ino}")))?;
        Ok(target.into_bytes())
    }

    fn link(&self, ino: InodeId, newparent: InodeId, newname: &OsStr) -> VfsResult<VfsAttr> {
        let name_str = name_to_str(newname)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("link begin: {e}")))?;

        // Verify source exists and is not a directory.
        let attr = Self::get_inode(&tx, ino)?;
        if attr.kind == FileType::Directory {
            return Err(VfsError::PermissionDenied);
        }

        // Check parent is a directory.
        let parent_attr = Self::get_inode(&tx, newparent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(VfsError::NotADirectory);
        }

        // Check name doesn't already exist.
        let exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM fs_dentry WHERE parent_ino=?1 AND name=?2",
                params![newparent as i64, name_str],
                |row| row.get::<_, i64>(0).map(|c| c > 0),
            )
            .map_err(|e| VfsError::Other(format!("link check: {e}")))?;
        if exists {
            return Err(VfsError::AlreadyExists);
        }

        // Insert dentry.
        tx.execute(
            "INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?1, ?2, ?3)",
            params![newparent as i64, name_str, ino as i64],
        )
        .map_err(|e| VfsError::Other(format!("link dentry: {e}")))?;

        // Increment nlink.
        let now = SystemTime::now();
        let (s, ns) = systime_to_secs_nanos(now);
        tx.execute(
            "UPDATE fs_inode SET nlink=nlink+1, ctime_s=?2, ctime_ns=?3 WHERE ino=?1",
            params![ino as i64, s, ns],
        )
        .map_err(|e| VfsError::Other(format!("link nlink: {e}")))?;

        Self::update_times(&tx, newparent, false, true, true)?;

        let result = Self::get_inode(&tx, ino)?;
        tx.commit().map_err(|e| VfsError::Other(format!("link commit: {e}")))?;
        Ok(result)
    }

    // ── Extended attributes ──────────────────────────────────────

    fn getxattr(&self, ino: InodeId, name: &OsStr, size: u32) -> VfsResult<Vec<u8>> {
        let name_str = name_to_str(name)?;
        let conn = self.read_lock();

        let value: Vec<u8> = conn
            .query_row(
                "SELECT value FROM fs_xattr WHERE ino=?1 AND name=?2",
                params![ino as i64, name_str],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VfsError::Other(format!("getxattr: {e}")))?
            .ok_or(VfsError::NoXattr)?;

        if size == 0 {
            let len = value.len() as u32;
            Ok(len.to_ne_bytes().to_vec())
        } else if (size as usize) < value.len() {
            Err(VfsError::XattrRange)
        } else {
            Ok(value)
        }
    }

    fn setxattr(&self, ino: InodeId, name: &OsStr, value: &[u8], _flags: i32) -> VfsResult<()> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();

        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("setxattr begin: {e}")))?;

        // Ensure inode exists.
        Self::get_inode(&tx, ino)?;

        tx.execute(
            "INSERT OR REPLACE INTO fs_xattr (ino, name, value) VALUES (?1, ?2, ?3)",
            params![ino as i64, name_str, value],
        )
        .map_err(|e| VfsError::Other(format!("setxattr: {e}")))?;

        Self::update_times(&tx, ino, false, false, true)?;

        // Update FTS5 if this is a tag xattr.
        if name_str.starts_with("user.zodaix.") {
            let path = Self::build_path(&tx, ino);
            let tags = Self::get_tags_string(&tx, ino);
            let desc = Self::get_description(&tx, ino);
            Self::upsert_fts(&tx, ino, &path, &tags, &desc)?;
        }

        tx.commit().map_err(|e| VfsError::Other(format!("setxattr commit: {e}")))?;
        Ok(())
    }

    fn listxattr(&self, ino: InodeId, size: u32) -> VfsResult<Vec<u8>> {
        let conn = self.read_lock();

        // Ensure inode exists.
        Self::get_inode(&conn, ino)?;

        let mut stmt = conn
            .prepare_cached("SELECT name FROM fs_xattr WHERE ino=?1")
            .map_err(|e| VfsError::Other(format!("listxattr prepare: {e}")))?;

        let names: Vec<String> = stmt
            .query_map([ino as i64], |row| row.get(0))
            .map_err(|e| VfsError::Other(format!("listxattr query: {e}")))?
            .collect::<Result<Vec<String>, _>>()
            .map_err(|e| VfsError::Other(format!("listxattr collect: {e}")))?;

        let mut buf = Vec::new();
        for name in &names {
            buf.extend_from_slice(name.as_bytes());
            buf.push(0);
        }

        if size == 0 {
            let len = buf.len() as u32;
            Ok(len.to_ne_bytes().to_vec())
        } else if (size as usize) < buf.len() {
            Err(VfsError::XattrRange)
        } else {
            Ok(buf)
        }
    }

    fn removexattr(&self, ino: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = name_to_str(name)?;
        let conn = self.write_conn.lock();
        let tx = conn.unchecked_transaction()
            .map_err(|e| VfsError::Other(format!("removexattr begin: {e}")))?;

        let changes = tx
            .execute(
                "DELETE FROM fs_xattr WHERE ino=?1 AND name=?2",
                params![ino as i64, name_str],
            )
            .map_err(|e| VfsError::Other(format!("removexattr: {e}")))?;

        if changes == 0 {
            return Err(VfsError::NoXattr);
        }

        Self::update_times(&tx, ino, false, false, true)?;

        // Update FTS5 index when a zodaix xattr is removed.
        if name_str.starts_with("user.zodaix.") {
            let tags = Self::get_tags_string(&tx, ino);
            let desc = Self::get_description(&tx, ino);
            if tags.is_empty() && desc.is_empty() {
                // No zodaix metadata left — remove FTS5 entry entirely.
                tx.execute("DELETE FROM fs_fts WHERE rowid=?1", [ino as i64]).ok();
            } else {
                let path = Self::build_path(&tx, ino);
                Self::upsert_fts(&tx, ino, &path, &tags, &desc)?;
            }
        }

        tx.commit().map_err(|e| VfsError::Other(format!("removexattr commit: {e}")))?;
        Ok(())
    }

    // ── Filesystem info ──────────────────────────────────────────

    fn statfs(&self, _ino: InodeId) -> VfsResult<StatFs> {
        let conn = self.read_lock();
        let file_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fs_inode", [], |row| row.get(0))
            .unwrap_or(0);

        Ok(StatFs {
            blocks: 1_000_000,
            bfree: 500_000,
            bavail: 500_000,
            files: file_count as u64 + 1_000_000,
            ffree: 1_000_000,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }

    // ── Access check ─────────────────────────────────────────────

    fn access(&self, ino: InodeId, mask: i32) -> VfsResult<()> {
        let conn = self.read_lock();
        let attr = Self::get_inode(&conn, ino)?;

        // F_OK: just check existence.
        if mask == libc::F_OK {
            return Ok(());
        }

        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();

        // Root can access anything.
        if uid == 0 {
            return Ok(());
        }

        let perm = attr.perm as u32;
        let effective = if uid == attr.uid {
            (perm >> 6) & 0o7
        } else if gid == attr.gid {
            (perm >> 3) & 0o7
        } else {
            perm & 0o7
        };

        if mask & libc::R_OK != 0 && effective & 0o4 == 0 {
            return Err(VfsError::PermissionDenied);
        }
        if mask & libc::W_OK != 0 && effective & 0o2 == 0 {
            return Err(VfsError::PermissionDenied);
        }
        if mask & libc::X_OK != 0 && effective & 0o1 == 0 {
            return Err(VfsError::PermissionDenied);
        }

        Ok(())
    }

    // ── Search ───────────────────────────────────────────────────

    fn search(&self, query: &str, limit: usize) -> VfsResult<Vec<SearchResult>> {
        // Reject empty/blank queries (FTS5 would return a syntax error).
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let limit = limit.min(10_000); // Cap to prevent unbounded result allocation.
        // Quote user input to force literal matching and prevent FTS5 operator injection.
        let safe_query = format!("\"{}\"", query.replace('"', "\"\""));
        let conn = self.read_lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT rowid, path, tags, description, rank FROM fs_fts WHERE fs_fts MATCH ?1 ORDER BY rank LIMIT ?2",
            )
            .map_err(|e| VfsError::Other(format!("search prepare: {e}")))?;

        let results = stmt
            .query_map(params![safe_query, limit as i64], |row| {
                let ino: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                let tags_str: String = row.get(2)?;
                let description: String = row.get(3)?;
                let rank: f64 = row.get(4)?;
                Ok(SearchResult {
                    ino: ino as u64,
                    path,
                    tags: tags_str
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(|s| s.trim().to_string())
                        .collect(),
                    description,
                    score: (-rank) as f32, // FTS5 rank is negative
                })
            })
            .map_err(|e| VfsError::Other(format!("search query: {e}")))?;

        let mut out = Vec::new();
        for r in results {
            out.push(r.map_err(|e| VfsError::Other(format!("search row: {e}")))?);
        }
        Ok(out)
    }
}

impl Drop for SqliteBackend {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// ── FTS5 helpers ─────────────────────────────────────────────────

impl SqliteBackend {
    fn get_tags_string(conn: &Connection, ino: u64) -> String {
        conn.query_row(
            "SELECT value FROM fs_xattr WHERE ino=?1 AND name='user.zodaix.tags'",
            [ino as i64],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(String::from_utf8_lossy(&bytes).to_string())
            },
        )
        .unwrap_or_default()
    }

    fn get_description(conn: &Connection, ino: u64) -> String {
        conn.query_row(
            "SELECT value FROM fs_xattr WHERE ino=?1 AND name='user.zodaix.description'",
            [ino as i64],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(String::from_utf8_lossy(&bytes).to_string())
            },
        )
        .unwrap_or_default()
    }

    fn upsert_fts(conn: &Connection, ino: u64, path: &str, tags: &str, desc: &str) -> VfsResult<()> {
        // Delete old entry if exists.
        conn.execute("DELETE FROM fs_fts WHERE rowid = ?1", [ino as i64])
            .ok();

        conn.execute(
            "INSERT INTO fs_fts(rowid, path, tags, description) VALUES(?1, ?2, ?3, ?4)",
            params![ino as i64, path, tags, desc],
        )
        .map_err(|e| VfsError::Other(format!("fts insert: {e}")))?;

        Ok(())
    }
}
