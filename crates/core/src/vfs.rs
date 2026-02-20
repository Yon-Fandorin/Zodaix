use crate::error::VfsResult;
use crate::types::*;
use std::ffi::OsStr;
use std::path::Path;

/// Core VFS backend trait.
///
/// Inode-based design that maps directly to FUSE semantics.
/// All methods are synchronous (fuser uses sync callbacks).
/// Implementations must be `Send + Sync` for multi-threaded FUSE dispatch.
pub trait VfsBackend: Send + Sync {
    // ── Lifecycle ─────────────────────────────────────────────────

    /// Human-readable backend name (e.g. "memory", "sqlite").
    fn name(&self) -> &str;

    /// Initialize the backend (called once before use).
    fn init(&self) -> VfsResult<()> {
        Ok(())
    }

    /// Gracefully shut down the backend.
    fn shutdown(&self) -> VfsResult<()> {
        Ok(())
    }

    /// Advertised capabilities of this backend.
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::all()
    }

    /// Full-text search (requires `SEARCH` capability).
    fn search(&self, _query: &str, _limit: usize) -> VfsResult<Vec<SearchResult>> {
        Err(crate::error::VfsError::NotSupported)
    }

    // ── Lookup & attributes ──────────────────────────────────────

    /// Look up a directory entry by name under `parent`.
    fn lookup(&self, parent: InodeId, name: &OsStr) -> VfsResult<VfsAttr>;

    /// Get attributes of an inode.
    fn getattr(&self, ino: InodeId) -> VfsResult<VfsAttr>;

    /// Set attributes on an inode.
    fn setattr(&self, ino: InodeId, params: SetAttrParams) -> VfsResult<VfsAttr>;

    // ── File I/O ─────────────────────────────────────────────────

    /// Open a file and return a file handle.
    fn open(&self, ino: InodeId, flags: i32) -> VfsResult<FileHandle>;

    /// Read data from a file.
    fn read(&self, ino: InodeId, fh: FileHandle, offset: i64, size: u32) -> VfsResult<Vec<u8>>;

    /// Write data to a file. Returns bytes written.
    fn write(
        &self,
        ino: InodeId,
        fh: FileHandle,
        offset: i64,
        data: &[u8],
        flags: i32,
    ) -> VfsResult<u32>;

    /// Flush a file handle.
    fn flush(&self, ino: InodeId, fh: FileHandle) -> VfsResult<()>;

    /// Release (close) a file handle.
    fn release(&self, ino: InodeId, fh: FileHandle, flags: i32, flush: bool) -> VfsResult<()>;

    /// Synchronize file contents.
    fn fsync(&self, ino: InodeId, fh: FileHandle, datasync: bool) -> VfsResult<()>;

    // ── Directory ────────────────────────────────────────────────

    /// Create a file node. Returns the new file's attributes.
    #[allow(clippy::too_many_arguments)]
    fn mknod(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> VfsResult<VfsAttr>;

    /// Create a directory. Returns the new directory's attributes.
    fn mkdir(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
    ) -> VfsResult<VfsAttr>;

    /// Remove a file.
    fn unlink(&self, parent: InodeId, name: &OsStr) -> VfsResult<()>;

    /// Remove a directory.
    fn rmdir(&self, parent: InodeId, name: &OsStr) -> VfsResult<()>;

    /// Rename a file/directory.
    fn rename(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        flags: u32,
    ) -> VfsResult<()>;

    /// Open a directory and return a file handle.
    fn opendir(&self, ino: InodeId, flags: i32) -> VfsResult<FileHandle>;

    /// Read directory entries (includes `.` and `..`).
    fn readdir(&self, ino: InodeId, fh: FileHandle, offset: i64) -> VfsResult<Vec<DirEntry>>;

    /// Release (close) a directory handle.
    fn releasedir(&self, ino: InodeId, fh: FileHandle, flags: i32) -> VfsResult<()>;

    // ── Symlinks & hardlinks ─────────────────────────────────────

    /// Create a symlink. Returns the new symlink's attributes.
    fn symlink(
        &self,
        parent: InodeId,
        link_name: &OsStr,
        target: &Path,
        uid: u32,
        gid: u32,
    ) -> VfsResult<VfsAttr>;

    /// Read the target of a symlink.
    fn readlink(&self, ino: InodeId) -> VfsResult<Vec<u8>>;

    /// Create a hard link. Returns the linked inode's updated attributes.
    fn link(&self, ino: InodeId, newparent: InodeId, newname: &OsStr) -> VfsResult<VfsAttr>;

    // ── Extended attributes ──────────────────────────────────────

    /// Get an extended attribute value.
    fn getxattr(&self, ino: InodeId, name: &OsStr, size: u32) -> VfsResult<Vec<u8>>;

    /// Set an extended attribute.
    fn setxattr(&self, ino: InodeId, name: &OsStr, value: &[u8], flags: i32) -> VfsResult<()>;

    /// List extended attribute names.
    fn listxattr(&self, ino: InodeId, size: u32) -> VfsResult<Vec<u8>>;

    /// Remove an extended attribute.
    fn removexattr(&self, ino: InodeId, name: &OsStr) -> VfsResult<()>;

    // ── Filesystem info ──────────────────────────────────────────

    /// Get filesystem statistics.
    fn statfs(&self, ino: InodeId) -> VfsResult<StatFs>;

    // ── Truncate ─────────────────────────────────────────────────

    /// Allocate/deallocate space (fallocate).
    fn fallocate(
        &self,
        ino: InodeId,
        fh: FileHandle,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> VfsResult<()> {
        let _ = (ino, fh, offset, length, mode);
        Err(crate::error::VfsError::NotSupported)
    }

    // ── Access check ─────────────────────────────────────────────

    /// Check file access permissions.
    fn access(&self, ino: InodeId, mask: i32) -> VfsResult<()>;
}

/// Filesystem statistics (maps to statvfs).
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Total data blocks
    pub blocks: u64,
    /// Free blocks
    pub bfree: u64,
    /// Free blocks for unprivileged users
    pub bavail: u64,
    /// Total inodes
    pub files: u64,
    /// Free inodes
    pub ffree: u64,
    /// Filesystem block size
    pub bsize: u32,
    /// Maximum name length
    pub namelen: u32,
    /// Fragment size
    pub frsize: u32,
}

impl Default for StatFs {
    fn default() -> Self {
        Self {
            blocks: 1_000_000,
            bfree: 500_000,
            bavail: 500_000,
            files: 1_000_000,
            ffree: 500_000,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        }
    }
}
