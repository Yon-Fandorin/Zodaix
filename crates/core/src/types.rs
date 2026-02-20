use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

bitflags! {
    /// Capabilities advertised by a backend.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BackendCapabilities: u32 {
        const HARDLINKS  = 1;
        const SYMLINKS   = 2;
        const XATTRS     = 4;
        const SEARCH     = 16;
        const PERSISTENT = 32;
    }
}

/// A single search result returned by `VfsBackend::search`.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub ino: InodeId,
    pub path: String,
    pub tags: Vec<String>,
    pub description: String,
    pub score: f32,
}

/// Inode identifier (matches FUSE inode numbering, root = 1).
pub type InodeId = u64;

/// File handle identifier returned by open/opendir.
pub type FileHandle = u64;

/// Root inode, always 1 per FUSE convention.
pub const ROOT_INO: InodeId = 1;

/// File type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

impl FileType {
    pub fn as_mode_bits(self) -> u32 {
        match self {
            FileType::RegularFile => libc::S_IFREG as u32,
            FileType::Directory => libc::S_IFDIR as u32,
            FileType::Symlink => libc::S_IFLNK as u32,
        }
    }
}

/// VFS file attributes (platform-independent superset of stat).
#[derive(Debug, Clone)]
pub struct VfsAttr {
    pub ino: InodeId,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

impl VfsAttr {
    /// Create default attributes for a new file.
    pub fn new_file(ino: InodeId, perm: u16, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        Self {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
        }
    }

    /// Create default attributes for a new directory.
    pub fn new_dir(ino: InodeId, perm: u16, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        Self {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm,
            nlink: 2,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
        }
    }

    /// Create default attributes for a new symlink.
    pub fn new_symlink(ino: InodeId, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        Self {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Symlink,
            perm: 0o777,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
        }
    }
}

/// Directory entry for readdir responses.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: InodeId,
    pub name: String,
    pub kind: FileType,
}

/// Parameters for setattr — each field is `Some` only if that attribute should change.
#[derive(Debug, Default)]
pub struct SetAttrParams {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}
