use fuser::INodeNo;
use std::time::Duration;
use zodaix_core::{FileType, StatFs, VfsAttr};

/// TTL for attribute caching (1 second).
pub const TTL: Duration = Duration::from_secs(1);

/// Convert VfsAttr to fuser::FileAttr.
pub fn to_fuser_attr(attr: &VfsAttr) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: INodeNo(attr.ino),
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: to_fuser_file_type(attr.kind),
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: attr.blksize,
        flags: 0,
    }
}

/// Convert VfsAttr FileType to fuser::FileType.
pub fn to_fuser_file_type(kind: FileType) -> fuser::FileType {
    match kind {
        FileType::RegularFile => fuser::FileType::RegularFile,
        FileType::Directory => fuser::FileType::Directory,
        FileType::Symlink => fuser::FileType::Symlink,
    }
}

/// Convert VFS StatFs to FUSE statvfs reply values.
pub fn reply_statfs(reply: fuser::ReplyStatfs, st: &StatFs) {
    reply.statfs(
        st.blocks, st.bfree, st.bavail, st.files, st.ffree, st.bsize, st.namelen, st.frsize,
    );
}
