use crate::reply::{to_fuser_attr, to_fuser_file_type, TTL};
use fuser::{
    FileHandle as FuserFh, Filesystem, FopenFlags, Generation, INodeNo, KernelConfig, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs,
    ReplyWrite, ReplyXattr, Request,
};
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use tracing::debug;
use zodaix_core::{SetAttrParams, VfsBackend};

/// Convert VfsError errno (i32) to fuser Errno.
fn to_fuser_errno(errno: i32) -> fuser::Errno {
    fuser::Errno::from_i32(errno)
}

/// FUSE bridge: delegates fuser::Filesystem calls to a VfsBackend.
pub struct ZodaixFuse {
    backend: Box<dyn VfsBackend>,
}

impl ZodaixFuse {
    pub fn new(backend: Box<dyn VfsBackend>) -> Self {
        Self { backend }
    }
}

impl Filesystem for ZodaixFuse {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> io::Result<()> {
        debug!("FUSE init");
        Ok(())
    }

    fn destroy(&mut self) {
        debug!("FUSE destroy");
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self.backend.lookup(u64::from(parent), name) {
            Ok(attr) => reply.entry(&TTL, &to_fuser_attr(&attr), Generation(0)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FuserFh>, reply: ReplyAttr) {
        match self.backend.getattr(u64::from(ino)) {
            Ok(attr) => reply.attr(&TTL, &to_fuser_attr(&attr)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FuserFh>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let now = std::time::SystemTime::now();
        let params = SetAttrParams {
            mode,
            uid,
            gid,
            size,
            atime: atime.map(|t| match t {
                fuser::TimeOrNow::SpecificTime(st) => st,
                fuser::TimeOrNow::Now => now,
            }),
            mtime: mtime.map(|t| match t {
                fuser::TimeOrNow::SpecificTime(st) => st,
                fuser::TimeOrNow::Now => now,
            }),
        };
        match self.backend.setattr(u64::from(ino), params) {
            Ok(attr) => reply.attr(&TTL, &to_fuser_attr(&attr)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: ReplyOpen) {
        match self.backend.open(u64::from(ino), flags.0) {
            Ok(fh) => reply.opened(FuserFh(fh), FopenFlags::empty()),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        match self
            .backend
            .read(u64::from(ino), u64::from(fh), offset as i64, size)
        {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        match self
            .backend
            .write(u64::from(ino), u64::from(fh), offset as i64, data, flags.0)
        {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.backend.flush(u64::from(ino), u64::from(fh)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        match self
            .backend
            .release(u64::from(ino), u64::from(fh), flags.0, flush)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, fh: FuserFh, datasync: bool, reply: ReplyEmpty) {
        match self.backend.fsync(u64::from(ino), u64::from(fh), datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        match self.backend.mknod(
            u64::from(parent),
            name,
            mode,
            umask,
            req.uid(),
            req.gid(),
            rdev,
        ) {
            Ok(attr) => reply.entry(&TTL, &to_fuser_attr(&attr), Generation(0)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        match self
            .backend
            .mkdir(u64::from(parent), name, mode, umask, req.uid(), req.gid())
        {
            Ok(attr) => reply.entry(&TTL, &to_fuser_attr(&attr), Generation(0)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.backend.unlink(u64::from(parent), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.backend.rmdir(u64::from(parent), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        match self.backend.rename(
            u64::from(parent),
            name,
            u64::from(newparent),
            newname,
            flags.bits(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: ReplyOpen) {
        match self.backend.opendir(u64::from(ino), flags.0) {
            Ok(fh) => reply.opened(FuserFh(fh), FopenFlags::empty()),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        match self
            .backend
            .readdir(u64::from(ino), u64::from(fh), offset as i64)
        {
            Ok(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let full_offset = offset + i as u64 + 1;
                    let buffer_full = reply.add(
                        INodeNo(entry.ino),
                        full_offset,
                        to_fuser_file_type(entry.kind),
                        &entry.name,
                    );
                    if buffer_full {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        flags: fuser::OpenFlags,
        reply: ReplyEmpty,
    ) {
        match self
            .backend
            .releasedir(u64::from(ino), u64::from(fh), flags.0)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        match self
            .backend
            .symlink(u64::from(parent), link_name, target, req.uid(), req.gid())
        {
            Ok(attr) => reply.entry(&TTL, &to_fuser_attr(&attr), Generation(0)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.backend.readlink(u64::from(ino)) {
            Ok(target) => reply.data(&target),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        match self
            .backend
            .link(u64::from(ino), u64::from(newparent), newname)
        {
            Ok(attr) => reply.entry(&TTL, &to_fuser_attr(&attr), Generation(0)),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        match self.backend.getxattr(u64::from(ino), name, size) {
            Ok(data) => {
                if size == 0 {
                    let actual_size = if data.len() == 4 {
                        u32::from_ne_bytes(data[..4].try_into().unwrap())
                    } else {
                        data.len() as u32
                    };
                    reply.size(actual_size);
                } else {
                    reply.data(&data);
                }
            }
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        match self.backend.setxattr(u64::from(ino), name, value, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.backend.listxattr(u64::from(ino), size) {
            Ok(data) => {
                if size == 0 {
                    let actual_size = if data.len() == 4 {
                        u32::from_ne_bytes(data[..4].try_into().unwrap())
                    } else {
                        data.len() as u32
                    };
                    reply.size(actual_size);
                } else {
                    reply.data(&data);
                }
            }
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.backend.removexattr(u64::from(ino), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn statfs(&self, _req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        match self.backend.statfs(u64::from(ino)) {
            Ok(st) => crate::reply::reply_statfs(reply, &st),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn access(&self, _req: &Request, ino: INodeNo, mask: fuser::AccessFlags, reply: ReplyEmpty) {
        match self.backend.access(u64::from(ino), mask.bits()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        match self.backend.mknod(
            u64::from(parent),
            name,
            mode,
            umask,
            req.uid(),
            req.gid(),
            0,
        ) {
            Ok(attr) => match self.backend.open(attr.ino, flags) {
                Ok(fh) => reply.created(
                    &TTL,
                    &to_fuser_attr(&attr),
                    Generation(0),
                    FuserFh(fh),
                    FopenFlags::empty(),
                ),
                Err(e) => {
                    // Rollback: remove the created file since create should be atomic.
                    let _ = self.backend.unlink(u64::from(parent), name);
                    reply.error(to_fuser_errno(e.to_errno()));
                }
            },
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FuserFh,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        match self.backend.fallocate(
            u64::from(ino),
            u64::from(fh),
            offset as i64,
            length as i64,
            mode,
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_fuser_errno(e.to_errno())),
        }
    }
}
