use nfs3_server::nfs3_types::nfs3::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zodaix_core::{FileType, SetAttrParams, VfsAttr, VfsError};

/// Convert VfsAttr to NFS fattr3.
pub fn vfs_attr_to_fattr3(attr: &VfsAttr) -> fattr3 {
    fattr3 {
        type_: file_type_to_ftype3(attr.kind),
        mode: attr.perm as u32,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        used: attr.blocks * 512, // blocks are in 512-byte units
        rdev: specdata3 {
            specdata1: (attr.rdev >> 8) & 0xff,
            specdata2: attr.rdev & 0xff,
        },
        fsid: 0,
        fileid: attr.ino,
        atime: system_time_to_nfstime3(attr.atime),
        mtime: system_time_to_nfstime3(attr.mtime),
        ctime: system_time_to_nfstime3(attr.ctime),
    }
}

/// Convert NFS sattr3 to VfsBackend SetAttrParams.
pub fn sattr3_to_set_attr_params(sattr: &sattr3) -> SetAttrParams {
    SetAttrParams {
        mode: nfs3_option_to_option(&sattr.mode),
        uid: nfs3_option_to_option(&sattr.uid),
        gid: nfs3_option_to_option(&sattr.gid),
        size: nfs3_option_to_option(&sattr.size),
        atime: set_atime_to_option(&sattr.atime),
        mtime: set_mtime_to_option(&sattr.mtime),
    }
}

/// Convert FileType to NFS ftype3.
pub fn file_type_to_ftype3(ft: FileType) -> ftype3 {
    match ft {
        FileType::RegularFile => ftype3::NF3REG,
        FileType::Directory => ftype3::NF3DIR,
        FileType::Symlink => ftype3::NF3LNK,
    }
}

/// Convert VfsError to NFS nfsstat3.
pub fn vfs_error_to_nfsstat(e: &VfsError) -> nfsstat3 {
    match e {
        VfsError::NotFound => nfsstat3::NFS3ERR_NOENT,
        VfsError::AlreadyExists => nfsstat3::NFS3ERR_EXIST,
        VfsError::NotADirectory => nfsstat3::NFS3ERR_NOTDIR,
        VfsError::IsADirectory => nfsstat3::NFS3ERR_ISDIR,
        VfsError::NotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
        VfsError::PermissionDenied => nfsstat3::NFS3ERR_ACCES,
        VfsError::InvalidArgument => nfsstat3::NFS3ERR_INVAL,
        VfsError::NoSpace => nfsstat3::NFS3ERR_NOSPC,
        VfsError::NameTooLong => nfsstat3::NFS3ERR_NAMETOOLONG,
        VfsError::TooManyOpenFiles => nfsstat3::NFS3ERR_SERVERFAULT,
        VfsError::BadFileDescriptor => nfsstat3::NFS3ERR_BADHANDLE,
        VfsError::CrossDeviceLink => nfsstat3::NFS3ERR_XDEV,
        VfsError::NoXattr => nfsstat3::NFS3ERR_NOTSUPP,
        VfsError::XattrRange => nfsstat3::NFS3ERR_INVAL,
        VfsError::NotSupported => nfsstat3::NFS3ERR_NOTSUPP,
        VfsError::Io(_) => nfsstat3::NFS3ERR_IO,
        VfsError::Other(_) => nfsstat3::NFS3ERR_SERVERFAULT,
    }
}

/// Convert SystemTime to NFS nfstime3.
fn system_time_to_nfstime3(time: SystemTime) -> nfstime3 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(dur) => nfstime3 {
            // Clamp to u32::MAX to avoid silent truncation after Y2038.
            seconds: dur.as_secs().min(u32::MAX as u64) as u32,
            nseconds: dur.subsec_nanos(),
        },
        Err(_) => nfstime3 {
            seconds: 0,
            nseconds: 0,
        },
    }
}

/// Convert nfstime3 to SystemTime.
pub fn nfstime3_to_system_time(t: &nfstime3) -> SystemTime {
    UNIX_EPOCH + Duration::new(t.seconds as u64, t.nseconds)
}

/// Convert Nfs3Option<T> to Option<T>.
fn nfs3_option_to_option<T>(opt: &Nfs3Option<T>) -> Option<T>
where
    T: Copy + nfs3_server::nfs3_types::xdr_codec::Pack + nfs3_server::nfs3_types::xdr_codec::Unpack,
{
    match opt {
        Nfs3Option::Some(v) => Some(*v),
        Nfs3Option::None => None,
    }
}

/// Convert set_atime to Option<SystemTime>.
fn set_atime_to_option(sa: &set_atime) -> Option<SystemTime> {
    match sa {
        set_atime::DONT_CHANGE => None,
        set_atime::SET_TO_SERVER_TIME => Some(SystemTime::now()),
        set_atime::SET_TO_CLIENT_TIME(t) => Some(nfstime3_to_system_time(t)),
    }
}

/// Convert set_mtime to Option<SystemTime>.
fn set_mtime_to_option(sm: &set_mtime) -> Option<SystemTime> {
    match sm {
        set_mtime::DONT_CHANGE => None,
        set_mtime::SET_TO_SERVER_TIME => Some(SystemTime::now()),
        set_mtime::SET_TO_CLIENT_TIME(t) => Some(nfstime3_to_system_time(t)),
    }
}

/// Extract mode from sattr3 for file creation (defaults to 0o644).
pub fn sattr3_mode_or_default(sattr: &sattr3, default: u32) -> u32 {
    match &sattr.mode {
        Nfs3Option::Some(m) => *m,
        Nfs3Option::None => default,
    }
}

/// Extract uid from sattr3, falling back to current process uid.
pub fn sattr3_uid(sattr: &sattr3) -> u32 {
    match &sattr.uid {
        Nfs3Option::Some(u) => *u,
        Nfs3Option::None => nix::unistd::getuid().as_raw(),
    }
}

/// Extract gid from sattr3, falling back to current process gid.
pub fn sattr3_gid(sattr: &sattr3) -> u32 {
    match &sattr.gid {
        Nfs3Option::Some(g) => *g,
        Nfs3Option::None => nix::unistd::getgid().as_raw(),
    }
}
