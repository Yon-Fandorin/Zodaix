use thiserror::Error;

/// VFS error type with direct errno mapping.
#[derive(Debug, Error)]
pub enum VfsError {
    #[error("No such file or directory")]
    NotFound,

    #[error("File exists")]
    AlreadyExists,

    #[error("Not a directory")]
    NotADirectory,

    #[error("Is a directory")]
    IsADirectory,

    #[error("Directory not empty")]
    NotEmpty,

    #[error("Permission denied")]
    PermissionDenied,

    #[error("Invalid argument")]
    InvalidArgument,

    #[error("No space left on device")]
    NoSpace,

    #[error("File name too long")]
    NameTooLong,

    #[error("Too many open files")]
    TooManyOpenFiles,

    #[error("Bad file descriptor")]
    BadFileDescriptor,

    #[error("Cross-device link")]
    CrossDeviceLink,

    #[error("No extended attribute")]
    NoXattr,

    #[error("Value too large for attribute")]
    XattrRange,

    #[error("Not supported")]
    NotSupported,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl VfsError {
    /// Convert to libc errno value.
    pub fn to_errno(&self) -> i32 {
        match self {
            VfsError::NotFound => libc::ENOENT,
            VfsError::AlreadyExists => libc::EEXIST,
            VfsError::NotADirectory => libc::ENOTDIR,
            VfsError::IsADirectory => libc::EISDIR,
            VfsError::NotEmpty => libc::ENOTEMPTY,
            VfsError::PermissionDenied => libc::EACCES,
            VfsError::InvalidArgument => libc::EINVAL,
            VfsError::NoSpace => libc::ENOSPC,
            VfsError::NameTooLong => libc::ENAMETOOLONG,
            VfsError::TooManyOpenFiles => libc::EMFILE,
            VfsError::BadFileDescriptor => libc::EBADF,
            VfsError::CrossDeviceLink => libc::EXDEV,
            VfsError::NoXattr => Self::noattr_errno(),
            VfsError::XattrRange => libc::ERANGE,
            VfsError::NotSupported => libc::ENOTSUP,
            VfsError::Io(e) => e.raw_os_error().unwrap_or(libc::EIO),
            VfsError::Other(_) => libc::EIO,
        }
    }

    /// Platform-specific errno for "no such xattr".
    /// macOS uses ENOATTR (93), Linux uses ENODATA (61).
    fn noattr_errno() -> i32 {
        #[cfg(target_os = "macos")]
        {
            93 // ENOATTR
        }
        #[cfg(target_os = "linux")]
        {
            libc::ENODATA
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            libc::ENODATA
        }
    }
}

pub type VfsResult<T> = Result<T, VfsError>;
