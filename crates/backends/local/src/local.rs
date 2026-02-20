use dashmap::DashMap;
use parking_lot::Mutex;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;
use zodaix_core::*;

/// Maximum number of cached inode-to-path mappings.
/// When exceeded, the cache is pruned to prevent unbounded memory growth
/// in large filesystem trees.
const MAX_CACHE_SIZE: usize = 100_000;

/// Local filesystem passthrough backend.
///
/// Maps real filesystem inodes to VFS operations.
/// Uses DashMap-based bidirectional inode-to-path cache.
/// File handles are cached to avoid re-opening files on every read/write.
pub struct LocalBackend {
    /// Root directory being served.
    root: PathBuf,
    /// Inode → path mapping.
    ino_to_path: DashMap<InodeId, PathBuf>,
    /// Path → inode mapping.
    path_to_ino: DashMap<PathBuf, InodeId>,
    /// Next file handle.
    next_fh: AtomicU64,
    /// File handle → open File descriptor cache.
    open_files: DashMap<FileHandle, Mutex<fs::File>>,
}

impl LocalBackend {
    pub fn new(root: PathBuf) -> VfsResult<Self> {
        let root = root.canonicalize().map_err(VfsError::Io)?;

        let backend = Self {
            root: root.clone(),
            ino_to_path: DashMap::new(),
            path_to_ino: DashMap::new(),
            next_fh: AtomicU64::new(1),
            open_files: DashMap::new(),
        };

        // Register root directory with inode 1.
        backend.ino_to_path.insert(ROOT_INO, root.clone());
        backend.path_to_ino.insert(root, ROOT_INO);

        Ok(backend)
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the real path for an inode, or NotFound.
    fn get_path(&self, ino: InodeId) -> VfsResult<PathBuf> {
        self.ino_to_path
            .get(&ino)
            .map(|p| p.value().clone())
            .ok_or(VfsError::NotFound)
    }

    /// Register a path and return its inode.
    fn register_path(&self, path: PathBuf) -> VfsResult<InodeId> {
        if let Some(ino) = self.path_to_ino.get(&path) {
            return Ok(*ino.value());
        }

        let meta = fs::symlink_metadata(&path).map_err(VfsError::Io)?;
        let ino = meta.ino();

        // Use the real inode number, but remap root to ROOT_INO.
        let vfs_ino = if path == self.root { ROOT_INO } else { ino };

        // Evict old entries if cache is too large to prevent unbounded growth.
        if self.ino_to_path.len() > MAX_CACHE_SIZE {
            self.evict_cache();
        }

        // Only set ino_to_path if the inode is not yet registered (preserve
        // existing mapping for hardlinks to avoid overwriting the primary path).
        self.ino_to_path.entry(vfs_ino).or_insert(path.clone());
        self.path_to_ino.insert(path, vfs_ino);

        Ok(vfs_ino)
    }

    /// Evict roughly half the cache entries (excluding root).
    fn evict_cache(&self) {
        let to_evict = MAX_CACHE_SIZE / 2;
        // Collect keys to remove first (can't remove while iterating DashMap).
        let keys: Vec<InodeId> = self
            .ino_to_path
            .iter()
            .filter(|entry| *entry.key() != ROOT_INO)
            .take(to_evict)
            .map(|entry| *entry.key())
            .collect();
        for ino in keys {
            if let Some((_, path)) = self.ino_to_path.remove(&ino) {
                self.path_to_ino.remove(&path);
            }
        }
    }

    /// Validate that a child path is contained within the root directory.
    fn validate_containment(&self, path: &Path) -> VfsResult<()> {
        // Canonicalize to resolve any symlinks or ".." components.
        // If the file doesn't exist yet, check the parent.
        let check_path = if path.exists() {
            path.canonicalize().map_err(VfsError::Io)?
        } else if let Some(parent) = path.parent() {
            if parent.exists() {
                let canonical_parent = parent.canonicalize().map_err(VfsError::Io)?;
                canonical_parent.join(path.file_name().ok_or(VfsError::InvalidArgument)?)
            } else {
                return Err(VfsError::NotFound);
            }
        } else {
            return Err(VfsError::NotFound);
        };

        if !check_path.starts_with(&self.root) {
            return Err(VfsError::PermissionDenied);
        }
        Ok(())
    }

    /// Convert fs::Metadata to VfsAttr.
    fn meta_to_attr(&self, meta: &fs::Metadata, ino: InodeId) -> VfsAttr {
        let kind = if meta.is_dir() {
            FileType::Directory
        } else if meta.file_type().is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        };

        VfsAttr {
            ino,
            size: meta.len(),
            blocks: meta.blocks(),
            atime: if meta.atime() >= 0 { UNIX_EPOCH + std::time::Duration::new(meta.atime() as u64, meta.atime_nsec() as u32) } else { UNIX_EPOCH },
            mtime: if meta.mtime() >= 0 { UNIX_EPOCH + std::time::Duration::new(meta.mtime() as u64, meta.mtime_nsec() as u32) } else { UNIX_EPOCH },
            ctime: if meta.ctime() >= 0 { UNIX_EPOCH + std::time::Duration::new(meta.ctime() as u64, meta.ctime_nsec() as u32) } else { UNIX_EPOCH },
            crtime: meta.created().unwrap_or(UNIX_EPOCH),
            kind,
            perm: (meta.mode() & 0o7777) as u16,
            nlink: meta.nlink() as u32,
            uid: meta.uid(),
            gid: meta.gid(),
            rdev: meta.rdev() as u32,
            blksize: meta.blksize() as u32,
        }
    }

    fn name_to_str(name: &OsStr) -> VfsResult<&str> {
        name.to_str().ok_or(VfsError::InvalidArgument)
    }
}

impl VfsBackend for LocalBackend {
    fn name(&self) -> &str {
        "local"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::HARDLINKS
            | BackendCapabilities::SYMLINKS
            | BackendCapabilities::XATTRS
            | BackendCapabilities::PERSISTENT
    }

    fn lookup(&self, parent: InodeId, name: &OsStr) -> VfsResult<VfsAttr> {
        let parent_path = self.get_path(parent)?;
        let child_path = parent_path.join(name);
        self.validate_containment(&child_path)?;
        let meta = fs::symlink_metadata(&child_path).map_err(VfsError::Io)?;
        let ino = self.register_path(child_path)?;
        Ok(self.meta_to_attr(&meta, ino))
    }

    fn getattr(&self, ino: InodeId) -> VfsResult<VfsAttr> {
        let path = self.get_path(ino)?;
        let meta = fs::symlink_metadata(&path).map_err(VfsError::Io)?;
        Ok(self.meta_to_attr(&meta, ino))
    }

    fn setattr(&self, ino: InodeId, params: SetAttrParams) -> VfsResult<VfsAttr> {
        let path = self.get_path(ino)?;

        if let Some(mode) = params.mode {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).map_err(VfsError::Io)?;
        }

        if params.uid.is_some() || params.gid.is_some() {
            let meta = fs::symlink_metadata(&path).map_err(VfsError::Io)?;
            let uid = params.uid.unwrap_or(meta.uid());
            let gid = params.gid.unwrap_or(meta.gid());
            nix::unistd::chown(
                &path,
                Some(nix::unistd::Uid::from_raw(uid)),
                Some(nix::unistd::Gid::from_raw(gid)),
            )
            .map_err(|e| VfsError::Io(e.into()))?;
        }

        if let Some(size) = params.size {
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(VfsError::Io)?;
            f.set_len(size).map_err(VfsError::Io)?;
        }

        if params.atime.is_some() || params.mtime.is_some() {
            let meta = fs::symlink_metadata(&path).map_err(VfsError::Io)?;
            let atime = params.atime.unwrap_or_else(|| {
                if meta.atime() >= 0 {
                    UNIX_EPOCH + std::time::Duration::new(meta.atime() as u64, meta.atime_nsec() as u32)
                } else {
                    UNIX_EPOCH
                }
            });
            let mtime = params.mtime.unwrap_or_else(|| {
                if meta.mtime() >= 0 {
                    UNIX_EPOCH + std::time::Duration::new(meta.mtime() as u64, meta.mtime_nsec() as u32)
                } else {
                    UNIX_EPOCH
                }
            });
            filetime::set_file_times(
                &path,
                filetime::FileTime::from_system_time(atime),
                filetime::FileTime::from_system_time(mtime),
            )
            .map_err(VfsError::Io)?;
        }

        self.getattr(ino)
    }

    fn open(&self, ino: InodeId, flags: i32) -> VfsResult<FileHandle> {
        let path = self.get_path(ino)?;
        let fh = self.alloc_fh();

        // Open the actual file descriptor and cache it.
        let file = if flags & libc::O_WRONLY != 0 || flags & libc::O_RDWR != 0 {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(VfsError::Io)?
        } else {
            fs::File::open(&path).map_err(VfsError::Io)?
        };

        self.open_files.insert(fh, Mutex::new(file));
        Ok(fh)
    }

    fn read(&self, ino: InodeId, fh: FileHandle, offset: i64, size: u32) -> VfsResult<Vec<u8>> {
        if offset < 0 {
            return Err(VfsError::InvalidArgument);
        }
        // Cap buffer allocation to prevent OOM from untrusted size values.
        let size = size.min(4 * 1024 * 1024); // 4 MB max
        use std::io::{Read, Seek, SeekFrom};

        // Try to use cached file handle first.
        if let Some(entry) = self.open_files.get(&fh) {
            let mut f = entry.value().lock();
            f.seek(SeekFrom::Start(offset as u64)).map_err(VfsError::Io)?;
            let mut buf = vec![0u8; size as usize];
            let n = f.read(&mut buf).map_err(VfsError::Io)?;
            buf.truncate(n);
            return Ok(buf);
        }

        // Fallback: open file from path (e.g., NFS stateless access).
        let path = self.get_path(ino)?;
        let mut f = fs::File::open(&path).map_err(VfsError::Io)?;
        f.seek(SeekFrom::Start(offset as u64))
            .map_err(VfsError::Io)?;
        let mut buf = vec![0u8; size as usize];
        let n = f.read(&mut buf).map_err(VfsError::Io)?;
        buf.truncate(n);
        Ok(buf)
    }

    fn write(
        &self,
        ino: InodeId,
        fh: FileHandle,
        offset: i64,
        data: &[u8],
        _flags: i32,
    ) -> VfsResult<u32> {
        if offset < 0 {
            return Err(VfsError::InvalidArgument);
        }
        use std::io::{Seek, SeekFrom, Write};

        // Try to use cached file handle first.
        if let Some(entry) = self.open_files.get(&fh) {
            let mut f = entry.value().lock();
            f.seek(SeekFrom::Start(offset as u64)).map_err(VfsError::Io)?;
            let n = f.write(data).map_err(VfsError::Io)?;
            return Ok(n as u32);
        }

        // Fallback: open file from path.
        let path = self.get_path(ino)?;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(VfsError::Io)?;
        f.seek(SeekFrom::Start(offset as u64))
            .map_err(VfsError::Io)?;
        let n = f.write(data).map_err(VfsError::Io)?;
        Ok(n as u32)
    }

    fn flush(&self, _ino: InodeId, fh: FileHandle) -> VfsResult<()> {
        if let Some(entry) = self.open_files.get(&fh) {
            use std::io::Write;
            let mut f = entry.value().lock();
            f.flush().map_err(VfsError::Io)?;
        }
        Ok(())
    }

    fn release(&self, _ino: InodeId, fh: FileHandle, _flags: i32, _flush: bool) -> VfsResult<()> {
        // Remove and drop the cached file descriptor.
        self.open_files.remove(&fh);
        Ok(())
    }

    fn fsync(&self, _ino: InodeId, fh: FileHandle, datasync: bool) -> VfsResult<()> {
        if let Some(entry) = self.open_files.get(&fh) {
            let f = entry.value().lock();
            if datasync {
                f.sync_data().map_err(VfsError::Io)?;
            } else {
                f.sync_all().map_err(VfsError::Io)?;
            }
        }
        Ok(())
    }

    fn mknod(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _uid: u32,
        _gid: u32,
        _rdev: u32,
    ) -> VfsResult<VfsAttr> {
        let parent_path = self.get_path(parent)?;
        let child_path = parent_path.join(name);
        self.validate_containment(&child_path)?;
        let perm = mode & !umask & 0o7777;

        fs::File::create(&child_path).map_err(VfsError::Io)?;
        fs::set_permissions(&child_path, fs::Permissions::from_mode(perm)).map_err(VfsError::Io)?;

        let ino = self.register_path(child_path.clone())?;
        let meta = fs::symlink_metadata(&child_path).map_err(VfsError::Io)?;
        Ok(self.meta_to_attr(&meta, ino))
    }

    fn mkdir(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _uid: u32,
        _gid: u32,
    ) -> VfsResult<VfsAttr> {
        let parent_path = self.get_path(parent)?;
        let child_path = parent_path.join(name);
        self.validate_containment(&child_path)?;
        let perm = mode & !umask & 0o7777;

        fs::create_dir(&child_path).map_err(VfsError::Io)?;
        fs::set_permissions(&child_path, fs::Permissions::from_mode(perm)).map_err(VfsError::Io)?;

        let ino = self.register_path(child_path.clone())?;
        let meta = fs::symlink_metadata(&child_path).map_err(VfsError::Io)?;
        Ok(self.meta_to_attr(&meta, ino))
    }

    fn unlink(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let parent_path = self.get_path(parent)?;
        let child_path = parent_path.join(name);

        fs::remove_file(&child_path).map_err(VfsError::Io)?;

        // Remove from caches. Only remove ino_to_path if no other path
        // references the same inode (hardlink case).
        if let Some((_, ino)) = self.path_to_ino.remove(&child_path) {
            let has_other_link = self.path_to_ino.iter().any(|entry| *entry.value() == ino);
            if !has_other_link {
                self.ino_to_path.remove(&ino);
            }
        }
        Ok(())
    }

    fn rmdir(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let parent_path = self.get_path(parent)?;
        let child_path = parent_path.join(name);

        fs::remove_dir(&child_path).map_err(VfsError::Io)?;

        if let Some((_, ino)) = self.path_to_ino.remove(&child_path) {
            self.ino_to_path.remove(&ino);
        }
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
        let old_path = self.get_path(parent)?.join(name);
        let new_path = self.get_path(newparent)?.join(newname);
        self.validate_containment(&new_path)?;

        // Evict stale cache entry for the replaced target (if any).
        if let Some((_, old_ino)) = self.path_to_ino.remove(&new_path) {
            self.ino_to_path.remove(&old_ino);
        }

        fs::rename(&old_path, &new_path).map_err(VfsError::Io)?;

        // Update caches: remove old path entry and register new one.
        if let Some((_, ino)) = self.path_to_ino.remove(&old_path) {
            self.ino_to_path.insert(ino, new_path.clone());
            self.path_to_ino.insert(new_path.clone(), ino);
        }

        // Invalidate cached children whose paths are under the old or new directory.
        // Children under old_path moved; children under new_path may have been replaced.
        // They will be re-registered on next access with correct paths.
        let mut prefixes_to_invalidate = Vec::new();
        for base in [&old_path, &new_path] {
            let mut p = base.as_os_str().to_os_string();
            p.push("/");
            prefixes_to_invalidate.push(PathBuf::from(p));
        }
        let stale_children: Vec<(PathBuf, InodeId)> = self
            .path_to_ino
            .iter()
            .filter(|entry| {
                prefixes_to_invalidate
                    .iter()
                    .any(|prefix| entry.key().starts_with(prefix))
            })
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        for (path, ino) in stale_children {
            self.path_to_ino.remove(&path);
            self.ino_to_path.remove(&ino);
        }

        Ok(())
    }

    fn opendir(&self, ino: InodeId, _flags: i32) -> VfsResult<FileHandle> {
        let path = self.get_path(ino)?;
        let meta = fs::symlink_metadata(&path).map_err(VfsError::Io)?;
        if !meta.is_dir() {
            return Err(VfsError::NotADirectory);
        }
        Ok(self.alloc_fh())
    }

    fn readdir(&self, ino: InodeId, _fh: FileHandle, offset: i64) -> VfsResult<Vec<DirEntry>> {
        let path = self.get_path(ino)?;
        let parent_ino = if path == self.root {
            ROOT_INO
        } else if let Some(parent) = path.parent() {
            self.register_path(parent.to_path_buf()).unwrap_or(ROOT_INO)
        } else {
            ROOT_INO
        };

        let mut entries = vec![
            DirEntry {
                ino,
                name: ".".to_string(),
                kind: FileType::Directory,
            },
            DirEntry {
                ino: parent_ino,
                name: "..".to_string(),
                kind: FileType::Directory,
            },
        ];

        let dir = fs::read_dir(&path).map_err(VfsError::Io)?;
        for entry in dir {
            let entry = entry.map_err(VfsError::Io)?;
            let child_path = entry.path();
            let child_ino = self.register_path(child_path)?;
            let ft = entry.file_type().map_err(VfsError::Io)?;
            let kind = if ft.is_dir() {
                FileType::Directory
            } else if ft.is_symlink() {
                FileType::Symlink
            } else {
                FileType::RegularFile
            };
            entries.push(DirEntry {
                ino: child_ino,
                name: entry.file_name().to_string_lossy().to_string(),
                kind,
            });
        }

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

    fn symlink(
        &self,
        parent: InodeId,
        link_name: &OsStr,
        target: &Path,
        _uid: u32,
        _gid: u32,
    ) -> VfsResult<VfsAttr> {
        let parent_path = self.get_path(parent)?;
        let link_path = parent_path.join(link_name);
        self.validate_containment(&link_path)?;

        std::os::unix::fs::symlink(target, &link_path).map_err(VfsError::Io)?;

        let ino = self.register_path(link_path.clone())?;
        let meta = fs::symlink_metadata(&link_path).map_err(VfsError::Io)?;
        Ok(self.meta_to_attr(&meta, ino))
    }

    fn readlink(&self, ino: InodeId) -> VfsResult<Vec<u8>> {
        let path = self.get_path(ino)?;
        let target = fs::read_link(&path).map_err(VfsError::Io)?;
        Ok(target.as_os_str().as_encoded_bytes().to_vec())
    }

    fn link(&self, ino: InodeId, newparent: InodeId, newname: &OsStr) -> VfsResult<VfsAttr> {
        let src = self.get_path(ino)?;
        let dst = self.get_path(newparent)?.join(newname);
        self.validate_containment(&dst)?;

        fs::hard_link(&src, &dst).map_err(VfsError::Io)?;

        let new_ino = self.register_path(dst.clone())?;
        let meta = fs::symlink_metadata(&dst).map_err(VfsError::Io)?;
        Ok(self.meta_to_attr(&meta, new_ino))
    }

    fn getxattr(&self, ino: InodeId, name: &OsStr, size: u32) -> VfsResult<Vec<u8>> {
        let path = self.get_path(ino)?;
        let name_str = Self::name_to_str(name)?;

        match xattr::get(&path, name_str).map_err(VfsError::Io)? {
            Some(value) => {
                if size == 0 {
                    let len = value.len() as u32;
                    Ok(len.to_ne_bytes().to_vec())
                } else if (size as usize) < value.len() {
                    Err(VfsError::XattrRange)
                } else {
                    Ok(value)
                }
            }
            None => Err(VfsError::NoXattr),
        }
    }

    fn setxattr(&self, ino: InodeId, name: &OsStr, value: &[u8], _flags: i32) -> VfsResult<()> {
        let path = self.get_path(ino)?;
        let name_str = Self::name_to_str(name)?;
        xattr::set(&path, name_str, value).map_err(VfsError::Io)
    }

    fn listxattr(&self, ino: InodeId, size: u32) -> VfsResult<Vec<u8>> {
        let path = self.get_path(ino)?;
        let attrs = xattr::list(&path).map_err(VfsError::Io)?;

        let mut buf = Vec::new();
        for name in attrs {
            buf.extend_from_slice(name.as_encoded_bytes());
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
        let path = self.get_path(ino)?;
        let name_str = Self::name_to_str(name)?;
        xattr::remove(&path, name_str).map_err(VfsError::Io)
    }

    fn statfs(&self, _ino: InodeId) -> VfsResult<StatFs> {
        Ok(StatFs::default())
    }

    fn access(&self, ino: InodeId, mask: i32) -> VfsResult<()> {
        let path = self.get_path(ino)?;
        // Use the real access() syscall for actual permission checking.
        nix::unistd::access(
            &path,
            nix::unistd::AccessFlags::from_bits_truncate(mask),
        )
        .map_err(|e| VfsError::Io(e.into()))
    }
}
