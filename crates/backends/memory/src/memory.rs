use crate::inode_table::{InodeData, InodeTable};
use std::ffi::OsStr;
use std::path::Path;
use std::time::SystemTime;
use zodaix_core::*;

/// In-memory VFS backend.
///
/// All data lives in a `DashMap<InodeId, InodeData>`.
/// Thread-safe for concurrent FUSE dispatch.
#[derive(Debug)]
pub struct MemoryBackend {
    inodes: InodeTable,
}

impl MemoryBackend {
    pub fn new() -> Self {
        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();
        Self {
            inodes: InodeTable::new(uid, gid),
        }
    }

    /// Helper: convert OsStr to String, returning InvalidArgument on failure.
    fn name_to_string(name: &OsStr) -> VfsResult<String> {
        name.to_str()
            .map(|s| s.to_string())
            .ok_or(VfsError::InvalidArgument)
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl VfsBackend for MemoryBackend {
    fn name(&self) -> &str {
        "memory"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::HARDLINKS
            | BackendCapabilities::SYMLINKS
            | BackendCapabilities::XATTRS
    }

    fn lookup(&self, parent: InodeId, name: &OsStr) -> VfsResult<VfsAttr> {
        let name_str = Self::name_to_string(name)?;

        let child_ino = self
            .inodes
            .with_inode(parent, |data| {
                if data.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                data.children
                    .get(&name_str)
                    .copied()
                    .ok_or(VfsError::NotFound)
            })
            .ok_or(VfsError::NotFound)??;

        self.getattr(child_ino)
    }

    fn getattr(&self, ino: InodeId) -> VfsResult<VfsAttr> {
        self.inodes
            .with_inode(ino, |data| data.attr.clone())
            .ok_or(VfsError::NotFound)
    }

    fn setattr(&self, ino: InodeId, params: SetAttrParams) -> VfsResult<VfsAttr> {
        self.inodes
            .with_inode_mut(ino, |data| {
                if let Some(mode) = params.mode {
                    data.attr.perm = (mode & 0o7777) as u16;
                }
                if let Some(uid) = params.uid {
                    data.attr.uid = uid;
                }
                if let Some(gid) = params.gid {
                    data.attr.gid = gid;
                }
                if let Some(size) = params.size {
                    data.content.resize(size as usize, 0);
                    data.attr.size = size;
                    data.attr.blocks = size.div_ceil(512);
                }
                if let Some(atime) = params.atime {
                    data.attr.atime = atime;
                }
                if let Some(mtime) = params.mtime {
                    data.attr.mtime = mtime;
                }
                data.attr.ctime = SystemTime::now();
                data.attr.clone()
            })
            .ok_or(VfsError::NotFound)
    }

    fn open(&self, ino: InodeId, _flags: i32) -> VfsResult<FileHandle> {
        if !self.inodes.contains(ino) {
            return Err(VfsError::NotFound);
        }
        Ok(self.inodes.alloc_fh())
    }

    fn read(&self, ino: InodeId, _fh: FileHandle, offset: i64, size: u32) -> VfsResult<Vec<u8>> {
        if offset < 0 {
            return Err(VfsError::InvalidArgument);
        }
        self.inodes
            .with_inode(ino, |data| {
                let offset = offset as usize;
                if offset >= data.content.len() {
                    return Vec::new();
                }
                let end = (offset + size as usize).min(data.content.len());
                data.content[offset..end].to_vec()
            })
            .ok_or(VfsError::NotFound)
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
        self.inodes
            .with_inode_mut(ino, |inode| {
                let offset = offset as usize;
                let end = offset + data.len();

                // Extend content if writing past current end (gap fill with zeros).
                if end > inode.content.len() {
                    inode.content.resize(end, 0);
                }
                inode.content[offset..end].copy_from_slice(data);
                inode.attr.size = inode.content.len() as u64;
                inode.attr.blocks = inode.attr.size.div_ceil(512);
                inode.attr.mtime = SystemTime::now();
                inode.attr.ctime = SystemTime::now();
                data.len() as u32
            })
            .ok_or(VfsError::NotFound)
    }

    fn flush(&self, _ino: InodeId, _fh: FileHandle) -> VfsResult<()> {
        Ok(())
    }

    fn release(&self, _ino: InodeId, _fh: FileHandle, _flags: i32, _flush: bool) -> VfsResult<()> {
        Ok(())
    }

    fn fsync(&self, _ino: InodeId, _fh: FileHandle, _datasync: bool) -> VfsResult<()> {
        Ok(())
    }

    fn mknod(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
        _rdev: u32,
    ) -> VfsResult<VfsAttr> {
        let name_str = Self::name_to_string(name)?;
        let ino = self.inodes.alloc_ino();
        let perm = (mode & !umask & 0o7777) as u16;
        let attr = VfsAttr::new_file(ino, perm, uid, gid);
        let result = attr.clone();

        // Insert child inode first so concurrent lookups won't see a dangling reference.
        self.inodes.insert(ino, InodeData::new_file(attr, parent));

        // Check parent is directory and name doesn't exist.
        let insert_result = self
            .inodes
            .with_inode_mut(parent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                if pdata.children.contains_key(&name_str) {
                    return Err(VfsError::AlreadyExists);
                }
                pdata.children.insert(name_str, ino);
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(())
            })
            .ok_or(VfsError::NotFound)?;

        if let Err(e) = insert_result {
            // Rollback: remove the orphan inode.
            self.inodes.remove(ino);
            return Err(e);
        }

        Ok(result)
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
        let name_str = Self::name_to_string(name)?;
        let ino = self.inodes.alloc_ino();
        let perm = (mode & !umask & 0o7777) as u16;
        let attr = VfsAttr::new_dir(ino, perm, uid, gid);
        let result = attr.clone();

        // Insert child inode first so concurrent lookups won't see a dangling reference.
        self.inodes.insert(ino, InodeData::new_dir(attr, parent));

        let insert_result = self
            .inodes
            .with_inode_mut(parent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                if pdata.children.contains_key(&name_str) {
                    return Err(VfsError::AlreadyExists);
                }
                pdata.children.insert(name_str, ino);
                pdata.attr.nlink += 1; // subdirectory adds link to parent
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(())
            })
            .ok_or(VfsError::NotFound)?;

        if let Err(e) = insert_result {
            self.inodes.remove(ino);
            return Err(e);
        }

        Ok(result)
    }

    fn unlink(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = Self::name_to_string(name)?;

        // Check type and remove from parent atomically to avoid orphaning
        // directory inodes if the type check fails after removal.
        let child_ino = self
            .inodes
            .with_inode_mut(parent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                let &ino = pdata.children.get(&name_str).ok_or(VfsError::NotFound)?;
                // Check type BEFORE removing from parent.
                let is_dir = self
                    .inodes
                    .with_inode(ino, |data| data.attr.kind == FileType::Directory)
                    .unwrap_or(false);
                if is_dir {
                    return Err(VfsError::IsADirectory);
                }
                pdata.children.remove(&name_str);
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(ino)
            })
            .ok_or(VfsError::NotFound)??;

        // Decrement nlink; remove inode if zero.
        let should_remove = self
            .inodes
            .with_inode_mut(child_ino, |data| {
                data.attr.nlink = data.attr.nlink.saturating_sub(1);
                data.attr.nlink == 0
            })
            .unwrap_or(false);

        if should_remove {
            self.inodes.remove(child_ino);
        }

        Ok(())
    }

    fn rmdir(&self, parent: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = Self::name_to_string(name)?;

        // Check empty and remove from parent atomically to prevent TOCTOU race
        // where a concurrent thread could add children between the check and removal.
        let child_ino = self
            .inodes
            .with_inode_mut(parent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                let &ino = pdata.children.get(&name_str).ok_or(VfsError::NotFound)?;

                // Verify child is an empty directory while still holding parent lock.
                let check = self
                    .inodes
                    .with_inode(ino, |data| {
                        if data.attr.kind != FileType::Directory {
                            return Err(VfsError::NotADirectory);
                        }
                        if !data.children.is_empty() {
                            return Err(VfsError::NotEmpty);
                        }
                        Ok(())
                    })
                    .ok_or(VfsError::NotFound)?;
                check?;

                pdata.children.remove(&name_str);
                pdata.attr.nlink = pdata.attr.nlink.saturating_sub(1);
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(ino)
            })
            .ok_or(VfsError::NotFound)??;

        self.inodes.remove(child_ino);
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
        let old_name = Self::name_to_string(name)?;
        let new_name = Self::name_to_string(newname)?;

        // Helper: check POSIX type compatibility for rename replacement.
        let validate_replacement = |src_ino: InodeId, replaced_ino: InodeId, inodes: &crate::inode_table::InodeTable| -> VfsResult<()> {
            let src_is_dir = inodes
                .with_inode(src_ino, |d| d.attr.kind == FileType::Directory)
                .unwrap_or(false);
            let replaced_is_dir = inodes
                .with_inode(replaced_ino, |d| d.attr.kind == FileType::Directory)
                .unwrap_or(false);

            if src_is_dir && !replaced_is_dir {
                return Err(VfsError::NotADirectory);
            }
            if !src_is_dir && replaced_is_dir {
                return Err(VfsError::IsADirectory);
            }
            if replaced_is_dir {
                let is_empty = inodes
                    .with_inode(replaced_ino, |d| d.children.is_empty())
                    .unwrap_or(true);
                if !is_empty {
                    return Err(VfsError::NotEmpty);
                }
            }
            Ok(())
        };

        // Atomic rename: acquire both parent locks simultaneously to prevent
        // intermediate states visible to other threads.
        let (ino, replaced) = if parent == newparent {
            // Same directory: single lock acquisition.
            self.inodes
                .with_inode_mut(parent, |pdata| {
                    if pdata.attr.kind != FileType::Directory {
                        return Err(VfsError::NotADirectory);
                    }
                    let &src_ino = pdata.children.get(&old_name).ok_or(VfsError::NotFound)?;

                    // Validate type compatibility if target exists.
                    if let Some(&replaced_ino) = pdata.children.get(&new_name) {
                        validate_replacement(src_ino, replaced_ino, &self.inodes)?;
                    }

                    let ino = pdata.children.remove(&old_name).unwrap();
                    let replaced = pdata.children.insert(new_name.clone(), ino);
                    pdata.attr.mtime = SystemTime::now();
                    pdata.attr.ctime = SystemTime::now();
                    Ok((ino, replaced))
                })
                .ok_or(VfsError::NotFound)??
        } else {
            // Different directories: remove-modify-reinsert to avoid DashMap
            // same-shard deadlock (two get_mut() on same shard would deadlock).
            let mut parent_data = self.inodes.remove(parent).ok_or(VfsError::NotFound)?;
            let mut newparent_data = self.inodes.remove(newparent).ok_or(VfsError::NotFound)?;

            if parent_data.attr.kind != FileType::Directory
                || newparent_data.attr.kind != FileType::Directory
            {
                self.inodes.insert(parent, parent_data);
                self.inodes.insert(newparent, newparent_data);
                return Err(VfsError::NotADirectory);
            }

            let src_ino = match parent_data.children.get(&old_name) {
                Some(&ino) => ino,
                None => {
                    self.inodes.insert(parent, parent_data);
                    self.inodes.insert(newparent, newparent_data);
                    return Err(VfsError::NotFound);
                }
            };

            // Validate type compatibility if target exists.
            if let Some(&replaced_ino) = newparent_data.children.get(&new_name) {
                if let Err(e) = validate_replacement(src_ino, replaced_ino, &self.inodes) {
                    self.inodes.insert(parent, parent_data);
                    self.inodes.insert(newparent, newparent_data);
                    return Err(e);
                }
            }

            let ino = parent_data.children.remove(&old_name).unwrap();
            let replaced = newparent_data.children.insert(new_name.clone(), ino);
            let now = SystemTime::now();
            parent_data.attr.mtime = now;
            parent_data.attr.ctime = now;
            newparent_data.attr.mtime = now;
            newparent_data.attr.ctime = now;

            self.inodes.insert(parent, parent_data);
            self.inodes.insert(newparent, newparent_data);

            (ino, replaced)
        };

        if let Some(replaced_ino) = replaced {
            // Decrement nlink or remove replaced inode.
            let replaced_is_dir = self
                .inodes
                .with_inode(replaced_ino, |data| data.attr.kind == FileType::Directory)
                .unwrap_or(false);
            let should_remove = self
                .inodes
                .with_inode_mut(replaced_ino, |data| {
                    data.attr.nlink = data.attr.nlink.saturating_sub(1);
                    data.attr.nlink == 0
                })
                .unwrap_or(false);
            if should_remove {
                self.inodes.remove(replaced_ino);
            }
            // If replaced target was a directory, decrement newparent's nlink
            // (the replaced dir's ".." link is gone — applies even in same-parent case).
            if replaced_is_dir {
                self.inodes.with_inode_mut(newparent, |data| {
                    data.attr.nlink = data.attr.nlink.saturating_sub(1);
                });
            }
        }

        // Check if the moved inode is a directory — update parent nlinks.
        let moved_is_dir = self
            .inodes
            .with_inode(ino, |data| data.attr.kind == FileType::Directory)
            .unwrap_or(false);

        if moved_is_dir && parent != newparent {
            // Old parent loses ".." link from moved directory.
            self.inodes.with_inode_mut(parent, |data| {
                data.attr.nlink = data.attr.nlink.saturating_sub(1);
            });
            // New parent gains ".." link from moved directory.
            self.inodes.with_inode_mut(newparent, |data| {
                data.attr.nlink += 1;
            });
        }

        // Update parent reference in moved inode.
        self.inodes.with_inode_mut(ino, |data| {
            data.parent = newparent;
            data.attr.ctime = SystemTime::now();
        });

        Ok(())
    }

    fn opendir(&self, ino: InodeId, _flags: i32) -> VfsResult<FileHandle> {
        let is_dir = self
            .inodes
            .with_inode(ino, |data| data.attr.kind == FileType::Directory)
            .ok_or(VfsError::NotFound)?;
        if !is_dir {
            return Err(VfsError::NotADirectory);
        }
        Ok(self.inodes.alloc_fh())
    }

    fn readdir(&self, ino: InodeId, _fh: FileHandle, offset: i64) -> VfsResult<Vec<DirEntry>> {
        // Collect children info while holding the parent lock, then release it
        // before looking up child types to avoid nested DashMap shard access.
        let (parent_ino, children) = self
            .inodes
            .with_inode(ino, |data| {
                if data.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                let children: Vec<(String, InodeId)> = data
                    .children
                    .iter()
                    .map(|(name, &child_ino)| (name.clone(), child_ino))
                    .collect();
                Ok((data.parent, children))
            })
            .ok_or(VfsError::NotFound)??;

        let mut entries = Vec::with_capacity(children.len() + 2);

        // `.` and `..` first.
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

        // Look up child types outside the parent lock.
        for (name, child_ino) in children {
            let kind = self
                .inodes
                .with_inode(child_ino, |c| c.attr.kind)
                .unwrap_or(FileType::RegularFile);
            entries.push(DirEntry {
                ino: child_ino,
                name,
                kind,
            });
        }

        // Apply offset: skip first `offset` entries.
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
        uid: u32,
        gid: u32,
    ) -> VfsResult<VfsAttr> {
        let name_str = Self::name_to_string(link_name)?;
        let ino = self.inodes.alloc_ino();
        let attr = VfsAttr::new_symlink(ino, uid, gid);
        let target_bytes = target.as_os_str().as_encoded_bytes().to_vec();
        let result_attr = VfsAttr {
            size: target_bytes.len() as u64,
            ..attr.clone()
        };

        // Insert child inode first so concurrent lookups won't see a dangling reference.
        self.inodes
            .insert(ino, InodeData::new_symlink(attr, target_bytes, parent));

        let insert_result = self
            .inodes
            .with_inode_mut(parent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                if pdata.children.contains_key(&name_str) {
                    return Err(VfsError::AlreadyExists);
                }
                pdata.children.insert(name_str, ino);
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(())
            })
            .ok_or(VfsError::NotFound)?;

        if let Err(e) = insert_result {
            self.inodes.remove(ino);
            return Err(e);
        }

        Ok(result_attr)
    }

    fn readlink(&self, ino: InodeId) -> VfsResult<Vec<u8>> {
        self.inodes
            .with_inode(ino, |data| {
                data.symlink_target.clone().ok_or(VfsError::InvalidArgument)
            })
            .ok_or(VfsError::NotFound)?
    }

    fn link(&self, ino: InodeId, newparent: InodeId, newname: &OsStr) -> VfsResult<VfsAttr> {
        let name_str = Self::name_to_string(newname)?;

        // Verify source exists and is not a directory.
        let kind = self
            .inodes
            .with_inode(ino, |data| data.attr.kind)
            .ok_or(VfsError::NotFound)?;
        if kind == FileType::Directory {
            return Err(VfsError::PermissionDenied);
        }

        // Add to new parent.
        self.inodes
            .with_inode_mut(newparent, |pdata| {
                if pdata.attr.kind != FileType::Directory {
                    return Err(VfsError::NotADirectory);
                }
                if pdata.children.contains_key(&name_str) {
                    return Err(VfsError::AlreadyExists);
                }
                pdata.children.insert(name_str, ino);
                pdata.attr.mtime = SystemTime::now();
                pdata.attr.ctime = SystemTime::now();
                Ok(())
            })
            .ok_or(VfsError::NotFound)??;

        // Increment nlink on target.
        self.inodes
            .with_inode_mut(ino, |data| {
                data.attr.nlink += 1;
                data.attr.ctime = SystemTime::now();
                data.attr.clone()
            })
            .ok_or(VfsError::NotFound)
    }

    fn getxattr(&self, ino: InodeId, name: &OsStr, size: u32) -> VfsResult<Vec<u8>> {
        let name_str = Self::name_to_string(name)?;
        self.inodes
            .with_inode(ino, |data| {
                let value = data.xattrs.get(&name_str).ok_or(VfsError::NoXattr)?;
                if size == 0 {
                    // Return the size needed.
                    let len = value.len() as u32;
                    Ok(len.to_ne_bytes().to_vec())
                } else if (size as usize) < value.len() {
                    Err(VfsError::XattrRange)
                } else {
                    Ok(value.clone())
                }
            })
            .ok_or(VfsError::NotFound)?
    }

    fn setxattr(&self, ino: InodeId, name: &OsStr, value: &[u8], _flags: i32) -> VfsResult<()> {
        let name_str = Self::name_to_string(name)?;
        self.inodes
            .with_inode_mut(ino, |data| {
                data.xattrs.insert(name_str, value.to_vec());
                data.attr.ctime = SystemTime::now();
            })
            .ok_or(VfsError::NotFound)
    }

    fn listxattr(&self, ino: InodeId, size: u32) -> VfsResult<Vec<u8>> {
        self.inodes
            .with_inode(ino, |data| {
                // Build null-separated list of xattr names.
                let mut buf = Vec::new();
                for name in data.xattrs.keys() {
                    buf.extend_from_slice(name.as_bytes());
                    buf.push(0);
                }

                if size == 0 {
                    // Return size needed.
                    let len = buf.len() as u32;
                    Ok(len.to_ne_bytes().to_vec())
                } else if (size as usize) < buf.len() {
                    Err(VfsError::XattrRange)
                } else {
                    Ok(buf)
                }
            })
            .ok_or(VfsError::NotFound)?
    }

    fn removexattr(&self, ino: InodeId, name: &OsStr) -> VfsResult<()> {
        let name_str = Self::name_to_string(name)?;
        self.inodes
            .with_inode_mut(ino, |data| {
                if data.xattrs.remove(&name_str).is_none() {
                    return Err(VfsError::NoXattr);
                }
                data.attr.ctime = SystemTime::now();
                Ok(())
            })
            .ok_or(VfsError::NotFound)?
    }

    fn statfs(&self, _ino: InodeId) -> VfsResult<StatFs> {
        Ok(StatFs::default())
    }

    fn access(&self, ino: InodeId, mask: i32) -> VfsResult<()> {
        let attr = self
            .inodes
            .with_inode(ino, |data| data.attr.clone())
            .ok_or(VfsError::NotFound)?;

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
}
