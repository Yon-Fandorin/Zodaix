use crate::convert::*;
use nfs3_server::nfs3_types::nfs3::*;
use nfs3_server::vfs::{
    DirEntryPlus, FileHandleU64, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use tracing::debug;
use zodaix_core::{FileType, VfsBackend, ROOT_INO};

/// NFSv3 bridge wrapping a VfsBackend.
pub struct ZodaixNfs {
    backend: Arc<dyn VfsBackend>,
}

impl ZodaixNfs {
    pub fn new(backend: Arc<dyn VfsBackend>) -> Self {
        Self { backend }
    }

    /// Convert filename3 bytes to OsStr.
    fn filename_to_osstr<'a>(name: &'a filename3<'_>) -> &'a OsStr {
        OsStr::from_bytes(name.as_ref())
    }

    /// Convert handle to inode id.
    fn handle_to_ino(handle: &FileHandleU64) -> u64 {
        handle.as_u64()
    }
}

impl NfsReadFileSystem for ZodaixNfs {
    type Handle = FileHandleU64;

    fn root_dir(&self) -> Self::Handle {
        FileHandleU64::new(ROOT_INO)
    }

    async fn lookup(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
    ) -> Result<Self::Handle, nfsstat3> {
        let parent = Self::handle_to_ino(dirid);
        let name = Self::filename_to_osstr(filename);
        let backend = self.backend.clone();
        let name_owned = name.to_os_string();

        let attr = tokio::task::spawn_blocking(move || backend.lookup(parent, &name_owned))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        debug!(
            "NFS lookup: parent={}, name={:?} -> ino={}",
            parent, name, attr.ino
        );
        Ok(FileHandleU64::new(attr.ino))
    }

    async fn getattr(&self, id: &Self::Handle) -> Result<fattr3, nfsstat3> {
        let ino = Self::handle_to_ino(id);
        let backend = self.backend.clone();

        let attr = tokio::task::spawn_blocking(move || backend.getattr(ino))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        Ok(vfs_attr_to_fattr3(&attr))
    }

    async fn read(
        &self,
        id: &Self::Handle,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let ino = Self::handle_to_ino(id);
        let backend = self.backend.clone();

        // Guard against u64 offsets that would become negative as i64.
        if offset > i64::MAX as u64 {
            return Err(nfsstat3::NFS3ERR_FBIG);
        }
        // NFS is stateless — use fh=0.
        let data = tokio::task::spawn_blocking(move || backend.read(ino, 0, offset as i64, count))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        let eof = (data.len() as u32) < count;
        Ok((data, eof))
    }

    async fn readdirplus(
        &self,
        dirid: &Self::Handle,
        cookie: u64,
    ) -> Result<impl ReadDirPlusIterator<Self::Handle>, nfsstat3> {
        let ino = Self::handle_to_ino(dirid);
        let backend = self.backend.clone();

        // Batch: readdir + getattr for all entries in a single spawn_blocking call
        // to avoid N+1 problem (one spawn_blocking per entry).
        // Clamp cookie to i64::MAX to prevent negative offset in backend.
        let safe_cookie = cookie.min(i64::MAX as u64);
        let plus_entries = tokio::task::spawn_blocking(move || {
            let entries = backend.readdir(ino, 0, safe_cookie as i64)
                .map_err(|e| vfs_error_to_nfsstat(&e))?;

            let mut result = Vec::with_capacity(entries.len());
            for (i, entry) in entries.into_iter().enumerate() {
                let entry_cookie = safe_cookie.saturating_add(i as u64 + 1);
                let handle = FileHandleU64::new(entry.ino);
                let attrs = backend.getattr(entry.ino).ok().map(|a| vfs_attr_to_fattr3(&a));

                result.push(DirEntryPlus {
                    fileid: entry.ino,
                    name: filename3::from(entry.name.into_bytes()),
                    cookie: entry_cookie,
                    name_attributes: attrs,
                    name_handle: Some(handle),
                });
            }
            Ok::<_, nfsstat3>(result)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)??;

        Ok(VecDirPlusIter {
            entries: plus_entries,
            pos: 0,
        })
    }

    async fn readlink(&self, id: &Self::Handle) -> Result<nfspath3<'_>, nfsstat3> {
        let ino = Self::handle_to_ino(id);
        let backend = self.backend.clone();

        let target = tokio::task::spawn_blocking(move || backend.readlink(ino))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        Ok(nfspath3::from(target))
    }
}

impl NfsFileSystem for ZodaixNfs {
    async fn setattr(&self, id: &Self::Handle, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let ino = Self::handle_to_ino(id);
        let params = sattr3_to_set_attr_params(&setattr);
        let backend = self.backend.clone();

        let attr = tokio::task::spawn_blocking(move || backend.setattr(ino, params))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        Ok(vfs_attr_to_fattr3(&attr))
    }

    async fn write(&self, id: &Self::Handle, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let ino = Self::handle_to_ino(id);
        let backend = self.backend.clone();
        let data = data.to_vec();

        // Guard against u64 offsets that would become negative as i64.
        if offset > i64::MAX as u64 {
            return Err(nfsstat3::NFS3ERR_FBIG);
        }
        tokio::task::spawn_blocking(move || backend.write(ino, 0, offset as i64, &data, 0))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        // Return updated attributes after write.
        self.getattr(id).await
    }

    async fn create(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
        attr: sattr3,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let parent = Self::handle_to_ino(dirid);
        let name = Self::filename_to_osstr(filename).to_os_string();
        let mode = sattr3_mode_or_default(&attr, 0o644) | libc::S_IFREG as u32;
        let uid = sattr3_uid(&attr);
        let gid = sattr3_gid(&attr);
        let backend = self.backend.clone();

        let vfs_attr = tokio::task::spawn_blocking(move || {
            backend.mknod(parent, &name, mode, 0o022, uid, gid, 0)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
        .map_err(|e| vfs_error_to_nfsstat(&e))?;

        let handle = FileHandleU64::new(vfs_attr.ino);
        let fattr = vfs_attr_to_fattr3(&vfs_attr);
        Ok((handle, fattr))
    }

    async fn create_exclusive(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
        _createverf: createverf3,
    ) -> Result<Self::Handle, nfsstat3> {
        let attr = sattr3 {
            mode: Nfs3Option::Some(0o644),
            uid: Nfs3Option::None,
            gid: Nfs3Option::None,
            size: Nfs3Option::None,
            atime: set_atime::default(),
            mtime: set_mtime::default(),
        };
        let (handle, _) = self.create(dirid, filename, attr).await?;
        Ok(handle)
    }

    async fn mkdir(
        &self,
        dirid: &Self::Handle,
        dirname: &filename3<'_>,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let parent = Self::handle_to_ino(dirid);
        let name = Self::filename_to_osstr(dirname).to_os_string();
        let uid = nix::unistd::getuid().as_raw();
        let gid = nix::unistd::getgid().as_raw();
        let backend = self.backend.clone();

        let vfs_attr = tokio::task::spawn_blocking(move || {
            backend.mkdir(parent, &name, 0o755 | libc::S_IFDIR as u32, 0o022, uid, gid)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
        .map_err(|e| vfs_error_to_nfsstat(&e))?;

        let handle = FileHandleU64::new(vfs_attr.ino);
        let fattr = vfs_attr_to_fattr3(&vfs_attr);
        Ok((handle, fattr))
    }

    async fn remove(&self, dirid: &Self::Handle, filename: &filename3<'_>) -> Result<(), nfsstat3> {
        let parent = Self::handle_to_ino(dirid);
        let name = Self::filename_to_osstr(filename).to_os_string();
        let backend = self.backend.clone();

        // Check if it's a file or directory, then call appropriate method.
        let backend2 = backend.clone();
        let name2 = name.clone();
        let lookup_result = tokio::task::spawn_blocking(move || backend2.lookup(parent, &name2))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
            .map_err(|e| vfs_error_to_nfsstat(&e))?;

        if lookup_result.kind == FileType::Directory {
            tokio::task::spawn_blocking(move || backend.rmdir(parent, &name))
                .await
                .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
                .map_err(|e| vfs_error_to_nfsstat(&e))
        } else {
            tokio::task::spawn_blocking(move || backend.unlink(parent, &name))
                .await
                .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
                .map_err(|e| vfs_error_to_nfsstat(&e))
        }
    }

    async fn rename<'a>(
        &self,
        from_dirid: &Self::Handle,
        from_filename: &filename3<'a>,
        to_dirid: &Self::Handle,
        to_filename: &filename3<'a>,
    ) -> Result<(), nfsstat3> {
        let from_parent = Self::handle_to_ino(from_dirid);
        let from_name = Self::filename_to_osstr(from_filename).to_os_string();
        let to_parent = Self::handle_to_ino(to_dirid);
        let to_name = Self::filename_to_osstr(to_filename).to_os_string();
        let backend = self.backend.clone();

        tokio::task::spawn_blocking(move || {
            backend.rename(from_parent, &from_name, to_parent, &to_name, 0)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
        .map_err(|e| vfs_error_to_nfsstat(&e))
    }

    async fn symlink<'a>(
        &self,
        dirid: &Self::Handle,
        linkname: &filename3<'a>,
        symlink_target: &nfspath3<'a>,
        attr: &sattr3,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let parent = Self::handle_to_ino(dirid);
        let name = Self::filename_to_osstr(linkname).to_os_string();
        let target_bytes: &[u8] = symlink_target.as_ref();
        let target = Path::new(OsStr::from_bytes(target_bytes)).to_path_buf();
        let uid = sattr3_uid(attr);
        let gid = sattr3_gid(attr);
        let backend = self.backend.clone();

        let vfs_attr =
            tokio::task::spawn_blocking(move || backend.symlink(parent, &name, &target, uid, gid))
                .await
                .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
                .map_err(|e| vfs_error_to_nfsstat(&e))?;

        let handle = FileHandleU64::new(vfs_attr.ino);
        let fattr = vfs_attr_to_fattr3(&vfs_attr);
        Ok((handle, fattr))
    }
}

/// A simple vec-backed ReadDirPlusIterator.
struct VecDirPlusIter {
    entries: Vec<DirEntryPlus<FileHandleU64>>,
    pos: usize,
}

impl ReadDirPlusIterator<FileHandleU64> for VecDirPlusIter {
    async fn next(&mut self) -> NextResult<DirEntryPlus<FileHandleU64>> {
        if self.pos >= self.entries.len() {
            return NextResult::Eof;
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        NextResult::Ok(entry)
    }
}
