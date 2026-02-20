use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use zodaix_core::{InodeId, VfsAttr, ROOT_INO};

/// Data stored for each inode.
#[derive(Debug)]
pub struct InodeData {
    pub attr: VfsAttr,
    /// File content bytes (only for regular files).
    pub content: Vec<u8>,
    /// Children map: name → inode (only for directories).
    pub children: BTreeMap<String, InodeId>,
    /// Symlink target (only for symlinks).
    pub symlink_target: Option<Vec<u8>>,
    /// Extended attributes.
    pub xattrs: BTreeMap<String, Vec<u8>>,
    /// Parent inode (for `..` traversal).
    pub parent: InodeId,
}

impl InodeData {
    pub fn new_file(attr: VfsAttr, parent: InodeId) -> Self {
        Self {
            attr,
            content: Vec::new(),
            children: BTreeMap::new(),
            symlink_target: None,
            xattrs: BTreeMap::new(),
            parent,
        }
    }

    pub fn new_dir(attr: VfsAttr, parent: InodeId) -> Self {
        Self {
            attr,
            content: Vec::new(),
            children: BTreeMap::new(),
            symlink_target: None,
            xattrs: BTreeMap::new(),
            parent,
        }
    }

    pub fn new_symlink(attr: VfsAttr, target: Vec<u8>, parent: InodeId) -> Self {
        let mut data = Self {
            attr,
            content: Vec::new(),
            children: BTreeMap::new(),
            symlink_target: Some(target.clone()),
            xattrs: BTreeMap::new(),
            parent,
        };
        data.attr.size = target.len() as u64;
        data
    }
}

/// Thread-safe inode table backed by DashMap.
///
/// Uses DashMap's built-in shard-level locking directly (no additional RwLock).
pub struct InodeTable {
    map: DashMap<InodeId, InodeData>,
    next_ino: AtomicU64,
    next_fh: AtomicU64,
}

impl InodeTable {
    /// Create a new inode table with root directory pre-created.
    pub fn new(uid: u32, gid: u32) -> Self {
        let table = Self {
            map: DashMap::new(),
            next_ino: AtomicU64::new(ROOT_INO + 1),
            next_fh: AtomicU64::new(1),
        };

        let root_attr = VfsAttr::new_dir(ROOT_INO, 0o755, uid, gid);
        let root_data = InodeData::new_dir(root_attr, ROOT_INO);
        table.map.insert(ROOT_INO, root_data);

        table
    }

    /// Allocate the next inode number.
    pub fn alloc_ino(&self) -> InodeId {
        self.next_ino.fetch_add(1, Ordering::Relaxed)
    }

    /// Allocate the next file handle.
    pub fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert a new inode.
    pub fn insert(&self, ino: InodeId, data: InodeData) {
        self.map.insert(ino, data);
    }

    /// Remove an inode.
    pub fn remove(&self, ino: InodeId) -> Option<InodeData> {
        self.map.remove(&ino).map(|(_, data)| data)
    }

    /// Check if an inode exists.
    pub fn contains(&self, ino: InodeId) -> bool {
        self.map.contains_key(&ino)
    }

    /// Get a read-only view of inode data. Calls `f` with the data while holding
    /// DashMap's shard read lock.
    pub fn with_inode<F, R>(&self, ino: InodeId, f: F) -> Option<R>
    where
        F: FnOnce(&InodeData) -> R,
    {
        self.map.get(&ino).map(|entry| f(entry.value()))
    }

    /// Get a mutable view of inode data. Calls `f` with the data while holding
    /// DashMap's shard write lock.
    pub fn with_inode_mut<F, R>(&self, ino: InodeId, f: F) -> Option<R>
    where
        F: FnOnce(&mut InodeData) -> R,
    {
        self.map.get_mut(&ino).map(|mut entry| f(entry.value_mut()))
    }
}

impl std::fmt::Debug for InodeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InodeTable")
            .field("count", &self.map.len())
            .field("next_ino", &self.next_ino.load(Ordering::Relaxed))
            .finish()
    }
}
