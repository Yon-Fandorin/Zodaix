use crate::SqliteBackend;
use std::ffi::OsStr;
use std::path::Path;
use zodaix_core::*;

fn backend() -> SqliteBackend {
    // Use in-memory SQLite for fast tests.
    SqliteBackend::open(":memory:").unwrap()
}

fn current_uid_gid() -> (u32, u32) {
    (
        nix::unistd::getuid().as_raw(),
        nix::unistd::getgid().as_raw(),
    )
}

// ── Basic tests (mirror memory backend) ──────────────────────────

#[test]
fn test_root_exists() {
    let fs = backend();
    let attr = fs.getattr(ROOT_INO).unwrap();
    assert_eq!(attr.ino, ROOT_INO);
    assert_eq!(attr.kind, FileType::Directory);
}

#[test]
fn test_create_file() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("hello.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    assert_eq!(attr.kind, FileType::RegularFile);
    assert_eq!(attr.perm, 0o644);
    assert_eq!(attr.size, 0);
}

#[test]
fn test_create_duplicate_file() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    fs.mknod(ROOT_INO, OsStr::new("dup.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let err = fs
        .mknod(ROOT_INO, OsStr::new("dup.txt"), 0o644, 0, uid, gid, 0)
        .unwrap_err();
    assert_eq!(err.to_errno(), libc::EEXIST);
}

#[test]
fn test_lookup() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let created = fs
        .mknod(ROOT_INO, OsStr::new("look.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let found = fs.lookup(ROOT_INO, OsStr::new("look.txt")).unwrap();
    assert_eq!(created.ino, found.ino);
}

#[test]
fn test_lookup_not_found() {
    let fs = backend();
    let err = fs.lookup(ROOT_INO, OsStr::new("nope")).unwrap_err();
    assert_eq!(err.to_errno(), libc::ENOENT);
}

#[test]
fn test_write_and_read() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("data.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let fh = fs.open(attr.ino, 0).unwrap();

    let written = fs.write(attr.ino, fh, 0, b"Hello, VFS!", 0).unwrap();
    assert_eq!(written, 11);

    let data = fs.read(attr.ino, fh, 0, 100).unwrap();
    assert_eq!(data, b"Hello, VFS!");
}

#[test]
fn test_write_offset_gap_fill() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("gap.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let fh = fs.open(attr.ino, 0).unwrap();

    // Write at offset 10, leaving a gap of zeros.
    fs.write(attr.ino, fh, 10, b"data", 0).unwrap();

    let data = fs.read(attr.ino, fh, 0, 100).unwrap();
    assert_eq!(data.len(), 14);
    assert_eq!(&data[..10], &[0u8; 10]);
    assert_eq!(&data[10..], b"data");
}

#[test]
fn test_mkdir_and_readdir() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    fs.mkdir(ROOT_INO, OsStr::new("subdir"), 0o755, 0, uid, gid)
        .unwrap();

    let fh = fs.opendir(ROOT_INO, 0).unwrap();
    let entries = fs.readdir(ROOT_INO, fh, 0).unwrap();

    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"."));
    assert!(names.contains(&".."));
    assert!(names.contains(&"subdir"));
}

#[test]
fn test_unlink() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    fs.mknod(ROOT_INO, OsStr::new("rm.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    fs.unlink(ROOT_INO, OsStr::new("rm.txt")).unwrap();
    let err = fs.lookup(ROOT_INO, OsStr::new("rm.txt")).unwrap_err();
    assert_eq!(err.to_errno(), libc::ENOENT);
}

#[test]
fn test_rmdir() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    fs.mkdir(ROOT_INO, OsStr::new("empty"), 0o755, 0, uid, gid)
        .unwrap();
    fs.rmdir(ROOT_INO, OsStr::new("empty")).unwrap();
    let err = fs.lookup(ROOT_INO, OsStr::new("empty")).unwrap_err();
    assert_eq!(err.to_errno(), libc::ENOENT);
}

#[test]
fn test_rmdir_not_empty() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let dir = fs
        .mkdir(ROOT_INO, OsStr::new("notempty"), 0o755, 0, uid, gid)
        .unwrap();
    fs.mknod(dir.ino, OsStr::new("child.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let err = fs.rmdir(ROOT_INO, OsStr::new("notempty")).unwrap_err();
    assert_eq!(err.to_errno(), libc::ENOTEMPTY);
}

#[test]
fn test_rename() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    fs.mknod(ROOT_INO, OsStr::new("old.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    fs.rename(
        ROOT_INO,
        OsStr::new("old.txt"),
        ROOT_INO,
        OsStr::new("new.txt"),
        0,
    )
    .unwrap();
    assert!(fs.lookup(ROOT_INO, OsStr::new("old.txt")).is_err());
    assert!(fs.lookup(ROOT_INO, OsStr::new("new.txt")).is_ok());
}

#[test]
fn test_symlink_and_readlink() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .symlink(
            ROOT_INO,
            OsStr::new("link"),
            Path::new("/target/path"),
            uid,
            gid,
        )
        .unwrap();
    assert_eq!(attr.kind, FileType::Symlink);
    let target = fs.readlink(attr.ino).unwrap();
    assert_eq!(target, b"/target/path");
}

#[test]
fn test_hardlink() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let file = fs
        .mknod(ROOT_INO, OsStr::new("original"), 0o644, 0, uid, gid, 0)
        .unwrap();

    let linked = fs
        .link(file.ino, ROOT_INO, OsStr::new("hardlink"))
        .unwrap();
    assert_eq!(linked.ino, file.ino);
    assert_eq!(linked.nlink, 2);

    let a = fs.lookup(ROOT_INO, OsStr::new("original")).unwrap();
    let b = fs.lookup(ROOT_INO, OsStr::new("hardlink")).unwrap();
    assert_eq!(a.ino, b.ino);
}

#[test]
fn test_setattr_truncate() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("trunc.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let fh = fs.open(attr.ino, 0).unwrap();
    fs.write(attr.ino, fh, 0, b"Hello, World!", 0).unwrap();

    let new_attr = fs
        .setattr(
            attr.ino,
            SetAttrParams {
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(new_attr.size, 5);

    let data = fs.read(attr.ino, fh, 0, 100).unwrap();
    assert_eq!(data, b"Hello");
}

#[test]
fn test_xattr_operations() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("xattr.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();

    // Set xattr.
    fs.setxattr(
        attr.ino,
        OsStr::new("user.zodaix.tags"),
        b"[\"rust\",\"vfs\"]",
        0,
    )
    .unwrap();

    // Get xattr.
    let val = fs
        .getxattr(attr.ino, OsStr::new("user.zodaix.tags"), 1024)
        .unwrap();
    assert_eq!(val, b"[\"rust\",\"vfs\"]");

    // List xattr.
    let list = fs.listxattr(attr.ino, 1024).unwrap();
    assert_eq!(list, b"user.zodaix.tags\0");

    // Remove xattr.
    fs.removexattr(attr.ino, OsStr::new("user.zodaix.tags"))
        .unwrap();
    let err = fs
        .getxattr(attr.ino, OsStr::new("user.zodaix.tags"), 1024)
        .unwrap_err();
    assert_eq!(err.to_errno(), VfsError::NoXattr.to_errno());
}

#[test]
fn test_readdir_with_offset() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();

    for name in &["a.txt", "b.txt", "c.txt"] {
        fs.mknod(ROOT_INO, OsStr::new(name), 0o644, 0, uid, gid, 0)
            .unwrap();
    }

    let fh = fs.opendir(ROOT_INO, 0).unwrap();
    let all = fs.readdir(ROOT_INO, fh, 0).unwrap();
    assert_eq!(all.len(), 5); // ".", "..", a, b, c

    let partial = fs.readdir(ROOT_INO, fh, 2).unwrap();
    assert_eq!(partial.len(), 3);

    let empty = fs.readdir(ROOT_INO, fh, 10).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn test_nested_directory_operations() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();

    let dir = fs
        .mkdir(ROOT_INO, OsStr::new("level1"), 0o755, 0, uid, gid)
        .unwrap();
    let subdir = fs
        .mkdir(dir.ino, OsStr::new("level2"), 0o755, 0, uid, gid)
        .unwrap();
    fs.mknod(subdir.ino, OsStr::new("deep.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();

    let found_dir = fs.lookup(ROOT_INO, OsStr::new("level1")).unwrap();
    let found_sub = fs.lookup(found_dir.ino, OsStr::new("level2")).unwrap();
    let found_file = fs.lookup(found_sub.ino, OsStr::new("deep.txt")).unwrap();
    assert_eq!(found_file.kind, FileType::RegularFile);
}

#[test]
fn test_rename_across_directories() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();

    let dir_a = fs
        .mkdir(ROOT_INO, OsStr::new("dir_a"), 0o755, 0, uid, gid)
        .unwrap();
    let dir_b = fs
        .mkdir(ROOT_INO, OsStr::new("dir_b"), 0o755, 0, uid, gid)
        .unwrap();
    fs.mknod(dir_a.ino, OsStr::new("file.txt"), 0o644, 0, uid, gid, 0)
        .unwrap();

    fs.rename(
        dir_a.ino,
        OsStr::new("file.txt"),
        dir_b.ino,
        OsStr::new("moved.txt"),
        0,
    )
    .unwrap();

    assert!(fs.lookup(dir_a.ino, OsStr::new("file.txt")).is_err());
    assert!(fs.lookup(dir_b.ino, OsStr::new("moved.txt")).is_ok());
}

// ── SQLite-specific tests ────────────────────────────────────────

#[test]
fn test_persistence_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db_str = db_path.display().to_string();
    let (uid, gid) = current_uid_gid();

    // Create some data.
    {
        let fs = SqliteBackend::open(&db_str).unwrap();
        fs.mknod(ROOT_INO, OsStr::new("persist.txt"), 0o644, 0, uid, gid, 0)
            .unwrap();
        let attr = fs.lookup(ROOT_INO, OsStr::new("persist.txt")).unwrap();
        let fh = fs.open(attr.ino, 0).unwrap();
        fs.write(attr.ino, fh, 0, b"persistent data", 0).unwrap();
        fs.mkdir(ROOT_INO, OsStr::new("mydir"), 0o755, 0, uid, gid)
            .unwrap();
        fs.shutdown().unwrap();
    }

    // Reopen and verify data survived.
    {
        let fs = SqliteBackend::open(&db_str).unwrap();
        let attr = fs.lookup(ROOT_INO, OsStr::new("persist.txt")).unwrap();
        assert_eq!(attr.kind, FileType::RegularFile);
        let data = fs.read(attr.ino, 0, 0, 100).unwrap();
        assert_eq!(data, b"persistent data");

        let dir_attr = fs.lookup(ROOT_INO, OsStr::new("mydir")).unwrap();
        assert_eq!(dir_attr.kind, FileType::Directory);
    }
}

#[test]
fn test_large_file_chunked_io() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("large.bin"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let fh = fs.open(attr.ino, 0).unwrap();

    // Write 10KB of data (spans multiple 4KB chunks).
    let data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect();
    let written = fs.write(attr.ino, fh, 0, &data, 0).unwrap();
    assert_eq!(written, 10240);

    // Read it all back.
    let read_data = fs.read(attr.ino, fh, 0, 10240).unwrap();
    assert_eq!(read_data.len(), 10240);
    assert_eq!(read_data, data);

    // Read a slice spanning chunk boundary (around 4096).
    let slice = fs.read(attr.ino, fh, 4090, 20).unwrap();
    assert_eq!(slice.len(), 20);
    assert_eq!(slice, &data[4090..4110]);

    // Verify file size in attrs.
    let attr = fs.getattr(attr.ino).unwrap();
    assert_eq!(attr.size, 10240);
}

#[test]
fn test_hardlink_shared_data() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();
    let file = fs
        .mknod(ROOT_INO, OsStr::new("src"), 0o644, 0, uid, gid, 0)
        .unwrap();
    let fh = fs.open(file.ino, 0).unwrap();
    fs.write(file.ino, fh, 0, b"shared", 0).unwrap();

    // Create hardlink.
    fs.link(file.ino, ROOT_INO, OsStr::new("dst")).unwrap();

    // Both names read the same data.
    let a = fs.lookup(ROOT_INO, OsStr::new("src")).unwrap();
    let b = fs.lookup(ROOT_INO, OsStr::new("dst")).unwrap();
    assert_eq!(a.ino, b.ino);

    let data_a = fs.read(a.ino, 0, 0, 100).unwrap();
    assert_eq!(data_a, b"shared");

    // Unlink original — data still accessible via hardlink.
    fs.unlink(ROOT_INO, OsStr::new("src")).unwrap();
    let data_b = fs.read(b.ino, 0, 0, 100).unwrap();
    assert_eq!(data_b, b"shared");

    // Now nlink = 1. Unlink the last one — inode should be gone.
    fs.unlink(ROOT_INO, OsStr::new("dst")).unwrap();
    assert!(fs.getattr(b.ino).is_err());
}

#[test]
fn test_backend_name_and_capabilities() {
    let fs = backend();
    assert_eq!(fs.name(), "sqlite");
    let caps = fs.capabilities();
    assert!(caps.contains(BackendCapabilities::PERSISTENT));
    assert!(caps.contains(BackendCapabilities::SEARCH));
    assert!(caps.contains(BackendCapabilities::HARDLINKS));
    assert!(caps.contains(BackendCapabilities::SYMLINKS));
    assert!(caps.contains(BackendCapabilities::XATTRS));
}

#[test]
fn test_fts5_search() {
    let fs = backend();
    let (uid, gid) = current_uid_gid();

    // Create a file and tag it.
    let attr = fs
        .mknod(ROOT_INO, OsStr::new("auth.rs"), 0o644, 0, uid, gid, 0)
        .unwrap();
    fs.setxattr(
        attr.ino,
        OsStr::new("user.zodaix.tags"),
        b"rust,auth",
        0,
    )
    .unwrap();
    fs.setxattr(
        attr.ino,
        OsStr::new("user.zodaix.description"),
        b"Authentication module",
        0,
    )
    .unwrap();

    // Search for it.
    let results = fs.search("auth", 10).unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].ino, attr.ino);
}

#[test]
fn test_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    // Use a file-based DB for thread safety test.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("concurrent.db");
    let db_str = db_path.display().to_string();

    let fs = Arc::new(SqliteBackend::open(&db_str).unwrap());
    let (uid, gid) = current_uid_gid();

    let mut handles = vec![];
    for i in 0..10 {
        let fs = Arc::clone(&fs);
        handles.push(thread::spawn(move || {
            let name = format!("file_{i}.txt");
            fs.mknod(ROOT_INO, OsStr::new(&name), 0o644, 0, uid, gid, 0)
                .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let fh = fs.opendir(ROOT_INO, 0).unwrap();
    let entries = fs.readdir(ROOT_INO, fh, 0).unwrap();
    assert_eq!(entries.len(), 12); // 10 files + "." + ".."
}

#[test]
fn test_registry_create() {
    let mut registry = BackendRegistry::new();
    super::register(&mut registry);

    let names = registry.list();
    assert!(names.contains(&"sqlite"));

    // Create with in-memory path.
    let mut config = BackendConfig::default();
    config.params.insert("db".to_string(), ":memory:".to_string());
    let backend = registry.create("sqlite", config).unwrap();
    assert_eq!(backend.name(), "sqlite");
}
