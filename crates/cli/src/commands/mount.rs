use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};
use zodaix_core::{BackendConfig, BackendRegistry, VfsBackend};

#[derive(Debug, Clone, ValueEnum)]
pub enum Transport {
    /// Use FUSE (requires macFUSE/FUSE-T).
    Fuse,
    /// Use NFSv3 server (no external dependencies).
    Nfs,
    /// Auto-detect: try FUSE first, fall back to NFS.
    Auto,
}

#[derive(Args, Debug)]
pub struct MountArgs {
    /// Mount point path.
    pub mountpoint: PathBuf,

    /// Backend type: "memory", "sqlite", or "local".
    #[arg(long, default_value = "sqlite")]
    pub backend: String,

    /// SQLite database path (for sqlite backend).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Root directory for the local backend.
    #[arg(long)]
    pub root: Option<PathBuf>,

    /// Transport layer: fuse, nfs, or auto.
    #[arg(long, value_enum, default_value = "auto")]
    pub transport: Transport,

    /// NFS server port (only used with nfs/auto transport).
    #[arg(long, default_value_t = zodaix_nfs::DEFAULT_NFS_PORT)]
    pub nfs_port: u16,

    /// Run in foreground (don't daemonize).
    #[arg(short, long)]
    pub foreground: bool,
}

/// Build the backend registry with all available backends.
fn build_registry() -> BackendRegistry {
    let mut registry = BackendRegistry::new();
    zodaix_memory::register(&mut registry);
    zodaix_sqlite::register(&mut registry);
    registry
}

/// Create a backend instance from CLI args using the registry.
fn create_backend(args: &MountArgs) -> Result<Box<dyn VfsBackend>> {
    // Handle local backend specially (not in registry, needs root path).
    if args.backend == "local" {
        let root = args.root.clone().unwrap_or_else(|| PathBuf::from("."));
        info!("Using local backend with root: {}", root.display());
        return Ok(Box::new(zodaix_local::LocalBackend::new(root).map_err(
            |e| anyhow::anyhow!("Failed to create local backend: {e}"),
        )?));
    }

    let registry = build_registry();
    let mut config = BackendConfig::default();

    // Pass db path for sqlite backend.
    if let Some(db) = &args.db {
        config
            .params
            .insert("db".to_string(), db.display().to_string());
    }

    let backend = registry
        .create(&args.backend, config)
        .map_err(|e| anyhow::anyhow!("Failed to create backend '{}': {e}", args.backend))?;

    info!("Using {} backend", backend.name());
    Ok(backend)
}

pub fn run(args: MountArgs) -> Result<()> {
    let mountpoint = &args.mountpoint;

    // Ensure mountpoint exists.
    if !mountpoint.exists() {
        std::fs::create_dir_all(mountpoint)
            .with_context(|| format!("Failed to create mountpoint: {}", mountpoint.display()))?;
    }

    match args.transport {
        Transport::Fuse => {
            let backend = create_backend(&args)?;
            fuse_mount(backend, mountpoint)
        }
        Transport::Nfs => {
            let backend = create_backend(&args)?;
            nfs_mount(Arc::from(backend), mountpoint, args.nfs_port)
        }
        Transport::Auto => {
            info!("Auto-detecting transport...");
            if is_fuse_available() {
                info!("FUSE detected, using FUSE transport");
                let backend = create_backend(&args)?;
                fuse_mount(backend, mountpoint)
            } else {
                info!("FUSE not available, falling back to NFS transport");
                let backend = create_backend(&args)?;
                nfs_mount(Arc::from(backend), mountpoint, args.nfs_port)
            }
        }
    }
}

/// Check if FUSE is available on this system.
fn is_fuse_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        // macFUSE: kext must be loaded, creating /dev/macfuse* devices.
        // The .fs bundle can exist without the driver being active.
        if Path::new("/dev/macfuse0").exists() {
            return true;
        }
        // FUSE-T: uses a userspace implementation, check for its library.
        if Path::new("/Library/Filesystems/fuse-t.fs/Contents/Resources/mount_fuse-t").exists() {
            return true;
        }
        false
    }
    #[cfg(target_os = "linux")]
    {
        Path::new("/dev/fuse").exists()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}

/// Mount using FUSE transport.
fn fuse_mount(backend: Box<dyn VfsBackend>, mountpoint: &Path) -> Result<()> {
    let fuse_fs = zodaix_fuse::ZodaixFuse::new(backend);

    info!("Mounting Zodaix VFS at {} (FUSE)", mountpoint.display());
    info!("Press Ctrl+C to unmount");

    let mut config = fuser::Config::default();
    config
        .mount_options
        .push(fuser::MountOption::FSName("zodaix".to_string()));
    config.mount_options.push(fuser::MountOption::AutoUnmount);
    config.acl = fuser::SessionACL::All;

    #[cfg(target_os = "macos")]
    {
        config
            .mount_options
            .push(fuser::MountOption::CUSTOM("volname=Zodaix".to_string()));
    }

    fuser::mount2(fuse_fs, mountpoint, &config)
        .with_context(|| format!("Failed to mount at {}", mountpoint.display()))?;

    info!("Zodaix VFS unmounted");
    Ok(())
}

/// Mount using NFS transport.
fn nfs_mount(backend: Arc<dyn VfsBackend>, mountpoint: &Path, port: u16) -> Result<()> {
    info!(
        "Mounting Zodaix VFS at {} (NFS on port {})",
        mountpoint.display(),
        port
    );

    let rt = tokio::runtime::Runtime::new().context("Failed to create tokio runtime")?;
    let mountpoint = mountpoint.to_path_buf();

    rt.block_on(async move {
        let server = zodaix_nfs::NfsServer::bind(backend, port)
            .await
            .context("Failed to start NFS server")?;

        info!("NFSv3 server listening on port {}", server.port());

        // Start handling NFS requests in background BEFORE mounting.
        // mount_nfs needs the server to be actively responding.
        let server_task = tokio::spawn(async move { server.handle_forever().await });

        // Mount NFS filesystem.
        let mount_result = tokio::task::spawn_blocking({
            let mp = mountpoint.clone();
            move || nfs_mount_command(&mp, port)
        })
        .await
        .map_err(|e| anyhow::anyhow!("mount task panicked: {e}"))?;

        if let Err(e) = mount_result {
            server_task.abort();
            return Err(e);
        }

        info!("NFS mount successful at {}", mountpoint.display());
        info!("Press Ctrl+C to unmount");

        // Wait for Ctrl+C.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = std::sync::Mutex::new(Some(tx));
        ctrlc::set_handler(move || {
            if let Some(tx) = tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
        })
        .context("Failed to set Ctrl+C handler")?;

        tokio::select! {
            result = server_task => {
                result.context("NFS server task panicked")?
                    .context("NFS server error")?;
            }
            _ = rx => {
                info!("Received Ctrl+C, unmounting...");
            }
        }

        // Unmount.
        nfs_unmount_command(&mountpoint)?;
        info!("Zodaix VFS unmounted");
        Ok(())
    })
}

/// Execute the platform-specific NFS mount command.
fn nfs_mount_command(mountpoint: &Path, port: u16) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("mount_nfs")
            .args([
                "-o",
                &format!("port={port},mountport={port},vers=3,tcp,nolocks"),
                "localhost:/",
                &mountpoint.display().to_string(),
            ])
            .status()
            .context("Failed to run mount_nfs")?;

        if !status.success() {
            anyhow::bail!("mount_nfs failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("mount")
            .args([
                "-t",
                "nfs",
                "-o",
                &format!("port={port},mountport={port},nfsvers=3,tcp,nolock"),
                "localhost:/",
                &mountpoint.display().to_string(),
            ])
            .status()
            .context("Failed to run mount -t nfs")?;

        if !status.success() {
            anyhow::bail!("mount -t nfs failed with exit code: {:?}", status.code());
        }
    }

    Ok(())
}

/// Execute platform-specific unmount command.
fn nfs_unmount_command(mountpoint: &Path) -> Result<()> {
    let status = std::process::Command::new("umount")
        .arg(mountpoint.display().to_string())
        .status()
        .context("Failed to run umount")?;

    if !status.success() {
        warn!("umount failed with exit code: {:?}", status.code());
    }
    Ok(())
}
