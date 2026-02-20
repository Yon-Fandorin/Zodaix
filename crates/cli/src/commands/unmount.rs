use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;
use tracing::info;

#[derive(Args, Debug)]
pub struct UnmountArgs {
    /// Mount point to unmount.
    pub mountpoint: PathBuf,
}

pub fn run(args: UnmountArgs) -> Result<()> {
    info!("Unmounting {}", args.mountpoint.display());

    // Use platform-specific unmount command.
    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("umount")
            .arg(&args.mountpoint)
            .status()
            .context("Failed to run umount")?;
        if !status.success() {
            anyhow::bail!("umount failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("fusermount")
            .arg("-u")
            .arg(&args.mountpoint)
            .status()
            .context("Failed to run fusermount -u")?;
        if !status.success() {
            anyhow::bail!("fusermount -u failed with exit code: {:?}", status.code());
        }
    }

    info!("Successfully unmounted {}", args.mountpoint.display());
    Ok(())
}
