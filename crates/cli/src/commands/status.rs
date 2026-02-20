use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct StatusArgs {}

pub fn run(_args: StatusArgs) -> Result<()> {
    // Check for active Zodaix mounts by parsing system mount list.
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("mount")
            .output()
            .map_err(|e| anyhow::anyhow!("Failed to run mount: {e}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);

        let mut found = false;
        for line in stdout.lines() {
            if line.contains("zodaix") || line.contains("macfuse") {
                println!("{line}");
                found = true;
            }
        }

        if !found {
            println!("No active Zodaix mounts found.");
        }
    }

    #[cfg(target_os = "linux")]
    {
        let output = std::process::Command::new("mount")
            .output()
            .map_err(|e| anyhow::anyhow!("Failed to run mount: {e}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);

        let mut found = false;
        for line in stdout.lines() {
            if line.contains("zodaix") || line.contains("fuse") {
                println!("{line}");
                found = true;
            }
        }

        if !found {
            println!("No active Zodaix mounts found.");
        }
    }

    Ok(())
}
