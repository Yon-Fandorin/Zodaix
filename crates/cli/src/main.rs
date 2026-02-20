mod commands;
#[allow(dead_code)]
mod config;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "zodaix",
    version,
    about = "Zodaix - AI-friendly Virtual File System"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Mount a Zodaix VFS.
    Mount(commands::mount::MountArgs),

    /// Unmount a Zodaix VFS.
    Unmount(commands::unmount::UnmountArgs),

    /// Show active Zodaix mounts.
    Status(commands::status::StatusArgs),

    /// Manage file tags.
    Tag(commands::tag::TagArgs),

    /// Search indexed files.
    Search(commands::search::SearchArgs),
}

fn main() {
    let cli = Cli::parse();

    // Initialize logging.
    let default_filter = if cli.verbose {
        "debug,nfs3_server=info"
    } else {
        "info,nfs3_server=error"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .with_target(false)
        .init();

    let result = match cli.command {
        Commands::Mount(args) => commands::mount::run(args),
        Commands::Unmount(args) => commands::unmount::run(args),
        Commands::Status(args) => commands::status::run(args),
        Commands::Tag(args) => commands::tag::run(args),
        Commands::Search(args) => commands::search::run(args),
    };

    if let Err(e) = result {
        eprintln!("Error: {e:#}");
        std::process::exit(1);
    }
}
