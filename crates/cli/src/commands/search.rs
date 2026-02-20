use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;
use zodaix_core::VfsBackend;

#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Search query.
    pub query: String,

    /// Maximum number of results.
    #[arg(long, default_value = "20")]
    pub limit: usize,

    /// SQLite database path (default: ~/.local/share/zodaix/default.db).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Legacy: Tantivy index directory.
    #[arg(long, hide = true)]
    pub index_dir: Option<PathBuf>,
}

pub fn run(args: SearchArgs) -> Result<()> {
    // If --index-dir is provided, try legacy Tantivy search.
    if let Some(index_dir) = &args.index_dir {
        if index_dir.exists() {
            let index = zodaix_metadata::SearchIndex::open(index_dir)
                .context("Failed to open search index")?;
            let results = index.search(&args.query, args.limit).context("Search failed")?;
            print_legacy_results(&args.query, &results);
            return Ok(());
        }
    }

    // Default: SQLite FTS5 search.
    let db_path = args.db.as_ref().map(|p| p.display().to_string());
    let db_path = db_path.unwrap_or_else(default_db_path);

    if !std::path::Path::new(&db_path).exists() {
        println!("No database found at {db_path}. Create one by mounting with: zodaix mount <mountpoint> --backend sqlite --db {db_path}");
        return Ok(());
    }

    let backend = zodaix_sqlite::SqliteBackend::open(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {e}"))?;

    let results = backend
        .search(&args.query, args.limit)
        .map_err(|e| anyhow::anyhow!("Search failed: {e}"))?;

    if results.is_empty() {
        println!("No results found for '{}'", args.query);
    } else {
        println!("Found {} result(s):\n", results.len());
        for result in &results {
            println!("  {} (score: {:.2})", result.path, result.score);
            if !result.tags.is_empty() {
                println!("    tags: {}", result.tags.join(", "));
            }
            if !result.description.is_empty() {
                println!("    desc: {}", result.description);
            }
        }
    }

    Ok(())
}

fn print_legacy_results(query: &str, results: &[zodaix_metadata::SearchResult]) {
    if results.is_empty() {
        println!("No results found for '{query}'");
    } else {
        println!("Found {} result(s):\n", results.len());
        for result in results {
            println!("  {} (score: {:.2})", result.path, result.score);
            if !result.tags.is_empty() {
                println!("    tags: {}", result.tags.join(", "));
            }
            if !result.description.is_empty() {
                println!("    desc: {}", result.description);
            }
        }
    }
}

fn default_db_path() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{home}/.local/share/zodaix/default.db")
}
