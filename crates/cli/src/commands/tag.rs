use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::path::PathBuf;
use zodaix_metadata::{xattr_keys, Tags};

#[derive(Args, Debug)]
pub struct TagArgs {
    #[command(subcommand)]
    pub action: TagAction,
}

#[derive(Subcommand, Debug)]
pub enum TagAction {
    /// Add a tag to a file.
    Add {
        /// File path.
        file: PathBuf,
        /// Tag to add.
        tag: String,
    },
    /// Remove a tag from a file.
    Remove {
        /// File path.
        file: PathBuf,
        /// Tag to remove.
        tag: String,
    },
    /// List tags on a file.
    List {
        /// File path.
        file: PathBuf,
    },
}

pub fn run(args: TagArgs) -> Result<()> {
    match args.action {
        TagAction::Add { file, tag } => {
            let mut tags = read_tags(&file)?;
            tags.add(&tag);
            write_tags(&file, &tags)?;
            println!("Added tag '{tag}' to {}", file.display());
        }
        TagAction::Remove { file, tag } => {
            let mut tags = read_tags(&file)?;
            if tags.remove(&tag) {
                write_tags(&file, &tags)?;
                println!("Removed tag '{tag}' from {}", file.display());
            } else {
                println!("Tag '{tag}' not found on {}", file.display());
            }
        }
        TagAction::List { file } => {
            let tags = read_tags(&file)?;
            if tags.is_empty() {
                println!("No tags on {}", file.display());
            } else {
                for tag in tags.list() {
                    println!("{tag}");
                }
            }
        }
    }
    Ok(())
}

fn read_tags(path: &PathBuf) -> Result<Tags> {
    match xattr::get(path, xattr_keys::TAGS) {
        Ok(Some(data)) => Tags::from_json(&data).context("Failed to parse tags"),
        Ok(None) => Ok(Tags::new()),
        Err(e) => {
            // ENOATTR / ENODATA means no tags set yet.
            if e.raw_os_error() == Some(libc::ENODATA) || e.raw_os_error() == Some(93)
            // macOS ENOATTR
            {
                Ok(Tags::new())
            } else {
                Err(e).context("Failed to read tags")
            }
        }
    }
}

fn write_tags(path: &PathBuf, tags: &Tags) -> Result<()> {
    let data = tags.to_json().context("Failed to serialize tags")?;
    xattr::set(path, xattr_keys::TAGS, &data).context("Failed to write tags")
}
