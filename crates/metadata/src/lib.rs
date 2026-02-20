pub mod index;
pub mod tags;

pub use index::{IndexError, SearchIndex, SearchResult};
pub use tags::{xattr_keys, Tags};

/// Get the default index directory path.
pub fn default_index_dir() -> std::path::PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("zodaix")
        .join("index")
}
