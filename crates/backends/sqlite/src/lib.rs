pub mod backend;
pub mod schema;

pub use backend::SqliteBackend;

use zodaix_core::{BackendConfig, BackendRegistry, VfsResult};

#[cfg(test)]
mod tests;

/// Register the SQLite backend with the given registry.
pub fn register(registry: &mut BackendRegistry) {
    registry.register("sqlite", |config: BackendConfig| -> VfsResult<Box<dyn zodaix_core::VfsBackend>> {
        let db_path = config
            .params
            .get("db")
            .cloned()
            .unwrap_or_else(default_db_path);
        Ok(Box::new(SqliteBackend::open(&db_path)?))
    });
}

/// Default database path: ~/.local/share/zodaix/default.db
fn default_db_path() -> String {
    let data_dir = dirs_fallback();
    format!("{}/default.db", data_dir)
}

fn dirs_fallback() -> String {
    if let Some(dir) = dirs_data_dir() {
        dir
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        format!("{home}/.local/share/zodaix")
    }
}

fn dirs_data_dir() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var("HOME").ok()?;
        Some(format!("{home}/.local/share/zodaix"))
    }
    #[cfg(target_os = "linux")]
    {
        std::env::var("XDG_DATA_HOME")
            .ok()
            .map(|d| format!("{d}/zodaix"))
            .or_else(|| {
                let home = std::env::var("HOME").ok()?;
                Some(format!("{home}/.local/share/zodaix"))
            })
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}
