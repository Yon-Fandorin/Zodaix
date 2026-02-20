use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// CLI configuration loaded from `~/.config/zodaix/config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Default backend type.
    pub default_backend: Option<String>,
    /// Default mount options.
    pub mount: Option<MountConfig>,
    /// Index configuration.
    pub index: Option<IndexConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    /// Run in foreground by default.
    pub foreground: Option<bool>,
    /// Default mount point.
    pub mountpoint: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Directory for the search index.
    pub dir: Option<PathBuf>,
}

impl Config {
    /// Load configuration from the default path, or return defaults.
    pub fn load() -> Self {
        let config_path = Self::config_path();
        if config_path.exists() {
            match std::fs::read_to_string(&config_path) {
                Ok(content) => match toml::from_str(&content) {
                    Ok(config) => return config,
                    Err(e) => {
                        tracing::warn!("Failed to parse config file: {e}");
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to read config file: {e}");
                }
            }
        }
        Self::default()
    }

    /// Default config file path.
    pub fn config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("zodaix")
            .join("config.toml")
    }
}
