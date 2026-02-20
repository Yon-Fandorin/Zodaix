pub mod error;
pub mod registry;
pub mod types;
pub mod vfs;

pub use error::{VfsError, VfsResult};
pub use registry::{BackendConfig, BackendFactory, BackendRegistry};
pub use types::*;
pub use vfs::{StatFs, VfsBackend};
