pub mod bridge;
pub mod convert;

use bridge::ZodaixNfs;
use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
use std::sync::Arc;
use tracing::info;
use zodaix_core::VfsBackend;

/// Default NFS port (unprivileged, no sudo needed).
pub const DEFAULT_NFS_PORT: u16 = 11111;

/// NFSv3 server wrapping a VfsBackend.
pub struct NfsServer {
    port: u16,
    listener: NFSTcpListener<ZodaixNfs>,
}

impl NfsServer {
    /// Bind a TCP listener and prepare the NFS server.
    pub async fn bind(backend: Arc<dyn VfsBackend>, port: u16) -> Result<Self, std::io::Error> {
        let nfs_fs = ZodaixNfs::new(backend);
        let addr = format!("127.0.0.1:{port}");
        info!("Starting NFSv3 server on {addr}");

        let listener = NFSTcpListener::bind(&addr, nfs_fs).await?;

        Ok(Self { port, listener })
    }

    /// Get the port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Run the NFS server forever (accepts connections in a loop).
    pub async fn handle_forever(&self) -> Result<(), std::io::Error> {
        self.listener.handle_forever().await
    }
}
