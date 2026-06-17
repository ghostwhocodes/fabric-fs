use fabricfs_transport::is_verified_nats_peer_identity;
use fs_core::{Authorizer, FsError, FsResult, RpcMetadata};
use fs_protocol::{Errno, RequestEnvelope};

#[derive(Debug, Clone)]
pub struct FabricFsAuthorizer {
    namespace: String,
}

impl FabricFsAuthorizer {
    pub fn for_namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

impl Authorizer for FabricFsAuthorizer {
    fn authorize(&self, metadata: &RpcMetadata, request: &RequestEnvelope) -> FsResult<()> {
        if metadata.namespace != self.namespace || request.namespace != self.namespace {
            return Err(FsError::new(
                Errno::PermissionDenied,
                format!(
                    "request namespace {} is not allowed for mount {}",
                    request.namespace, self.namespace
                ),
            ));
        }

        match metadata.peer_identity.as_deref() {
            Some(peer_identity) if is_verified_nats_peer_identity(peer_identity) => {}
            Some(peer_identity) => {
                return Err(FsError::new(
                    Errno::PermissionDenied,
                    format!("peer identity {peer_identity} is not authorized"),
                ));
            }
            None => {
                return Err(FsError::new(
                    Errno::PermissionDenied,
                    "peer identity is required",
                ));
            }
        }

        if metadata.caller.is_none() {
            return Err(FsError::new(
                Errno::PermissionDenied,
                format!(
                    "caller context is required for {}",
                    request.operation.as_str()
                ),
            ));
        }

        tracing::trace!(
            component = "authorizer",
            namespace = %request.namespace,
            operation = request.operation.as_str(),
            peer_identity = ?metadata.peer_identity,
            caller = ?metadata.caller,
            "authorized filesystem request"
        );

        Ok(())
    }
}
