use fs_core::{Dispatch, RpcClient, RpcError, RpcMetadata};
use fs_protocol::{decode_request, decode_response, encode_request, encode_response, pb};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalMode {
    Direct,
    Serialized,
}

pub struct LocalClient {
    dispatcher: Arc<dyn Dispatch>,
    mode: LocalMode,
    max_frame_bytes: usize,
    connected: AtomicBool,
}

impl LocalClient {
    pub fn new(dispatcher: Arc<dyn Dispatch>, mode: LocalMode) -> Self {
        Self {
            dispatcher,
            mode,
            max_frame_bytes: 4 * 1024 * 1024,
            connected: AtomicBool::new(true),
        }
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    pub fn reconnect(&self) {
        self.connected.store(true, Ordering::SeqCst);
    }

    pub fn call_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>, RpcError> {
        self.ensure_connected()?;
        self.check_frame_len(request_bytes.len())?;
        let request = decode_request(request_bytes)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        let metadata = RpcMetadata::for_request(&request, request_bytes.len() as u64);
        let response = self.dispatcher.dispatch_request(request, metadata);
        let response_bytes =
            encode_response(&response).map_err(|error| RpcError::Malformed(error.to_string()))?;
        self.check_frame_len(response_bytes.len())?;
        Ok(response_bytes)
    }

    fn call_direct(
        &self,
        request: fs_protocol::RequestEnvelope,
    ) -> Result<fs_protocol::ResponseEnvelope, RpcError> {
        self.ensure_connected()?;
        let metadata = RpcMetadata::for_request(&request, 0);
        let response = self.dispatcher.dispatch_request(request, metadata);
        Ok(response)
    }

    fn call_serialized(
        &self,
        request: fs_protocol::RequestEnvelope,
    ) -> Result<fs_protocol::ResponseEnvelope, RpcError> {
        let request_bytes =
            encode_request(&request).map_err(|error| RpcError::Malformed(error.to_string()))?;
        let response_bytes = self.call_bytes(&request_bytes)?;
        decode_response(&response_bytes).map_err(|error| RpcError::Malformed(error.to_string()))
    }

    fn ensure_connected(&self) -> Result<(), RpcError> {
        if self.connected.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RpcError::ConnectionClosed)
        }
    }

    fn check_frame_len(&self, frame_len: usize) -> Result<(), RpcError> {
        if frame_len <= self.max_frame_bytes {
            Ok(())
        } else {
            Err(RpcError::FrameTooLarge)
        }
    }
}

impl RpcClient for LocalClient {
    fn call(
        &self,
        request: fs_protocol::RequestEnvelope,
    ) -> Result<fs_protocol::ResponseEnvelope, RpcError> {
        match self.mode {
            LocalMode::Direct => self.call_direct(request),
            LocalMode::Serialized => self.call_serialized(request),
        }
    }

    fn drain_invalidations(&self, _namespace: &str) -> Result<Vec<pb::Invalidation>, RpcError> {
        self.ensure_connected()?;
        Ok(Vec::new())
    }
}
