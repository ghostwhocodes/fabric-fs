use crate::invalidation::validate_invalidation;
use crate::{
    pb, CallerContext, Errno, Observation, Operation, ProtocolError, RequestPayload,
    ResponsePayload, TraceContext, PROTOCOL_VERSION,
};

const MALFORMED_REQUEST_ID: &str = "malformed-request";
const MALFORMED_NAMESPACE: &str = "malformed-namespace";

#[derive(Debug, Clone, PartialEq)]
pub struct RequestEnvelope {
    pub protocol_version: u32,
    pub request_id: String,
    pub operation: Operation,
    pub namespace: String,
    pub deadline_unix_nanos: u64,
    pub trace: TraceContext,
    pub caller: Option<CallerContext>,
    pub payload: RequestPayload,
    pub observations: Vec<Observation>,
}

impl RequestEnvelope {
    pub fn new(
        request_id: impl Into<String>,
        namespace: impl Into<String>,
        deadline_unix_nanos: u64,
        trace: TraceContext,
        payload: RequestPayload,
    ) -> Result<Self, ProtocolError> {
        let payload_operation = payload.operation();
        payload.validate()?;
        let envelope = Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            operation: payload_operation,
            namespace: namespace.into(),
            deadline_unix_nanos,
            trace,
            caller: None,
            payload,
            observations: Vec::new(),
        };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn with_caller(mut self, caller: CallerContext) -> Self {
        self.caller = Some(caller);
        self
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.validate_identity()?;
        self.payload.validate()
    }

    fn validate_identity(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(self.protocol_version));
        }
        if self.request_id.is_empty() {
            return Err(ProtocolError::InvalidEnvelope(
                "request_id must not be empty".into(),
            ));
        }
        if self.namespace.is_empty() {
            return Err(ProtocolError::InvalidEnvelope(
                "namespace must not be empty".into(),
            ));
        }
        if self.operation != self.payload.operation() {
            return Err(ProtocolError::PayloadMismatch {
                envelope: self.operation,
                payload: self.payload.operation(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseEnvelope {
    pub protocol_version: u32,
    pub request_id: String,
    pub operation: Operation,
    pub namespace: String,
    pub deadline_unix_nanos: u64,
    pub trace: TraceContext,
    pub ok: bool,
    pub errno: Option<Errno>,
    pub error_message: String,
    pub payload: Option<ResponsePayload>,
    pub observations: Vec<Observation>,
    pub invalidations: Vec<pb::Invalidation>,
}

impl ResponseEnvelope {
    pub fn success_for(
        request: &RequestEnvelope,
        payload: ResponsePayload,
        invalidations: Vec<pb::Invalidation>,
    ) -> Result<Self, ProtocolError> {
        request.validate()?;
        if request.operation != payload.operation() {
            return Err(ProtocolError::PayloadMismatch {
                envelope: request.operation,
                payload: payload.operation(),
            });
        }
        payload.validate_for_request(&request.payload)?;
        let envelope = Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            operation: request.operation,
            namespace: request.namespace.clone(),
            deadline_unix_nanos: request.deadline_unix_nanos,
            trace: request.trace.clone(),
            ok: true,
            errno: None,
            error_message: String::new(),
            payload: Some(payload),
            observations: Vec::new(),
            invalidations,
        };
        envelope.validate_for_request(request)?;
        Ok(envelope)
    }

    pub fn failure_for(
        request: &RequestEnvelope,
        errno: Errno,
        message: impl Into<String>,
    ) -> Self {
        let errno = if errno == Errno::Success {
            Errno::InvalidArgument
        } else {
            errno
        };
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id: response_request_id(request),
            operation: request.operation,
            namespace: response_namespace(request),
            deadline_unix_nanos: request.deadline_unix_nanos,
            trace: request.trace.clone(),
            ok: false,
            errno: Some(errno),
            error_message: message.into(),
            payload: None,
            observations: Vec::new(),
            invalidations: Vec::new(),
        }
    }

    fn validate_identity_and_payload(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(self.protocol_version));
        }
        if self.request_id.is_empty() || self.namespace.is_empty() {
            return Err(ProtocolError::InvalidEnvelope(
                "request_id and namespace must not be empty".into(),
            ));
        }
        if self.ok {
            if self.errno.is_some() {
                return Err(ProtocolError::InvalidResponseState(
                    "successful response must not carry errno".into(),
                ));
            }
            let payload = self.payload.as_ref().ok_or_else(|| {
                ProtocolError::InvalidResponseState("missing success payload".into())
            })?;
            if payload.operation() != self.operation {
                return Err(ProtocolError::PayloadMismatch {
                    envelope: self.operation,
                    payload: payload.operation(),
                });
            }
            payload.validate()?;
        } else {
            if self.payload.is_some() {
                return Err(ProtocolError::InvalidResponseState(
                    "error response must not carry payload".into(),
                ));
            }
            self.errno
                .filter(|errno| errno.is_error())
                .ok_or_else(|| ProtocolError::InvalidResponseState("missing error errno".into()))?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.validate_identity_and_payload()?;
        for invalidation in &self.invalidations {
            validate_invalidation(invalidation)?;
        }
        Ok(())
    }

    pub fn validate_identity_and_payload_for_request(
        &self,
        request: &RequestEnvelope,
    ) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate_identity_and_payload()?;
        if self.request_id != request.request_id {
            return Err(ProtocolError::InvalidEnvelope(
                "response request_id does not match request".into(),
            ));
        }
        if self.namespace != request.namespace {
            return Err(ProtocolError::InvalidEnvelope(
                "response namespace does not match request".into(),
            ));
        }
        if self.operation != request.operation {
            return Err(ProtocolError::InvalidEnvelope(
                "response operation does not match request".into(),
            ));
        }
        if let Some(payload) = &self.payload {
            payload.validate_for_request(&request.payload)?;
        }
        Ok(())
    }

    pub fn validate_for_request(&self, request: &RequestEnvelope) -> Result<(), ProtocolError> {
        self.validate_identity_and_payload_for_request(request)?;
        for invalidation in &self.invalidations {
            validate_invalidation(invalidation)?;
        }
        Ok(())
    }
}

fn response_request_id(request: &RequestEnvelope) -> String {
    if request.request_id.is_empty() {
        MALFORMED_REQUEST_ID.into()
    } else {
        request.request_id.clone()
    }
}

fn response_namespace(request: &RequestEnvelope) -> String {
    if request.namespace.is_empty() {
        MALFORMED_NAMESPACE.into()
    } else {
        request.namespace.clone()
    }
}
