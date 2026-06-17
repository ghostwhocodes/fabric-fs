use prost::Message;
use std::convert::TryFrom;

use crate::invalidation::validate_invalidation;
use crate::{
    pb, Errno, Operation, ProtocolError, RequestEnvelope, RequestPayload, ResponseEnvelope,
    ResponsePayload, PROTOCOL_VERSION,
};

pub fn encode_request(envelope: &RequestEnvelope) -> Result<Vec<u8>, ProtocolError> {
    envelope.validate()?;
    let payload = envelope.payload.encode_payload()?;
    let raw = pb::RequestEnvelope {
        protocol_version: envelope.protocol_version,
        request_id: envelope.request_id.clone(),
        operation: envelope.operation.wire_value(),
        namespace: envelope.namespace.clone(),
        deadline_unix_nanos: envelope.deadline_unix_nanos,
        trace: Some(envelope.trace.clone()),
        payload,
        payload_operation: envelope.payload.operation().wire_value(),
        observations: envelope.observations.clone(),
        caller: envelope.caller.clone(),
    };
    encode_message(&raw)
}

pub fn decode_request(bytes: &[u8]) -> Result<RequestEnvelope, ProtocolError> {
    let raw: pb::RequestEnvelope = decode_message(bytes)?;
    if raw.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(raw.protocol_version));
    }
    if raw.request_id.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(
            "request_id must not be empty".into(),
        ));
    }
    if raw.namespace.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(
            "namespace must not be empty".into(),
        ));
    }
    let operation = Operation::try_from(raw.operation)?;
    let payload_operation = Operation::try_from(raw.payload_operation)?;
    if operation != payload_operation {
        return Err(ProtocolError::PayloadMismatch {
            envelope: operation,
            payload: payload_operation,
        });
    }
    let payload = RequestPayload::decode(operation, &raw.payload)?;
    Ok(RequestEnvelope {
        protocol_version: raw.protocol_version,
        request_id: raw.request_id,
        operation,
        namespace: raw.namespace,
        deadline_unix_nanos: raw.deadline_unix_nanos,
        trace: raw.trace.unwrap_or_default(),
        caller: raw.caller,
        payload,
        observations: raw.observations,
    })
}

pub fn encode_response(envelope: &ResponseEnvelope) -> Result<Vec<u8>, ProtocolError> {
    envelope.validate()?;
    let (payload, payload_operation, errno) = if envelope.ok {
        let payload = envelope
            .payload
            .as_ref()
            .ok_or_else(|| ProtocolError::InvalidResponseState("missing success payload".into()))?;
        (
            payload.encode_payload()?,
            payload.operation(),
            Errno::Success,
        )
    } else {
        let errno = envelope
            .errno
            .filter(|errno| errno.is_error())
            .ok_or_else(|| ProtocolError::InvalidResponseState("missing error errno".into()))?;
        (Vec::new(), envelope.operation, errno)
    };
    let raw = pb::ResponseEnvelope {
        protocol_version: envelope.protocol_version,
        request_id: envelope.request_id.clone(),
        operation: envelope.operation.wire_value(),
        namespace: envelope.namespace.clone(),
        deadline_unix_nanos: envelope.deadline_unix_nanos,
        trace: Some(envelope.trace.clone()),
        payload,
        ok: envelope.ok,
        errno: errno.wire_value(),
        error_message: envelope.error_message.clone(),
        observations: envelope.observations.clone(),
        invalidations: envelope.invalidations.clone(),
        payload_operation: payload_operation.wire_value(),
    };
    encode_message(&raw)
}

pub fn decode_response(bytes: &[u8]) -> Result<ResponseEnvelope, ProtocolError> {
    let raw: pb::ResponseEnvelope = decode_message(bytes)?;
    if raw.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(raw.protocol_version));
    }
    if raw.request_id.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(
            "request_id must not be empty".into(),
        ));
    }
    if raw.namespace.is_empty() {
        return Err(ProtocolError::InvalidEnvelope(
            "namespace must not be empty".into(),
        ));
    }
    let operation = Operation::try_from(raw.operation)?;
    let payload_operation = Operation::try_from(raw.payload_operation)?;
    if operation != payload_operation {
        return Err(ProtocolError::PayloadMismatch {
            envelope: operation,
            payload: payload_operation,
        });
    }
    let errno = Errno::try_from(raw.errno)?;
    for invalidation in &raw.invalidations {
        validate_invalidation(invalidation)?;
    }
    if raw.ok {
        if errno != Errno::Success {
            return Err(ProtocolError::InvalidResponseState(
                "successful response carried errno".into(),
            ));
        }
        let payload = ResponsePayload::decode(operation, &raw.payload)?;
        Ok(ResponseEnvelope {
            protocol_version: raw.protocol_version,
            request_id: raw.request_id,
            operation,
            namespace: raw.namespace,
            deadline_unix_nanos: raw.deadline_unix_nanos,
            trace: raw.trace.unwrap_or_default(),
            ok: true,
            errno: None,
            error_message: raw.error_message,
            payload: Some(payload),
            observations: raw.observations,
            invalidations: raw.invalidations,
        })
    } else {
        if errno == Errno::Success {
            return Err(ProtocolError::InvalidResponseState(
                "error response missing errno".into(),
            ));
        }
        if !raw.payload.is_empty() {
            return Err(ProtocolError::InvalidResponseState(
                "error response carried payload".into(),
            ));
        }
        Ok(ResponseEnvelope {
            protocol_version: raw.protocol_version,
            request_id: raw.request_id,
            operation,
            namespace: raw.namespace,
            deadline_unix_nanos: raw.deadline_unix_nanos,
            trace: raw.trace.unwrap_or_default(),
            ok: false,
            errno: Some(errno),
            error_message: raw.error_message,
            payload: None,
            observations: raw.observations,
            invalidations: raw.invalidations,
        })
    }
}

pub fn encode_message<M: Message>(message: &M) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut bytes)
        .map_err(|error| ProtocolError::Encode(error.to_string()))?;
    Ok(bytes)
}

pub fn decode_message<M: Message + Default>(bytes: &[u8]) -> Result<M, ProtocolError> {
    M::decode(bytes).map_err(|error| ProtocolError::Decode(error.to_string()))
}
