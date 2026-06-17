use fs_protocol::Operation;

const SUBJECT_PREFIX: &str = "fabricfs.v1";
const INVALIDATION_PREFIX: &str = "fabricfs.invalidate.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommandSubject {
    pub mount: String,
    pub operation: Operation,
}

fn encode_mount_token(mount: &str) -> String {
    hex::encode(mount.as_bytes())
}

fn decode_mount_token(token: &str) -> Option<String> {
    if token.is_empty() {
        return None;
    }
    let bytes = hex::decode(token).ok()?;
    let mount = String::from_utf8(bytes).ok()?;
    if mount.is_empty() {
        None
    } else {
        Some(mount)
    }
}

pub fn command_subject(mount: &str, command: &str) -> String {
    format!("{SUBJECT_PREFIX}.{}.{}", encode_mount_token(mount), command)
}

pub fn command_subject_for_operation(mount: &str, operation: Operation) -> String {
    command_subject(mount, operation.as_str())
}

pub fn query_subject(mount: &str, query: &str) -> String {
    format!(
        "{SUBJECT_PREFIX}.{}.query.{}",
        encode_mount_token(mount),
        query
    )
}

pub fn subscription_subject(mount: &str) -> String {
    format!("{SUBJECT_PREFIX}.{}.>", encode_mount_token(mount))
}

pub fn invalidation_subject(mount: &str) -> String {
    format!("{}.{}", INVALIDATION_PREFIX, encode_mount_token(mount))
}

pub(crate) fn command_subject_parts(subject: &str) -> Option<CommandSubject> {
    let rest = subject.strip_prefix(SUBJECT_PREFIX)?.strip_prefix('.')?;
    let mut parts = rest.split('.');
    let mount_token = parts.next()?;
    let operation_token = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let mount = decode_mount_token(mount_token)?;
    let operation = Operation::from_subject_token(operation_token)?;
    Some(CommandSubject { mount, operation })
}

pub fn command_operation_from_subject(subject: &str) -> Option<Operation> {
    command_subject_parts(subject).map(|subject| subject.operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_embed_hex_mount() {
        let mount = "/tmp/data";
        let token = encode_mount_token(mount);
        assert_eq!(token, "2f746d702f64617461");
        assert_eq!(
            command_subject(mount, "open"),
            "fabricfs.v1.2f746d702f64617461.open"
        );
        assert_eq!(
            query_subject(mount, "stat"),
            "fabricfs.v1.2f746d702f64617461.query.stat"
        );
        assert_eq!(
            subscription_subject(mount),
            "fabricfs.v1.2f746d702f64617461.>"
        );
        assert_eq!(
            invalidation_subject(mount),
            "fabricfs.invalidate.v1.2f746d702f64617461"
        );
    }

    #[test]
    fn subscription_subject_scopes_to_mount() {
        let mount = "demo-mount";
        let token = hex::encode(mount.as_bytes());
        let subject = subscription_subject(mount);
        assert_eq!(subject, format!("fabricfs.v1.{token}.>"));
        assert!(
            !subject.contains("*.>"),
            "subscription must be mount-scoped, not wildcard"
        );
    }

    #[test]
    fn parses_only_exact_command_operation_subjects() {
        let mount = "demo-mount";
        assert_eq!(
            command_operation_from_subject(&command_subject_for_operation(mount, Operation::Write)),
            Some(Operation::Write)
        );
        assert_eq!(
            command_operation_from_subject(&query_subject(mount, "stat")),
            None
        );
        assert_eq!(
            command_operation_from_subject(&format!("{}.extra", command_subject(mount, "write"))),
            None
        );
        assert_eq!(
            command_operation_from_subject("fabricfs.v1..write"),
            None,
            "empty mount tokens are never valid command subjects"
        );
    }

    #[test]
    fn parses_command_subject_mount_and_operation() {
        let parts = command_subject_parts(&command_subject_for_operation(
            "/tmp/data",
            Operation::Write,
        ))
        .expect("command subject parses");

        assert_eq!(parts.mount, "/tmp/data");
        assert_eq!(parts.operation, Operation::Write);
        assert!(command_subject_parts("fabricfs.v1.not-hex.write").is_none());
    }

    #[test]
    fn parses_every_supported_command_operation_subject() {
        for operation in Operation::ALL {
            let subject = command_subject_for_operation("demo-mount", operation);
            let parts = command_subject_parts(&subject)
                .unwrap_or_else(|| panic!("subject for {operation:?} should parse"));
            assert_eq!(parts.mount, "demo-mount");
            assert_eq!(parts.operation, operation);
        }
    }
}
