use std::collections::HashMap;

use fabricfs_session_protocol::session_proto as pb;
use uuid::Uuid;

const IMPORT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x9b, 0x08, 0x6a, 0x5a, 0x3d, 0x5f, 0x4c, 0x9d, 0x9a, 0x46, 0x58, 0xf8, 0x4c, 0x84, 0x95, 0x44,
]);

pub(super) fn imported_session_id(remote_checkpoint_id: &str) -> String {
    Uuid::new_v5(&IMPORT_NAMESPACE, remote_checkpoint_id.as_bytes()).to_string()
}

pub(super) fn snapshots_equivalent(a: &pb::SessionSnapshot, b: &pb::SessionSnapshot) -> bool {
    let Some(am) = a.metadata.as_ref() else {
        return false;
    };
    let Some(bm) = b.metadata.as_ref() else {
        return false;
    };
    if am.cow_root != bm.cow_root || am.overlay_version != bm.overlay_version {
        return false;
    }

    let a_entries = overlay_map(&a.entries);
    let b_entries = overlay_map(&b.entries);
    if a_entries.len() != b_entries.len() {
        return false;
    }

    for (path, entry) in a_entries {
        match b_entries.get(&path) {
            Some(other) if overlay_entries_equal(&entry, other) => continue,
            _ => return false,
        }
    }

    true
}

fn overlay_map(entries: &[pb::OverlayEntry]) -> HashMap<String, pb::OverlayEntry> {
    let mut map = HashMap::new();
    for entry in entries {
        map.insert(entry.logical_path.clone(), entry.clone());
    }
    map
}

fn overlay_entries_equal(a: &pb::OverlayEntry, b: &pb::OverlayEntry) -> bool {
    if a.logical_path != b.logical_path {
        return false;
    }
    match (&a.kind, &b.kind) {
        (Some(pb::overlay_entry::Kind::Alias(ax)), Some(pb::overlay_entry::Kind::Alias(bx))) => {
            ax.target_path == bx.target_path
        }
        (
            Some(pb::overlay_entry::Kind::Tombstone(_)),
            Some(pb::overlay_entry::Kind::Tombstone(_)),
        ) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_alias(logical: &str, target: &str) -> pb::OverlayEntry {
        pb::OverlayEntry {
            logical_path: logical.into(),
            kind: Some(pb::overlay_entry::Kind::Alias(pb::Alias {
                logical_path: logical.into(),
                target_path: target.into(),
                created_at_unix_nanos: 0,
                origin: None,
            })),
        }
    }

    #[test]
    fn import_session_ids_are_deterministic() {
        assert_eq!(
            imported_session_id("remote-1"),
            imported_session_id("remote-1")
        );
        assert_ne!(
            imported_session_id("remote-1"),
            imported_session_id("remote-2")
        );
    }

    #[test]
    fn detects_equivalent_snapshots() {
        let a = pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: "a".into(),
                display_name: "A".into(),
                workspace_name: String::new(),
                cow_root: "/tmp".into(),
                password: None,
                created_at_unix_nanos: 0,
                updated_at_unix_nanos: 0,
                overlay_version: 1,
            }),
            entries: vec![make_alias("/x", "/y")],
            overlay_version: 1,
        };

        let b = pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: "b".into(),
                display_name: "B".into(),
                workspace_name: String::new(),
                cow_root: "/tmp".into(),
                password: None,
                created_at_unix_nanos: 0,
                updated_at_unix_nanos: 0,
                overlay_version: 1,
            }),
            entries: vec![make_alias("/x", "/y")],
            overlay_version: 1,
        };

        assert!(snapshots_equivalent(&a, &b));
    }

    #[test]
    fn detects_snapshot_difference() {
        let a = pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: "a".into(),
                display_name: "A".into(),
                workspace_name: String::new(),
                cow_root: "/tmp".into(),
                password: None,
                created_at_unix_nanos: 0,
                updated_at_unix_nanos: 0,
                overlay_version: 1,
            }),
            entries: vec![make_alias("/x", "/y")],
            overlay_version: 1,
        };

        let b = pb::SessionSnapshot {
            metadata: Some(pb::SessionMetadata {
                session_id: "b".into(),
                display_name: "B".into(),
                workspace_name: String::new(),
                cow_root: "/tmp".into(),
                password: None,
                created_at_unix_nanos: 0,
                updated_at_unix_nanos: 0,
                overlay_version: 2,
            }),
            entries: vec![make_alias("/x", "/z")],
            overlay_version: 2,
        };

        assert!(!snapshots_equivalent(&a, &b));
    }
}
