use anyhow::{Context, Result};
use notify::{
    event::{AccessKind, AccessMode},
    Event, EventKind,
};

use super::admission::StorageInvalidationGate;
use super::self_notify::SelfNotifyEventClass;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WatchEventClassification {
    Ignored,
    SuppressibleFullResync(SelfNotifyEventClass),
    UnsuppressibleFullResync,
}

pub(super) fn handle_notify_result(
    gate: &StorageInvalidationGate,
    result: notify::Result<Event>,
) -> Result<bool> {
    let event = result.context("read storage watch event")?;
    handle_storage_event(gate, event)
}

pub(super) fn handle_storage_event(gate: &StorageInvalidationGate, event: Event) -> Result<bool> {
    match classify_watch_event(&event.kind) {
        WatchEventClassification::Ignored => Ok(false),
        WatchEventClassification::SuppressibleFullResync(event_class) => {
            Ok(gate.record_classified_watch_event(event_class, &event.paths))
        }
        WatchEventClassification::UnsuppressibleFullResync => {
            Ok(gate.record_unsuppressible_watch_event())
        }
    }
}

pub(super) fn classify_watch_event(kind: &EventKind) -> WatchEventClassification {
    match kind {
        EventKind::Any => {
            WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::Any)
        }
        EventKind::Create(_) => {
            WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::Create)
        }
        EventKind::Modify(_) => {
            WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::Modify)
        }
        EventKind::Remove(_) => {
            WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::Remove)
        }
        EventKind::Access(AccessKind::Any) => {
            WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::Any)
        }
        EventKind::Access(AccessKind::Close(
            AccessMode::Any | AccessMode::Write | AccessMode::Other,
        )) => WatchEventClassification::SuppressibleFullResync(SelfNotifyEventClass::CloseWrite),
        EventKind::Access(_) => WatchEventClassification::Ignored,
        EventKind::Other => WatchEventClassification::UnsuppressibleFullResync,
    }
}

pub(super) fn self_notify_event_class(kind: &EventKind) -> Option<SelfNotifyEventClass> {
    match classify_watch_event(kind) {
        WatchEventClassification::SuppressibleFullResync(event_class) => Some(event_class),
        WatchEventClassification::Ignored | WatchEventClassification::UnsuppressibleFullResync => {
            None
        }
    }
}
