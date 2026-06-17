use std::sync::{Mutex, MutexGuard};

pub fn lock_or_errno<'a, T>(
    mutex: &'a Mutex<T>,
    label: &'static str,
    repair: impl FnOnce(&mut T),
) -> Result<MutexGuard<'a, T>, i32> {
    match mutex.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            repair(&mut state);
            tracing::error!(component = "storage_state", %label, "poisoned runtime state cleared");
            drop(state);
            mutex.clear_poison();
            Err(libc::EIO)
        }
    }
}
