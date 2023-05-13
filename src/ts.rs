use std::sync::atomic::{AtomicUsize, Ordering};

use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref TS_MANAGER: TsManager = TsManager::default();
}

#[derive(Debug, Default)]
pub(crate) struct TsManager {
    ts: AtomicUsize,
}

impl TsManager {
    pub(crate) fn new_ts(&self) -> usize {
        self.ts.fetch_add(1, Ordering::SeqCst)
    }
}
