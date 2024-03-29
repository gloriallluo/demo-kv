//! Logging module

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::ts::TS_MANAGER;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogOp {
    pub(crate) key: String,
    pub(crate) value: Option<i64>,
    pub(crate) ts: usize,
}

#[derive(Debug)]
pub(crate) struct Logger {
    log_file: Mutex<File>,
}

pub(crate) trait Loggable {
    fn replay(&self, op: LogOp);
}

impl Logger {
    pub(crate) async fn new(log_path: &Path) -> Arc<Self> {
        let mut file = if let Ok(file) = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(log_path)
        {
            log::info!("open log file {log_path:?}");
            file
        } else {
            log::info!("crate log file {log_path:?}");
            File::create(log_path).expect("failed to create log file")
        };
        file.seek(SeekFrom::Start(0)).expect("failed to seek");
        Arc::new(Self {
            log_file: Mutex::new(file),
        })
    }

    pub(crate) fn restore(self: &Arc<Self>, state: &impl Loggable) {
        let mut f = self.log_file.lock().expect("log file lock poisoned");
        f.seek(SeekFrom::Start(0)).expect("failed to seek");
        let mut u32_arr: [u8; 4] = [0; 4];
        let mut max_ts = 0;
        while f.read_exact(&mut u32_arr[..]).is_ok() {
            let sz = u32::from_ne_bytes(u32_arr);
            let mut buffer = vec![0u8; sz as usize];
            (*f).read_exact(&mut buffer[..]).expect("failed to read");
            let op: LogOp = bincode::deserialize(&buffer[..]).expect("failed to deserialize");
            if op.ts > max_ts {
                max_ts = op.ts;
            }
            log::debug!("log replay new operation {op:?}");
            state.replay(op);
        }
        TS_MANAGER.set_ts(max_ts + 1);
        log::info!("log replay finish");
    }

    pub(crate) fn log(self: &Arc<Self>, op: &LogOp) {
        let data = bincode::serialize(op).expect("failed to serialize");
        let u32_arr: [u8; 4] = (data.len() as u32).to_ne_bytes();
        let mut f = self.log_file.lock().expect("log file lock poisoned");
        f.write_all(&u32_arr[..]).expect("failed to write");
        f.write_all(&data).expect("failed to write");
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        if let Ok(file) = self.log_file.lock() {
            file.sync_data().expect("failed to sync")
        }
    }
}
