//! Logging module

use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Mutex, Notify},
    task,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogOp {
    pub(crate) key: String,
    pub(crate) value: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct LogCommand {
    pub(crate) op: LogOp,
    pub(crate) done: Arc<Notify>,
}

#[derive(Debug)]
pub(crate) struct Logger {
    log_file: Mutex<File>, // XXX serialized
}

#[async_trait]
pub(crate) trait Loggable {
    async fn replay(&mut self, op: LogOp);
}

impl Logger {
    pub(crate) async fn new(log_path: &Path) -> (Arc<Self>, mpsc::UnboundedSender<LogCommand>) {
        let log_file = File::open(log_path).await.unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let this = Arc::new(Self {
            log_file: Mutex::new(log_file),
        });
        let that = Arc::clone(&this);
        task::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                let LogCommand { op, done } = cmd;
                that.log(&op).await;
                done.notify_one();
            }
        });
        (this, tx)
    }

    pub(crate) async fn restore(self: &Arc<Self>, state: &mut impl Loggable) {
        let mut f = self.log_file.lock().await;
        while let Ok(sz) = f.read_u64().await {
            let mut buffer = vec![0u8; sz as usize];
            f.read_exact(&mut buffer[..]).await.unwrap();
            let op: LogOp = bincode::deserialize(&buffer[..]).unwrap();
            state.replay(op).await;
        }
    }

    async fn log(self: &Arc<Self>, op: &LogOp) {
        let data = bincode::serialize(op).unwrap();
        let mut f = self.log_file.lock().await;
        f.write_u64(data.len() as _).await.unwrap();
        f.write_all(&data).await.unwrap();
    }
}
