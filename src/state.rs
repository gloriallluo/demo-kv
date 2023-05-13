//! KV instance.

use crate::{
    api::{kv_server::Kv, DelArg, DelReply, GetArg, GetReply, IncArg, IncReply, PutArg, PutReply},
    log::{LogCommand, LogOp, Loggable, Logger},
    ts::TS_MANAGER,
};
use async_trait::async_trait;
use concurrent_map::ConcurrentMap;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task, time,
};
use tonic::{Request, Response, Status};

/// KV Handle. It's a reference to `KVInner`.
#[derive(Debug, Clone)]
pub struct KVHandle {
    inner: Arc<KVInner>,
    _logger: Arc<Logger>,
}

unsafe impl Sync for KVHandle {}

#[tonic::async_trait]
impl Kv for KVHandle {
    async fn get(&self, req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        let GetArg { key } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        let reply = if let Some(value) = self.inner.get(key, ts) {
            GetReply {
                has_value: true,
                value,
            }
        } else {
            GetReply {
                has_value: false,
                value: 0,
            }
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, req: Request<PutArg>) -> Result<Response<PutReply>, Status> {
        let PutArg { key, value } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        self.inner.put(key, value, ts).await;
        let reply = PutReply {};
        Ok(Response::new(reply))
    }

    async fn inc(&self, req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        let IncArg { key, value } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        let reply = if let Some(orig) = self.inner.get(key.clone(), ts) {
            self.inner.put(key, orig + value, ts).await;
            IncReply { has_value: true }
        } else {
            IncReply { has_value: false }
        };
        Ok(Response::new(reply))
    }

    async fn del(&self, req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        let DelArg { key } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        self.inner.del(key, ts).await;
        let reply = DelReply {};
        Ok(Response::new(reply))
    }
}

impl KVHandle {
    /// Get a new instance of `KVHandle`.
    pub async fn new(mount_path: PathBuf, log_path: PathBuf) -> Self {
        // Get a Logger first.
        let (logger, tx) = Logger::new(&log_path).await;
        // Try to restore from snapshot.
        let mut kv = if let Some(kv) = Self::restore(&mount_path, &tx).await {
            kv
        } else {
            KVInner::new(tx).await
        };

        // Replay log.
        logger.restore(&mut kv).await;
        let this = Self {
            inner: Arc::new(kv),
            _logger: logger,
        };

        // Spawn a task that does persisting.
        let that = this.clone();
        task::spawn(async move {
            let path = mount_path;
            loop {
                time::sleep(Duration::from_millis(100)).await;
                that.persist(&path).await;
            }
        });
        this
    }

    /// Restore KV state from snapshot file. None if no snapshot available.
    async fn restore(_path: &Path, _tx: &mpsc::UnboundedSender<LogCommand>) -> Option<KVInner> {
        // TODO snapshot restore
        None
    }

    /// Dump snapshot.
    async fn persist(&self, _path: &Path) {
        // TODO snapshot
    }
}

type KeyVersion = (String, usize);

/// Real KV instance.
///
/// It should not expose any functions that requires to be called with a `&mut self`.
#[derive(Debug)]
struct KVInner {
    // data: RwLock<HashMap<String, Versions>>,
    data: ConcurrentMap<KeyVersion, Option<i64>>,
    log_tx: mpsc::UnboundedSender<LogCommand>,
}

impl KVInner {
    async fn new(log_tx: mpsc::UnboundedSender<LogCommand>) -> Self {
        Self {
            data: ConcurrentMap::default(),
            log_tx,
        }
    }

    fn get(&self, key: String, ts: usize) -> Option<i64> {
        self.data.get_lt(&(key, ts)).map(|v| v.1)?
    }

    async fn put(&self, key: String, value: i64, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value: Some(value),
            ts,
        };
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .unwrap();
        done.notified().await;
        self.data.insert((key, ts), Some(value));
    }

    async fn del(&self, key: String, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value: None,
            ts,
        };
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .unwrap();
        done.notified().await;
        self.data.insert((key, ts), None);
    }
}

#[async_trait]
impl Loggable for KVInner {
    async fn replay(&mut self, op: LogOp) {
        let LogOp { key, value, ts } = op;
        if let Some(value) = value {
            self.put(key, value, ts).await;
        } else {
            self.del(key, ts).await;
        }
    }
}
