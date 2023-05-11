//! KV instance.

use crate::{
    api::{kv_server::Kv, DelArg, DelReply, GetArg, GetReply, IncArg, IncReply, PutArg, PutReply},
    log::{LogCommand, LogOp, Loggable, Logger},
};
use async_trait::async_trait;
use std::{
    cell::SyncUnsafeCell,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock,
    },
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

#[tonic::async_trait]
impl Kv for KVHandle {
    async fn get(&self, req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        let GetArg { key } = req.into_inner();
        let reply = if let Some(value) = self.inner.get(&key) {
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
        self.inner.put(key, value).await;
        let reply = PutReply {};
        Ok(Response::new(reply))
    }

    async fn inc(&self, req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        let IncArg { key, value } = req.into_inner();
        let reply = if let Some(orig) = self.inner.get(&key) {
            self.inner.put(key, orig + value).await;
            IncReply { has_value: true }
        } else {
            IncReply { has_value: false }
        };
        Ok(Response::new(reply))
    }

    async fn del(&self, req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        let DelArg { key } = req.into_inner();
        self.inner.del(key).await;
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

/// Real KV instance.
///
/// It should not expose any functions that requires to be called with a `&mut self`.
#[derive(Debug)]
struct KVInner {
    data: RwLock<HashMap<String, Versions>>, // TODO remove lock !!
    log_tx: mpsc::UnboundedSender<LogCommand>,
}

impl KVInner {
    async fn new(log_tx: mpsc::UnboundedSender<LogCommand>) -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            log_tx,
        }
    }

    fn get(&self, key: &String) -> Option<i64> {
        self.data.read().unwrap().get(key)?.get_latest()
    }

    async fn put(&self, key: String, value: i64) {
        let op = LogOp {
            key: key.clone(),
            value: Some(value),
        };
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .unwrap();
        done.notified().await;
        self.data
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .update(0, Some(value));
    }

    async fn del(&self, key: String) {
        let op = LogOp {
            key: key.clone(),
            value: None,
        };
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .unwrap();
        done.notified().await;
        if let Some(versions) = self.data.read().unwrap().get(&key) {
            versions.update(0, None);
        }
    }
}

#[async_trait]
impl Loggable for KVInner {
    async fn replay(&mut self, op: LogOp) {
        let LogOp { key, value } = op;
        if let Some(value) = value {
            self.put(key, value).await;
        } else {
            self.del(key).await;
        }
    }
}

#[allow(dead_code)]
/// Max number of versions.
const MAX_VERSION_NUM: usize = 0x100;

type SingleVersion = Option<(usize, Option<i64>)>;

/// Multiple versions of a key.
#[derive(Debug)]
#[allow(dead_code)]
struct Versions {
    /// A lock.
    updating: AtomicBool,
    /// A index.
    size: AtomicUsize,
    /// Sorted by version number.
    versions: Vec<SyncUnsafeCell<SingleVersion>>,
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            updating: AtomicBool::new(false),
            size: AtomicUsize::new(0),
            versions: Default::default(),
        }
    }
}

impl Versions {
    // XXX to be deprecated
    fn get_latest(&self) -> Option<i64> {
        todo!()
    }

    #[allow(dead_code)] // TODO use timestamp
    fn get_version(&self, _version: usize) -> Option<i64> {
        todo!()
    }

    fn update(&self, _version: usize, _value: Option<i64>) {
        todo!()
    }
}
