//! KV instance.

use crate::{
    api::{
        kv_server::Kv, CommitArg, CommitReply, DelArg, DelReply, GetArg, GetReply, IncArg,
        IncReply, PutArg, PutReply, ReadArg, ReadReply,
    },
    log::{LogCommand, LogOp, Loggable, Logger},
    ts::TS_MANAGER,
};
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
use tonic::{Request, Response, Status, Streaming};

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
        let reply = if let Some(value) = self.inner.get(key, usize::MAX) {
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

    async fn read_txn(&self, _req: Request<ReadArg>) -> Result<Response<ReadReply>, tonic::Status> {
        todo!()
    }

    async fn commit_txn(
        &self,
        _req: Request<Streaming<CommitArg>>,
    ) -> Result<Response<CommitReply>, tonic::Status> {
        todo!()
    }
}

impl KVHandle {
    /// Get a new instance of `KVHandle`.
    pub async fn new(mount_path: PathBuf, log_path: PathBuf) -> Self {
        // Get a Logger first.
        let (logger, tx) = Logger::new(&log_path).await;
        // Try to restore from snapshot.
        let kv = if let Some(kv) = Self::restore(&mount_path, &tx).await {
            kv
        } else {
            KVInner::new(tx)
        };

        // Replay log.
        logger.restore(&kv);
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
        // TODO(snapshot) restore
        None
    }

    /// Dump snapshot.
    async fn persist(&self, _path: &Path) {
        // TODO(snapshot) persist
    }
}

type KeyVersion = (String, usize);

/// Real KV instance.
#[derive(Debug)]
struct KVInner {
    data: ConcurrentMap<KeyVersion, Option<i64>>, // TODO(gc)
    log_tx: mpsc::UnboundedSender<LogCommand>,
}

impl KVInner {
    const LOCK: usize = usize::MAX;

    fn new(log_tx: mpsc::UnboundedSender<LogCommand>) -> Self {
        Self {
            data: ConcurrentMap::default(),
            log_tx,
        }
    }

    fn get(&self, key: String, ts: usize) -> Option<i64> {
        log::trace!("get key={key} ts={ts}");
        self.data.get_lt(&(key, ts))?.1
    }

    #[allow(dead_code)] // TODO(txn)
    fn lock(&self, key: &String, txn_id: i64) {
        while self
            .data
            .cas((key.to_owned(), Self::LOCK), None, Some(Some(txn_id)))
            .is_err()
        {}
    }

    #[allow(dead_code)] // TODO(txn)
    fn unlock(&self, key: &String, txn_id: i64) {
        self.data
            .cas((key.to_owned(), Self::LOCK), Some(&Some(txn_id)), None)
            .ok();
    }

    async fn put(&self, key: String, value: i64, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value: Some(value),
            ts,
        };
        log::trace!("put key={key} value={value} ts={ts}");
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .expect("channel closed");
        done.notified().await;
        self.data.insert((key, ts), Some(value));
    }

    async fn del(&self, key: String, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value: None,
            ts,
        };
        log::trace!("del key={key} ts={ts}");
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .expect("channel closed");
        done.notified().await;
        self.data.insert((key, ts), None);
    }
}

impl Loggable for KVInner {
    fn replay(&self, op: LogOp) {
        let LogOp { key, value, ts } = op;
        self.data.insert((key, ts), value);
    }
}
