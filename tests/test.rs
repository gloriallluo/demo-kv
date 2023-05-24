//! Some basic kv ops.

use demo_kv::{
    api::kv_server::KvServer,
    cli::{Client, KvOutput},
    KVHandle,
};
use madsim::{
    task::{self, JoinHandle},
    time,
};
use std::{error::Error as StdError, time::Duration};
use tempfile::{tempdir, TempDir};
use tonic::transport::Server;

// single server
struct TestServer {
    _dir: TempDir,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn init() -> Result<Self, Box<dyn StdError>> {
        let addr = "0.0.0.0:33333".parse()?;
        let dir = tempdir().unwrap();
        let dir1 = dir.path().to_path_buf();
        let task = task::spawn(async move {
            let handle = KVHandle::new(&dir1.join("test.dkv.db"), &dir1.join("test.dkv.log")).await;
            Server::builder()
                .add_service(KvServer::new(handle))
                .serve(addr)
                .await
                .unwrap();
        });
        Ok(Self { _dir: dir, task })
    }

    #[allow(dead_code)]
    fn shutdown(&self) {
        self.task.abort();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[madsim::test]
async fn basic() {
    let _server = TestServer::init().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    let mut cli = Client::new("http://127.0.0.1:33333").await.unwrap();
    _ = cli.handle("PUT A 3").await.unwrap();
    _ = cli.handle("PUT B 4").await.unwrap();
    _ = cli.handle("PUT A (A+1)").await.unwrap();
    _ = cli.handle("PUT B (B+1)").await.unwrap();
    let res = cli.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(4)));
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)));
    _ = cli.handle("DEL A").await.unwrap();
    _ = cli.handle("DEL B").await.unwrap();
    _ = cli.handle("PUT A 5").await.unwrap();
    let res = cli.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)));
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(None));
    _ = cli.handle("PUT B 5").await.unwrap();
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)));
}
