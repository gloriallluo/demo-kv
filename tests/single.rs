//! Some basic kv ops on single node.

use demo_kv::{
    api::kv_server::KvServer,
    cli::{Client, KvOutput},
    KVHandle,
};
use futures_core::Future;
use madsim::{
    task::{self, JoinHandle},
    time,
};
use std::{
    error::Error as StdError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Once,
    time::Duration,
};
use tempfile::{tempdir, TempDir};
use tokio::join;
use tonic::transport::Server;

// single server
#[derive(Debug)]
struct TestEnv {
    dir: TempDir,
    server: Option<JoinHandle<()>>,
}

static LOGGER: Once = Once::new();

fn logger_setup() {
    LOGGER.call_once(|| {
        env_logger::init();
    })
}

impl TestEnv {
    async fn init(port: u16) -> Result<Self, Box<dyn StdError>> {
        let dir = tempdir().unwrap();
        let task = Self::start_server(port, dir.path());
        time::sleep(Duration::from_millis(200)).await;
        Ok(Self {
            dir,
            server: Some(task),
        })
    }

    async fn restart(&mut self, port: u16) {
        let task = Self::start_server(port, self.dir.path());
        self.server = Some(task);
        time::sleep(Duration::from_millis(200)).await;
    }

    async fn kill(&mut self) {
        self.server.take().unwrap().abort();
        time::sleep(Duration::from_millis(200)).await;
    }

    fn start_server(port: u16, dir: &Path) -> JoinHandle<()> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let dir = dir.to_path_buf();
        task::spawn(async move {
            let handle = KVHandle::new(
                false,
                &&format!("http://127.0.0.1:{port}"),
                dir.join("test.dkv.db").as_path(),
                dir.join("test.dkv.log").as_path(),
            )
            .await;
            Server::builder()
                .add_service(KvServer::new(handle))
                .serve(addr)
                .await
                .unwrap();
        })
    }
}

#[madsim::test]
async fn basic() {
    logger_setup();
    let _env = TestEnv::init(33333).await.unwrap();
    let mut cli = Client::new("http://127.0.0.1:33333".to_string())
        .await
        .unwrap();
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

#[madsim::test]
async fn persist() {
    logger_setup();
    let mut env = TestEnv::init(33334).await.unwrap();
    let mut cli = Client::new("http://127.0.0.1:33334".to_string())
        .await
        .unwrap();
    _ = cli.handle("PUT A 5").await.unwrap();
    let res = cli.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)));
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(None));
    _ = cli.handle("PUT B 5").await.unwrap();
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)));
    drop(cli);
    env.kill().await;

    env.restart(33334).await;
    let mut cli = Client::new("http://127.0.0.1:33334".to_string())
        .await
        .unwrap();
    let res = cli.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)), "persistence failed");
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(5)), "persistence failed");
}

#[madsim::test]
async fn concurrent() {
    fn client_future(key1: &'static str, key2: &'static str) -> impl Future<Output = ()> + 'static {
        async move {
            let mut cli = Client::new("http://127.0.0.1:33335".to_string())
                .await
                .unwrap();
            for _ in 0..10 {
                _ = cli
                    .handle(format!("PUT {} 1", key1).as_str())
                    .await
                    .unwrap();
                _ = cli
                    .handle(format!("PUT {} 1", key2).as_str())
                    .await
                    .unwrap();
                _ = cli
                    .handle(format!("PUT {0} ({0}+1)", key1).as_str())
                    .await
                    .unwrap();
                _ = cli
                    .handle(format!("PUT {0} ({0}+1)", key2).as_str())
                    .await
                    .unwrap();
                let res = cli.handle(format!("GET {}", key1).as_str()).await.unwrap();
                assert_eq!(
                    res,
                    KvOutput::Value(Some(2)),
                    "failed to read my own writes"
                );
                _ = cli.handle(format!("DEL {}", key1).as_str()).await.unwrap();
                let res = cli.handle(format!("GET {}", key2).as_str()).await.unwrap();
                assert_eq!(
                    res,
                    KvOutput::Value(Some(2)),
                    "failed to read my own writes"
                );
                _ = cli.handle(format!("DEL {}", key2).as_str()).await.unwrap();
            }
        }
    }

    logger_setup();
    let _env = TestEnv::init(33335).await.unwrap();
    let fut1 = task::spawn(client_future("A", "B"));
    let fut2 = task::spawn(client_future("C", "D"));
    let fut3 = task::spawn(client_future("E", "F"));
    _ = join!(fut1, fut2, fut3);
}

#[madsim::test]
async fn txn1() {
    logger_setup();
    let _env = TestEnv::init(33336).await.unwrap();
    let mut cli = Client::new("http://127.0.0.1:33336".to_string())
        .await
        .unwrap();
    _ = cli.handle("PUT A 0").await.unwrap();
    _ = cli.handle("PUT B 0").await.unwrap();

    let mut clients = vec![];

    for _ in 0..3 {
        clients.push(task::spawn(async move {
            let mut cli = Client::new("http://127.0.0.1:33336".to_string())
                .await
                .unwrap();
            loop {
                _ = cli.handle("BEGIN").await.unwrap();
                _ = cli.handle("PUT A (A+1)").await.unwrap();
                _ = cli.handle("PUT B (B+1)").await.unwrap();
                _ = cli.handle("PUT A (A+1)").await.unwrap();
                _ = cli.handle("PUT B (B+1)").await.unwrap();
                _ = cli.handle("COMMIT").await.unwrap();

                _ = cli.handle("BEGIN").await.unwrap();
                let res0 = cli.handle("GET A").await.unwrap();
                let res1 = cli.handle("GET B").await.unwrap();
                let res = cli.handle("COMMIT").await.unwrap();
                if res == KvOutput::Commit {
                    assert_eq!(res0, res1, "failed to achieve atomicity");
                }

                _ = cli.handle("BEGIN").await.unwrap();
                let res0 = cli.handle("GET A").await.unwrap();
                let res1 = cli.handle("GET B").await.unwrap();
                let res = cli.handle("COMMIT").await.unwrap();
                if res == KvOutput::Commit {
                    assert_eq!(res0, res1, "failed to achieve atomicity");
                }

                _ = cli.handle("BEGIN").await.unwrap();
                _ = cli.handle("PUT A (A-1)").await.unwrap();
                _ = cli.handle("PUT B (B-1)").await.unwrap();
                _ = cli.handle("COMMIT").await.unwrap();
            }
        }));
    }

    time::sleep(Duration::from_secs(5)).await;
    clients.into_iter().for_each(|handle| handle.abort());

    let res0 = cli.handle("GET A").await.unwrap();
    let res1 = cli.handle("GET B").await.unwrap();
    assert_eq!(res0, res1);
}

#[madsim::test]
async fn txn2() {
    logger_setup();
    let _env = TestEnv::init(33337).await.unwrap();
    let mut cli = Client::new("http://127.0.0.1:33337".to_string())
        .await
        .unwrap();
    _ = cli.handle("PUT A 0").await.unwrap();
    _ = cli.handle("PUT B 0").await.unwrap();

    let mut clients = vec![];

    for _ in 0..2 {
        clients.push(task::spawn(async move {
            let mut cli = Client::new("http://127.0.0.1:33337".to_string())
                .await
                .unwrap();
            loop {
                _ = cli.handle("BEGIN").await.unwrap();
                _ = cli.handle("PUT A (A+1)").await.unwrap();
                _ = cli.handle("PUT B (B+1)").await.unwrap();
                _ = cli.handle("PUT A (B+1)").await.unwrap();
                _ = cli.handle("PUT B (A+1)").await.unwrap();
                _ = cli.handle("COMMIT").await.unwrap();
            }
        }));
    }

    for _ in 0..8 {
        clients.push(task::spawn(async move {
            let mut cli = Client::new("http://127.0.0.1:33337".to_string())
                .await
                .unwrap();
            loop {
                _ = cli.handle("BEGIN").await.unwrap();
                let res0 = cli.handle("GET A").await.unwrap();
                let res1 = cli.handle("GET B").await.unwrap();
                let res2 = cli.handle("GET A").await.unwrap();
                let res3 = cli.handle("GET B").await.unwrap();
                let res = cli.handle("COMMIT").await.unwrap();
                if res == KvOutput::Commit {
                    assert_eq!(res0, res2, "failed to achieve repeatable read");
                    assert_eq!(res1, res3, "failed to achieve repeatable read");
                }
            }
        }));
    }

    time::sleep(Duration::from_secs(5)).await;
    clients.into_iter().for_each(|handle| handle.abort());

    let _res = cli.handle("GET A").await.unwrap();
    let _res = cli.handle("GET B").await.unwrap();
}
