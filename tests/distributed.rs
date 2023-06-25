use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    time::Duration,
};

use demo_kv::{
    api::kv_server::KvServer,
    cli::{Client, KvOutput},
    peers::peers_server::PeersServer,
    KVHandle,
};
use etcd_client::{Client as EtcdClient, GetOptions};
use madsim::{
    task::{self, JoinHandle},
    time,
};
use tempfile::{tempdir, TempDir};
use tonic::transport::Server;

// distributed environment
#[derive(Debug)]
struct TestEnv {
    dirs: Vec<TempDir>,
    servers: Vec<JoinHandle<()>>,
}

impl TestEnv {
    async fn init(n: u16) -> Self {
        let mut dirs = vec![];
        let mut servers = vec![];
        for i in 0..n {
            let dir = tempdir().unwrap();
            let task = Self::start_server(33333 + i, dir.path());
            dirs.push(dir);
            servers.push(task);
        }
        time::sleep(Duration::from_secs(2)).await;
        Self { dirs, servers }
    }

    fn start_server(port: u16, dir: &Path) -> JoinHandle<()> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let dir = dir.to_path_buf();
        task::spawn(async move {
            let handle = KVHandle::new(
                true,
                &format!("http://127.0.0.1:{port}"),
                dir.join("test.dkv.db").as_path(),
                dir.join("test.dkv.log").as_path(),
            )
            .await;
            Server::builder()
                .add_service(KvServer::new(handle.clone()))
                .add_service(PeersServer::new(handle))
                .serve(addr)
                .await
                .unwrap();
        })
    }
}

#[madsim::test]
async fn distributed() {
    env_logger::init();
    let _env = TestEnv::init(3).await;

    let mut etcd_client = EtcdClient::connect(["http://127.0.0.1:2379"], None)
        .await
        .unwrap();
    let members = etcd_client
        .get("", Some(GetOptions::new().with_all_keys()))
        .await
        .unwrap();
    let members = members
        .kvs()
        .iter()
        .map(|kv| kv.value_str().unwrap().to_owned())
        .collect::<Vec<_>>();

    let mut cli = Client::new(members[0].clone()).await.unwrap();
    _ = cli.handle("PUT A 0").await.unwrap();
    _ = cli.handle("PUT B 0").await.unwrap();
    _ = cli.handle("PUT A (A+1)").await.unwrap();
    _ = cli.handle("PUT B (B+1)").await.unwrap();
    _ = cli.handle("PUT A (A+1)").await.unwrap();

    let mut cli1 = Client::new(members[1].clone()).await.unwrap();
    let mut cli2 = Client::new(members[2].clone()).await.unwrap();

    let res = cli.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(2)));
    let res = cli.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(1)));

    let res = cli1.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(2)));
    let res = cli1.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(1)));

    let res = cli2.handle("GET A").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(2)));
    let res = cli2.handle("GET B").await.unwrap();
    assert_eq!(res, KvOutput::Value(Some(1)));
}
