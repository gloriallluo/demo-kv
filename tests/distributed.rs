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
            let task = Self::start_server(13333 + i, dir.path());
            dirs.push(dir);
            servers.push(task);
        }
        time::sleep(Duration::from_secs(2)).await;
        Self { dirs, servers }
    }

    async fn add_node(&mut self) {
        let i = self.dirs.len() as u16;
        let dir = tempdir().unwrap();
        let task = Self::start_server(13333 + i, dir.path());
        self.dirs.push(dir);
        self.servers.push(task);
        time::sleep(Duration::from_secs(2)).await;
    }

    async fn kill_node(&mut self, i: usize) {
        self.servers.remove(i);
        self.dirs.remove(i);
        time::sleep(Duration::from_millis(1)).await;
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
    let mut env = TestEnv::init(3).await;

    let mut etcd_client = EtcdClient::connect(["http://127.0.0.1:2379"], None)
        .await
        .unwrap();
    let members = etcd_client
        .get("member", Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    let leader_resp = etcd_client
        .leader("leader")
        .await
        .expect("failed to ask leader");
    let leader_id = leader_resp.kv().unwrap().value_str().unwrap();
    let leader_id = members
        .kvs()
        .iter()
        .position(|kv| kv.key_str().unwrap() == format!("member-{leader_id}"))
        .unwrap();
    let members = members
        .kvs()
        .iter()
        .map(|kv| kv.value_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let leader_addr = members[leader_id].clone();

    let mut cli = vec![];
    for m in members {
        cli.push(Client::new(m).await.unwrap());
    }
    let mut leader = Client::new(leader_addr).await.unwrap();
    _ = leader.handle("PUT A 0").await.unwrap();
    _ = leader.handle("PUT B 0").await.unwrap();
    _ = leader.handle("PUT A (A+1)").await.unwrap();
    _ = leader.handle("PUT B (B+1)").await.unwrap();
    _ = leader.handle("PUT A (A+1)").await.unwrap();

    for i in 0..3 {
        let res = cli[i].handle("GET A").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(2)));
        let res = cli[i].handle("GET B").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(1)));
    }

    env.add_node().await;

    let members = etcd_client
        .get("member", Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    let members = members
        .kvs()
        .iter()
        .map(|kv| kv.value_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let mut cli = vec![];
    for m in members {
        cli.push(Client::new(m).await.unwrap());
    }

    for i in 0..4 {
        let res = cli[i].handle("GET A").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(2)));
        let res = cli[i].handle("GET B").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(1)));
    }

    _ = leader.handle("PUT A (A+1)").await.unwrap();
    _ = leader.handle("PUT B (B+1)").await.unwrap();
    _ = leader.handle("PUT A (A+1)").await.unwrap();

    for i in 0..4 {
        let res = cli[i].handle("GET A").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(4)));
        let res = cli[i].handle("GET B").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(2)));
    }

    let victim = rand::random::<usize>() % 4;
    env.kill_node(victim).await;

    for i in 0..4 {
        if i == victim {
            continue;
        }
        let res = cli[i].handle("GET A").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(4)));
        let res = cli[i].handle("GET B").await.unwrap();
        assert_eq!(res, KvOutput::Value(Some(2)));
    }
}
