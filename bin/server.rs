//! Server cli tool
#![cfg(not(test))]

use demo_kv::{api::kv_server::KvServer, peers::peers_server::PeersServer, KVHandle};
use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    distributed: bool,
    #[structopt(short, long, default_value = "http://127.0.0.1:33333")]
    listen_addr: String,
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.db")]
    snapshot_path: PathBuf,
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.log")]
    log_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();
    let opt = Opt::from_args();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 33333);
    let handle = KVHandle::new(
        opt.distributed,
        &opt.listen_addr,
        &opt.snapshot_path,
        &opt.log_path,
    )
    .await;
    Server::builder()
        .add_service(KvServer::new(handle.clone()))
        .add_service(PeersServer::new(handle))
        .serve(addr)
        .await?;
    log::info!("Server listen at {addr:?}");
    Ok(())
}
