//! Server cli tool

use demo_kv::{api::kv_server::KvServer, KVHandle};
use std::error::Error as StdError;
use std::path::PathBuf;
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.db")]
    snapshot_path: PathBuf,
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.log")]
    log_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();
    let addr = "0.0.0.0:33333".parse()?;
    let opt = Opt::from_args();
    let handle = KVHandle::new(opt.snapshot_path, opt.log_path).await;
    Server::builder()
        .add_service(KvServer::new(handle))
        .serve(addr)
        .await?;
    log::info!("Server listen at {addr:?}");
    Ok(())
}
