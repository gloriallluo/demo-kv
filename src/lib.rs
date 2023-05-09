//! demo-kv

#![deny(clippy::all)]

mod state;
pub use state::KVHandle;

mod api {
    tonic::include_proto!("demokv.api");
}
pub use api::kv_client::KvClient;
pub use api::kv_server::KvServer;
