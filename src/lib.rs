//! demo-kv

#![deny(clippy::all)]
#![feature(sync_unsafe_cell)]

mod log;
mod state;
pub use state::KVHandle;

pub mod api {
    tonic::include_proto!("demokv.api");
}
