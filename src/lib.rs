//! demo-kv

#![deny(clippy::all)]
#![feature(sync_unsafe_cell)]

mod log;
mod state;
mod ts;

pub use state::KVHandle;

/// tonic APIs
pub mod api {
    tonic::include_proto!("demokv.api");
}
