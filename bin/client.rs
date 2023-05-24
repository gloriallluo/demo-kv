//! kv-cli

use demo_kv::cli::{Client, KvOutput};
use std::error::Error as StdError;
use std::io;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();

    let mut client = Client::new("http://127.0.0.1:33333").await?;
    let mut buffer = String::new();

    while io::stdin().read_line(&mut buffer).is_ok() {
        if buffer.is_empty() || buffer.as_str() == "exit\n" || buffer.as_str() == "exit\t\n" {
            break;
        }
        match client.handle(buffer.as_str()).await? {
            KvOutput::Value(v) => println!("got {v:?}"),
            KvOutput::Commit => println!("commit"),
            KvOutput::Abort => println!("abort"),
            _ => {}
        }
        buffer.clear();
    }
    Ok(())
}
