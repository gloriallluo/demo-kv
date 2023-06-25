use demo_kv::cli::Client;

#[tokio::main]
async fn main() {
    let mut cli = Client::new("http://127.0.0.1:33333".to_string())
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
}
