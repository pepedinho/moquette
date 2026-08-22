use anyhow::Result;
use moquette::network;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    network::server::start("127.0.0.1:1883").await?;

    Ok(())
}
