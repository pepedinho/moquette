use anyhow::Result;
use moquette::{config::Config, network};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_file("configs/moquette.toml").unwrap_or_else(|e| {
        error!("Failed to load config file: {e}");
        std::process::exit(1);
    });

    info!("Config load successfully !");

    network::server::start(config).await?;

    Ok(())
}
