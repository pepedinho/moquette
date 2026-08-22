use anyhow::Result;
use moquette::{broker::state::SharedBroker, config::Config, network};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let config = Config::from_file("configs/moquette.toml").unwrap_or_else(|e| {
        error!("Failed to load config file: {e}");
        std::process::exit(1);
    });

    info!("Config load successfully !");

    let broker = SharedBroker::new();
    network::server::start(config, broker).await?;

    Ok(())
}
