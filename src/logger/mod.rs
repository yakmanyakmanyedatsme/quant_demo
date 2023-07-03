use std::fs::File;
use std::io::Write;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

fn setup_logger() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::create("log.txt")?;
    let writer = tracing_appender::non_blocking(file);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish_with_writer(writer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
    Ok(())
}

fn log_name(name: &str) {
    info!("Processing name: {}", name);
}
