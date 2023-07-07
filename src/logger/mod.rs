use std::fs::File;
use std::sync::Mutex;
use std::io::Write;
use tracing::{info, Level};
use tracing_subscriber::fmt;
use tracing_appender;

pub fn setup_logger() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::create("quant_demo.log").unwrap();
    let subscriber = fmt()
        .with_max_level(Level::INFO)
        .with_writer(Mutex::new(file))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
    Ok(())
}

pub async fn log_name(name: &str) {
    info!("Processing name: {}", name);
}

pub async fn log_polars_object<T: std::fmt::Debug>(gen_object: &T){
    info!("Polars_Object: {:?}", &gen_object);
}
