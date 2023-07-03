use log::{info, LevelFilter};
use env_logger::Builder;
use std::{fs::File, io::Write};

pub fn setup() -> std::io::Result<()> {
    let file = File::create("log.txt")?;
    let mut builder = Builder::new();
    builder.format(|buf, record| writeln!(buf, "{}: {}", record.level(), record.args()));
    builder.filter(None, LevelFilter::Info);
    builder.write_style(env_logger::WriteStyle::Always);
    builder.target(env_logger::Target::Writer(Box::new(file)));
    builder.init();
    Ok(())
}

pub fn log_names(names: Vec<&str>) {
    for name in names {
        info!("Processing name: {}", name);
    }
}
