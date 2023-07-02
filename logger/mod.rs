[package]
name = "my_project"
version = "0.1.0"
edition = "2018"

# See more keys and their definitions at https://doc.rust-lang.org/cargo/reference/manifest.html

[[bin]]
name = "my_project"
path = "src/main.rs"

[lib]
name = "logger"
path = "logger/mod.rs"

[dependencies]
log = "0.4"
env_logger = "0.9"
