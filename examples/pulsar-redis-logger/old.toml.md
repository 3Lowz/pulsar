[package]
name = "pulsar-redis-logger"
version.workspace = true
license.workspace = true
edition.workspace = true
repository.workspace = true

[dependencies]
redis = { version = "0.25.4", features = ["json", "tokio-comp"] }
redis_rs = "0.9.0"
