extern crate core;
use anyhow::Result;
use tokio::task;

mod replication;
mod models;

#[tokio::main]
async fn main() -> Result<()> {
    let _ =
        task::spawn(async { replication::start_streaming_changes().await });

    Ok(())
}
