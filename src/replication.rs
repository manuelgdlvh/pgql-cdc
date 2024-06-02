use std::time::{SystemTime, UNIX_EPOCH};
use bytes::{ Bytes};
use futures::StreamExt;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use crate::models::{ ReplicationMessage};

pub async fn start_streaming_changes() -> Result<(), tokio_postgres::Error> {
    let conn_info = format!(
        "user=postgres password=postgres host=localhost port=5432 dbname={} replication=database",
        "postgres"
    );
    let (client, connection) = tokio_postgres::connect(&conn_info, NoTls).await.unwrap();

    tokio::spawn(async move { connection.await });

    let slot_name = "slot_".to_owned()
        + &SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    let lsn = client
        .simple_query(&format!("CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"pgoutput\"", slot_name))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .unwrap()
        .get("consistent_point")
        .unwrap()
        .to_owned();

    let query = format!("START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names 'test')", slot_name, lsn);
    let mut stream = Box::pin(client.copy_both_simple::<Bytes>(&query).await?);
    loop {
        let message = match stream.as_mut().next().await {
            None => continue,
            Some(Err(_)) => continue,
            Some(Ok(mut buffer)) => {
                ReplicationMessage::try_from(buffer)
            }
        };

        println!("{:?}", message);
    }
}
