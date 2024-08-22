// nats-server -js --user nats --pass nats --addr 127.0.0.1 -p 23561

use std::time::Duration;

use async_nats::jetstream;
use bytes::Bytes;
use tracing::warn;

#[tokio::main]
async fn main() {
    let client = create_nats_client().await;
    let context = jetstream::new(client);

    loop {
        let ack = context
            .publish(String::from("subject"), Bytes::from_static(b"message"))
            .await
            .unwrap();
        println!("ack: {:?}", ack.await);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn create_nats_client() -> async_nats::Client {
    let username = String::from("nats");
    let password = String::from("nats");

    let options = async_nats::ConnectOptions::new()
        .name("producer")
        .client_capacity(4096)
        .subscription_capacity(65536)
        .retry_on_initial_connect()
        .connection_timeout(Duration::from_secs(10))
        .user_and_password(username, password)
        .require_tls(false)
        .event_callback(move |event| async move {
            warn!(%event, "nats connection event");
        });

    let addr: String = String::from("nats://127.0.0.1:23561");
    async_nats::connect_with_options(addr, options)
        .await
        .unwrap()
}
