use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{self, AckPolicy},
    stream::{DiscardPolicy, RetentionPolicy, StorageType},
};
use futures::StreamExt;
use tracing::warn;

const CONSUMER_NAME: &str = "consumer";

#[tokio::main]
async fn main() {
    let client = create_nats_client().await;
    let mut stream = create_stream("my_stream".to_string(), &client).await;

    loop {
        let msg = stream.next().await;
        println!("{msg:?}");
    }
}

pub async fn create_nats_client() -> async_nats::Client {
    let username = String::from("nats");
    let password = String::from("nats");

    let options = async_nats::ConnectOptions::new()
        .name("consumer")
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

async fn create_stream(name: String, client: &async_nats::Client) -> consumer::pull::Stream {
    let subject = String::from("subject");
    let context = jetstream::new(client.clone());
    let stream_config = jetstream_config(name, subject);
    let jetstream = context.create_stream(stream_config).await.unwrap();
    let consumer_config = pull_consumer_config(String::from(CONSUMER_NAME));
    let consumer = jetstream
        .get_or_create_consumer(CONSUMER_NAME, consumer_config)
        .await
        .unwrap();
    consumer.messages().await.unwrap()
}

pub fn jetstream_config(name: String, subject: String) -> jetstream::stream::Config {
    jetstream::stream::Config {
        name,
        subjects: vec![subject],
        discard: DiscardPolicy::New,
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::Memory,
        ..Default::default()
    }
}

pub fn pull_consumer_config(name: String) -> consumer::pull::Config {
    jetstream::consumer::pull::Config {
        name: Some(name),
        ack_policy: AckPolicy::Explicit,
        ..Default::default()
    }
}
