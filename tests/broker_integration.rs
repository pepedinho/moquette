use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use std::time::Duration;
use tokio::time::timeout;

use moquette::{Config, broker::SharedBroker, network::server};

async fn spawn_broker(port: u16) -> SharedBroker {
    let broker = SharedBroker::new(1);
    let broker_clone = broker.clone();

    tokio::spawn(async move {
        let mut config = Config::from_file("configs/moquette.toml").unwrap();
        config.server.port = port;
        let _ = server::start(config, broker_clone).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    broker
}

/// Wait for CONNACK to ensure TCP connection handshaking is complete
/// before sending any subsequent MQTT packets (prevents packet merging in TCP buffer).
async fn wait_for_connack(eventloop: &mut rumqttc::EventLoop) {
    timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => break,
                Ok(_event) => {}
                Err(e) => panic!("Eventloop error while waiting for CONNACK: {:?}", e),
            }
        }
    })
    .await
    .expect("Timeout: CONNACK not received");
}

/// Wait for SUBACK response from the broker.
async fn wait_for_suback(eventloop: &mut rumqttc::EventLoop) {
    timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => break,
                Ok(event) => println!("Received event: {:?}", event),
                Err(e) => panic!("Eventloop error while waiting for SUBACK: {:?}", e),
            }
        }
    })
    .await
    .expect("Timeout: SUBACK not received");
}

/// Wait for UNSUBACK response from the broker.
async fn wait_for_unsuback(eventloop: &mut rumqttc::EventLoop) {
    timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::UnsubAck(_))) => break,
                Ok(_event) => {}
                Err(e) => panic!("Eventloop error while waiting for UNSUBACK: {:?}", e),
            }
        }
    })
    .await
    .expect("Timeout: UNSUBACK not received");
}

/// Wait for an incoming PUBLISH packet.
async fn wait_for_publish(eventloop: &mut rumqttc::EventLoop) -> Publish {
    timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(msg))) => return msg,
                Ok(_event) => {}
                Err(e) => panic!("Eventloop error while waiting for PUBLISH: {:?}", e),
            }
        }
    })
    .await
    .expect("Timeout: PUBLISH packet not received")
}

#[tokio::test]
async fn test_wildcards_and_unsubscribe() {
    let port = 18833;
    let _broker = spawn_broker(port).await;

    // Subscriber setup
    let mut sub_opts = MqttOptions::new("subscriber_client", "127.0.0.1", port);
    sub_opts.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

    // Wait for CONNACK first to isolate the CONNECT TCP frame
    wait_for_connack(&mut sub_eventloop).await;

    // Publisher setup
    let mut pub_opts = MqttOptions::new("publisher_client", "127.0.0.1", port);
    pub_opts.set_keep_alive(Duration::from_secs(5));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    // Wait for CONNACK for publisher as well
    wait_for_connack(&mut pub_eventloop).await;

    // Background poller for the publisher after connection establishment
    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    // Subscribe to wildcard topic and await SUBACK
    sub_client
        .subscribe("sensors/+/temperature", QoS::AtMostOnce)
        .await
        .unwrap();
    wait_for_suback(&mut sub_eventloop).await;

    // Publish matching message
    pub_client
        .publish(
            "sensors/kitchen/temperature",
            QoS::AtMostOnce,
            false,
            "21.5",
        )
        .await
        .unwrap();

    let msg = wait_for_publish(&mut sub_eventloop).await;
    assert_eq!(msg.topic, "sensors/kitchen/temperature");
    assert_eq!(msg.payload, "21.5");

    // Unsubscribe and await UNSUBACK
    sub_client
        .unsubscribe("sensors/+/temperature")
        .await
        .unwrap();
    wait_for_unsuback(&mut sub_eventloop).await;

    // Publish message after unsubscribing
    pub_client
        .publish(
            "sensors/kitchen/temperature",
            QoS::AtMostOnce,
            false,
            "23.0",
        )
        .await
        .unwrap();

    // Verify no message is received
    let result = timeout(Duration::from_millis(300), sub_eventloop.poll()).await;
    assert!(
        result.is_err(),
        "Client received a message after unsubscribing"
    );
}

#[tokio::test]
async fn test_stress_concurrent_sub_unsub() {
    let port = 18834;
    let _broker = spawn_broker(port).await;

    let client_count = 200;
    let mut handlers = vec![];

    for i in 0..client_count {
        handlers.push(tokio::spawn(async move {
            let opts = MqttOptions::new(format!("stress_client_{}", i), "127.0.0.1", port);
            let (client, mut eventloop) = AsyncClient::new(opts, 10);

            // 1. Await CONNACK before sending SUBSCRIBE
            wait_for_connack(&mut eventloop).await;

            // 2. Send SUBSCRIBE and await SUBACK
            client.subscribe("device/#", QoS::AtMostOnce).await.unwrap();
            wait_for_suback(&mut eventloop).await;

            // 3. Send UNSUBSCRIBE and await UNSUBACK
            client.unsubscribe("device/#").await.unwrap();
            wait_for_unsuback(&mut eventloop).await;
        }));
    }

    for h in handlers {
        h.await.unwrap();
    }
}
