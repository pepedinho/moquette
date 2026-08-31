use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{broker::topic_tree::TopicTree, packet::encoder::ServerPacket, snowflake::SnowFlake};

pub type ClientSender = mpsc::Sender<ServerPacket>;
pub type ClientId = String;

/// A single subscription of a client to a topic: the outbound channel used to
/// deliver messages plus the maximum QoS the client requested for this
/// subscription. Forwarded messages are capped to this QoS.
#[derive(Debug, Clone)]
pub struct ClientSubscription {
    pub sender: ClientSender,
    pub max_qos: u8,
}

/// Internal protected broker state
#[derive(Debug, Default)]
pub struct BrokerState {
    /// This HashMap associate a 'Topic' to a list of senders (subscribed clients)
    subscriptions: TopicTree,
}

/// This structure act like an envelop that we will clone and
/// share accross all TCP connections
#[derive(Clone)]
pub struct SharedBroker {
    state: Arc<RwLock<BrokerState>>,
    id_gen: Arc<SnowFlake>,
}

impl SharedBroker {
    pub fn new(node_id: u64) -> Self {
        Self {
            state: Arc::new(RwLock::new(BrokerState::default())),
            id_gen: Arc::new(SnowFlake::new(node_id)),
        }
    }

    pub fn generate_client_id(&self) -> String {
        self.id_gen.generate_client_id()
    }

    /// Register a [`ClientSender`] to a topic
    pub fn subscribe(
        &self,
        client_id: String,
        topic: String,
        sender: ClientSender,
        max_qos: u8,
    ) {
        //INFO: ask autorization to write (blocking)
        //WARN: if previous crashed without unlock the state this will panic
        let mut state = self.state.write().unwrap();
        info!("New subscribtion on topic: {}", &topic);

        //INFO: Add the Sender to this topic's list
        //(or create the list if doesn't exist)
        let subscription = ClientSubscription { sender, max_qos };
        state.subscriptions.insert(&topic, client_id, subscription);
    }

    /// Send a message to all subscriber of a topic
    pub async fn publish(&self, topic: &str, packet: ServerPacket) {
        //INFO: ask autorization to read current state
        //used a restricted scope to avoid vicious deadlock
        let subscribers: HashMap<ClientId, ClientSubscription> = {
            let state = self.state.read().unwrap();
            state.subscriptions.get_match(topic)
        };

        // send the packet for all subscriber on this topic
        for (client_id, subscription) in subscribers {
            // Cap the delivery QoS to what this subscriber asked for. Per the
            // MQTT spec the broker must never deliver a message at a higher
            // QoS than the subscription's granted QoS. Without this, a QoS 1/2
            // publish is forwarded as-is (with a 2-byte packet id) even to
            // QoS 0 subscribers, which desyncs simple clients like our ESP32.
            let delivery_qos = match &packet {
                ServerPacket::Publish { qos, .. } => (*qos).min(subscription.max_qos),
                _ => 0,
            };
            let forwarded = match &packet {
                ServerPacket::Publish {
                    topic_name,
                    packet_id,
                    payload,
                    dup,
                    retain,
                    ..
                } => ServerPacket::Publish {
                    topic_name: topic_name.clone(),
                    packet_id: if delivery_qos > 0 { *packet_id } else { None },
                    payload: payload.clone(),
                    dup: *dup,
                    qos: delivery_qos,
                    retain: *retain,
                },
                other => other.clone(),
            };

            if let Err(mpsc::error::TrySendError::Full(_)) =
                subscription.sender.try_send(forwarded)
            {
                warn!("Client <{client_id}> buffer is full, message dropped");
            }
        }
    }

    pub fn unsubscribe(&self, client_id: &str, topic: &str) {
        let mut state = self.state.write().unwrap();
        state
            .subscriptions
            .unsubscribe(topic, &client_id.to_string());

        info!("Client <{}> unsubscribed from topic: {}", client_id, topic);
    }

    pub fn disconnect(&self, client_id: &str) {
        let mut state = self.state.write().unwrap();
        state.subscriptions.remove(&client_id.to_string());
        info!(
            "Client <{}> disconnected, all subscriptions removed",
            client_id
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::encoder::ServerPacket;

    /// A QoS 0 (or QoS 1) subscriber must never receive a forwarded publish at
    /// a higher QoS than they subscribed with. This guards against desyncing
    /// simple QoS 0 clients when the publisher uses QoS 1/2.
    #[tokio::test]
    async fn publish_is_capped_to_subscriber_qos() {
        let broker = SharedBroker::new(1);
        let (tx, mut rx) = mpsc::channel(10);

        broker.subscribe("esp32".to_string(), "planty/mecha/motor".to_string(), tx, 0);

        let publish = ServerPacket::Publish {
            topic_name: "planty/mecha/motor".to_string(),
            packet_id: Some(42),
            payload: b"open".to_vec(),
            dup: false,
            qos: 1,
            retain: false,
        };
        broker.publish("planty/mecha/motor", publish).await;

        let forwarded = rx.try_recv().expect("message should be delivered");
        match forwarded {
            ServerPacket::Publish { qos, packet_id, .. } => {
                assert_eq!(qos, 0, "QoS must be capped to the subscriber's max_qos (0)");
                assert_eq!(packet_id, None, "QoS 0 publish must not carry a packet id");
            }
            other => panic!("expected a Publish, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn publish_carries_packet_id_when_qos_allowed() {
        let broker = SharedBroker::new(1);
        let (tx, mut rx) = mpsc::channel(10);

        broker.subscribe("client".to_string(), "topic".to_string(), tx, 2);

        let publish = ServerPacket::Publish {
            topic_name: "topic".to_string(),
            packet_id: Some(7),
            payload: b"hi".to_vec(),
            dup: false,
            qos: 2,
            retain: false,
        };
        broker.publish("topic", publish).await;

        let forwarded = rx.try_recv().expect("message should be delivered");
        match forwarded {
            ServerPacket::Publish { qos, packet_id, .. } => {
                assert_eq!(qos, 2, "QoS 2 subscriber keeps QoS 2");
                assert_eq!(packet_id, Some(7));
            }
            other => panic!("expected a Publish, got {:?}", other),
        }
    }
}
