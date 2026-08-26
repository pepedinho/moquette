use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{broker::topic_tree::TopicTree, packet::encoder::ServerPacket, snowflake::SnowFlake};

pub type ClientSender = mpsc::Sender<ServerPacket>;
pub type ClientId = String;

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
    pub fn subscribe(&self, client_id: String, topic: String, sender: ClientSender) {
        //INFO: ask autorization to write (blocking)
        //WARN: if previous crashed without unlock the state this will panic
        let mut state = self.state.write().unwrap();
        info!("New subscribtion on topic: {}", &topic);

        //INFO: Add the Sender to this topic's list
        //(or create the list if doesn't exist)
        state.subscriptions.insert(&topic, client_id, sender);
    }

    /// Send a message to all subscriber of a topic
    pub async fn publish(&self, topic: &str, packet: ServerPacket) {
        //INFO: ask autorization to read current state
        //used a restricted scope to avoid vicious deadlock
        let subscribers: HashMap<ClientId, ClientSender> = {
            let state = self.state.read().unwrap();
            // state
            //     .subscriptions
            //     .get(topic)
            //     .map(|map| map.values().cloned().collect())
            //     .unwrap_or_default()
            state.subscriptions.get_match(topic)
        };

        // send the packet for all subscriber on this topic
        for (client_id, sender) in subscribers {
            if let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send(packet.clone()) {
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
