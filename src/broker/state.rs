use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::mpsc;
use tracing::info;

use crate::packet::encoder::ServerPacket;

pub type ClientSender = mpsc::Sender<ServerPacket>;

/// Internal protected broker state
#[derive(Debug, Default)]
pub struct BrokerState {
    /// This HashMap associate a 'Topic' to a list of senders (subscribed clients)
    subscriptions: HashMap<String, Vec<ClientSender>>,
}

/// This structure act like an envelop that we will clone and
/// share accross all TCP connections
#[derive(Clone)]
pub struct SharedBroker {
    state: Arc<RwLock<BrokerState>>,
}

impl Default for SharedBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedBroker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(BrokerState::default())),
        }
    }

    /// Register a [`ClientSender`] to a topic
    pub fn subscribe(&self, topic: String, sender: ClientSender) {
        //INFO: ask autorization to write (blocking)
        //WARN: if previous crashed without unlock the state this will panic
        let mut state = self.state.write().unwrap();
        info!("New subscribtion on topic: {}", &topic);

        //INFO: Add the Sender to this topic's list
        //(or create the list if doesn't exist)
        state.subscriptions.entry(topic).or_default().push(sender);
    }

    /// Send a message to all subscriber of a topic
    pub async fn publish(&self, topic: &str, packet: ServerPacket) {
        //INFO: ask autorization to read current state
        //used a restricted scope to avoid vicious deadlock
        let subscribers = {
            let state = self.state.read().unwrap();
            state.subscriptions.get(topic).cloned()
        };

        //INFO: Fetch the list of current client subscibed to the reqested topic
        if let Some(subscribers) = subscribers {
            //INFO: Send a clone of the packet to each subscriber
            for sender in subscribers {
                let _ = sender.send(packet.clone()).await;
            }
        }
    }
}
