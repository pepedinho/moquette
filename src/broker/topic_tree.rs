use std::collections::{HashMap, HashSet};

use crate::broker::state::{ClientId, ClientSender};

pub type TopicTree = TopicNode;

#[derive(Default, Debug)]
pub struct TopicNode {
    subscribers: HashMap<ClientId, ClientSender>,
    childrens: HashMap<String, TopicNode>,

    wildcard_single: Option<Box<TopicNode>>,
    wildcard_multi: Option<Box<TopicNode>>,
}

impl TopicNode {
    pub fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            childrens: HashMap::new(),
            wildcard_single: None,
            wildcard_multi: None,
        }
    }

    pub fn insert(&mut self, path: &str, client_id: ClientId, sender: ClientSender) {
        if let Some(parts) = path.split_once('/') {
            let child = self.childrens.entry(parts.0.to_string()).or_default();
            child.insert(parts.1, client_id, sender);
        } else {
            let child = self.childrens.entry(path.to_string()).or_default();
            child.subscribers.insert(client_id, sender);
        }
    }

    pub fn get_match(&self, path: &str) -> HashMap<ClientId, ClientSender> {
        if let Some(parts) = path.split_once('/')
            && let Some(child) = self.childrens.get(parts.0)
        {
            child.get_match(parts.1)
        } else if let Some(child) = self.childrens.get(path) {
            child.subscribers.clone()
        } else {
            HashMap::new()
        }
    }

    pub fn remove(&mut self, client_id: &ClientId) {
        for (_topic, child) in &mut self.childrens {
            child.remove(client_id);
        }

        self.subscribers.remove(client_id);
    }
}
