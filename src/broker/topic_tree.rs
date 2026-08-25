use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn create_dummy_sender() -> ClientSender {
        let (tx, _rx) = mpsc::channel(10);
        tx
    }

    #[test]
    fn test_exact_match_single_level() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home", "client_1".to_string(), sender);

        let matches = tree.get_match("home");
        assert_eq!(matches.len(), 1);
        assert!(matches.contains_key("client_1"));

        assert!(tree.get_match("office").is_empty());
    }

    #[test]
    fn test_exact_match_multi_level() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home/livingroom/temp", "client_1".to_string(), sender);

        let matches = tree.get_match("home/livingroom/temp");
        assert_eq!(matches.len(), 1);
        assert!(matches.contains_key("client_1"));
    }

    #[test]
    fn test_branch_isolation() {
        let mut tree = TopicNode::default();
        let sender1 = create_dummy_sender();
        let sender2 = create_dummy_sender();

        tree.insert("home", "client_1".to_string(), sender1);
        tree.insert("home/sensors", "client_2".to_string(), sender2);

        let matches_home = tree.get_match("home");
        assert_eq!(matches_home.len(), 1);
        assert!(matches_home.contains_key("client_1"));

        let matches_sensors = tree.get_match("home/sensors");
        assert_eq!(matches_sensors.len(), 1);
        assert!(matches_sensors.contains_key("client_2"));
    }

    #[test]
    fn test_multiple_subscribers_same_topic() {
        let mut tree = TopicNode::default();
        let sender1 = create_dummy_sender();
        let sender2 = create_dummy_sender();

        tree.insert("home/sensors", "client_1".to_string(), sender1);
        tree.insert("home/sensors", "client_2".to_string(), sender2);

        let matches = tree.get_match("home/sensors");
        assert_eq!(matches.len(), 2);
        assert!(matches.contains_key("client_1"));
        assert!(matches.contains_key("client_2"));
    }

    #[test]
    fn test_remove_client() {
        let mut tree = TopicNode::default();
        let sender1 = create_dummy_sender();
        let sender2 = create_dummy_sender();

        tree.insert("home", "client_1".to_string(), sender1.clone());
        tree.insert("home/sensors", "client_1".to_string(), sender1);
        tree.insert("home/sensors", "client_2".to_string(), sender2);

        tree.remove(&"client_1".to_string());

        assert!(tree.get_match("home").is_empty());

        let matches_sensors = tree.get_match("home/sensors");
        assert_eq!(matches_sensors.len(), 1);
        assert!(matches_sensors.contains_key("client_2"));
    }
}
