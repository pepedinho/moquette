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
            match parts.0 {
                "+" => {
                    let child = self.wildcard_single.get_or_insert_with(Box::default);
                    child.insert(parts.1, client_id, sender);
                }
                "#" => {
                    //INFO : incorrect MQTT syntax ('#' must be the last segment)
                }
                _ => {
                    let child = self.childrens.entry(parts.0.to_string()).or_default();
                    child.insert(parts.1, client_id, sender);
                }
            }
        } else {
            match path {
                "+" => {
                    let child = self.wildcard_single.get_or_insert_with(Box::default);
                    child.subscribers.insert(client_id, sender);
                }
                "#" => {
                    let child = self.wildcard_multi.get_or_insert_with(Box::default);
                    child.subscribers.insert(client_id, sender);
                }
                _ => {
                    let child = self.childrens.entry(path.to_string()).or_default();
                    child.subscribers.insert(client_id, sender);
                }
            }
        }
    }

    pub fn get_match(&self, path: &str) -> HashMap<ClientId, ClientSender> {
        let mut matches = HashMap::new();
        if let Some(multi) = &self.wildcard_multi {
            matches.extend(multi.subscribers.clone());
        }

        if let Some((segment, rest)) = path.split_once('/') {
            if let Some(single) = &self.wildcard_single {
                matches.extend(single.get_match(rest));
            }

            if let Some(child) = self.childrens.get(segment) {
                matches.extend(child.get_match(rest));
            }
        } else {
            if let Some(single) = &self.wildcard_single {
                matches.extend(single.subscribers.clone());
                if let Some(mutli) = &single.wildcard_multi {
                    matches.extend(mutli.subscribers.clone());
                }
            }

            if let Some(child) = self.childrens.get(path) {
                matches.extend(child.subscribers.clone());
                if let Some(multi) = &child.wildcard_multi {
                    matches.extend(multi.subscribers.clone());
                }
            }
        }

        matches
    }

    pub fn remove(&mut self, client_id: &ClientId) {
        self.subscribers.remove(client_id);

        self.childrens.retain(|_segment, child| {
            child.remove(client_id);
            !child.is_empty()
        });

        if let Some(child) = &mut self.wildcard_single {
            child.remove(client_id);
            if child.is_empty() {
                self.wildcard_single = None;
            }
        }

        if let Some(child) = &mut self.wildcard_multi {
            child.remove(client_id);
            if child.is_empty() {
                self.wildcard_multi = None;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.childrens.is_empty()
            && self.wildcard_single.as_ref().is_none_or(|c| c.is_empty())
            && self.wildcard_multi.as_ref().is_none_or(|c| c.is_empty())
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

    #[test]
    fn test_single_level_wildcard_plus() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home/+/temp", "client_plus".to_string(), sender.clone());
        tree.insert("home/livingroom/temp", "client_exact".to_string(), sender);

        let matches = tree.get_match("home/livingroom/temp");
        assert_eq!(matches.len(), 2);
        assert!(matches.contains_key("client_plus"));
        assert!(matches.contains_key("client_exact"));

        let matches_kitchen = tree.get_match("home/kitchen/temp");
        assert_eq!(matches_kitchen.len(), 1);
        assert!(matches_kitchen.contains_key("client_plus"));

        assert!(tree.get_match("home/livingroom/temp/celsius").is_empty());
        assert!(tree.get_match("home/temp").is_empty());
    }

    #[test]
    fn test_multi_level_wildcard_hash() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home/#", "client_hash".to_string(), sender);

        assert_eq!(tree.get_match("home").len(), 1);
        assert_eq!(tree.get_match("home/livingroom").len(), 1);
        assert_eq!(tree.get_match("home/livingroom/temp/sensor").len(), 1);

        assert!(tree.get_match("office/livingroom").is_empty());
    }

    #[test]
    fn test_combined_wildcards_and_exact() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("#", "client_all".to_string(), sender.clone());
        tree.insert("home/+", "client_plus".to_string(), sender.clone());
        tree.insert("home/sensors/temp", "client_exact".to_string(), sender);

        let matches = tree.get_match("home/sensors/temp");
        assert_eq!(matches.len(), 2);
        assert!(matches.contains_key("client_all"));
        assert!(matches.contains_key("client_exact"));

        let matches_sensors = tree.get_match("home/sensors");
        assert_eq!(matches_sensors.len(), 2);
        assert!(matches_sensors.contains_key("client_all"));
        assert!(matches_sensors.contains_key("client_plus"));
    }

    #[test]
    fn test_invalid_hash_position_ignored() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home/#/temp", "client_invalid".to_string(), sender);

        assert!(tree.get_match("home/livingroom/temp").is_empty());
    }

    #[test]
    fn test_remove_client_from_wildcards() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert("home/+", "client_1".to_string(), sender.clone());
        tree.insert("sensors/#", "client_1".to_string(), sender.clone());
        tree.insert("home/livingroom", "client_2".to_string(), sender);

        tree.remove(&"client_1".to_string());

        let matches_home = tree.get_match("home/livingroom");
        assert_eq!(matches_home.len(), 1);
        assert!(matches_home.contains_key("client_2"));

        assert!(tree.get_match("sensors/temp/humidity").is_empty());
    }

    #[test]
    fn test_tree_pruning_on_remove() {
        let mut tree = TopicNode::default();
        let sender = create_dummy_sender();

        tree.insert(
            "home/livingroom/temp",
            "client_1".to_string(),
            sender.clone(),
        );
        tree.insert("sensors/+", "client_1".to_string(), sender);

        assert!(!tree.is_empty());

        tree.remove(&"client_1".to_string());

        assert!(tree.childrens.is_empty());
        assert!(tree.wildcard_single.is_none());
        assert!(tree.is_empty());
    }
}
