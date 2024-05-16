use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use uuid::Uuid;

use chrono::prelude::*;

#[derive(Debug, Clone)]
pub struct Broker {
    id: String,
    pipes: HashMap<String, Pipe>,
    msgs_clients: HashMap<String, HashSet<String>>,
}

impl Broker {
    pub fn new(id: &str) -> Self {
        Self {
            pipes: HashMap::new(),
            id: id.to_string(),
            msgs_clients: HashMap::new(),
        }
    }

    pub fn new_pipe(&mut self, name: &str) {
        let new_pipe = Pipe::new(name.to_string(), self.id.clone());
        self.pipes.insert(new_pipe.name.clone(), new_pipe);
    }

    pub fn post(
        &mut self,
        pipe_name: &str,
        payload: &str,
        tag: Option<String>,
    ) -> Result<(), Error> {
        let pipe = self
            .pipes
            .get_mut(&pipe_name.to_string())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Pipe not found"))?;

        let msg = Msg::new(payload, &pipe.name.clone(), tag);
        pipe.post(msg);

        Ok(())
    }

    pub fn sub(&mut self, pipe_name: &str, client_id: &str) -> Result<(), Error> {
        match self.pipes.get_mut(&pipe_name.to_string()) {
            Some(pipe) => {
                pipe.sub(client_id);
                Ok(())
            }
            None => Err(Error::new(ErrorKind::NotFound, "Pipe not found")),
        }
    }

    pub fn unsub(&mut self, pipe_name: &str, client_id: &str) -> Result<(), Error> {
        match self.pipes.get_mut(&pipe_name.to_string()) {
            Some(pipe) => {
                pipe.unsub(client_id);
                Ok(())
            }
            None => Err(Error::new(ErrorKind::NotFound, "Pipe not found")),
        }
    }

    pub fn fetch_all(&self, client_id: &str) -> Option<Vec<Msg>> {
        let mut msgs = vec![];
        for pipe in self
            .pipes
            .values()
            .filter(|p| p.has_sub(&client_id.to_string()))
        {
            msgs.append(&mut pipe.fetch(&client_id.to_string()).unwrap_or(vec![]));
        }
        let filtered_msgs: Vec<Msg> = msgs
            .iter()
            .filter(|m| !self.was_consumed(&m.id, client_id))
            .cloned()
            .collect();
        if filtered_msgs.is_empty() {
            None
        } else {
            Some(filtered_msgs)
        }
    }

    pub fn consumed_ack(&mut self, msg_id: &str, client_id: &str) {
        self.msgs_clients
            .entry(msg_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(client_id.to_string());
    }

    pub fn was_consumed(&self, msg_id: &str, client_id: &str) -> bool {
        match self.msgs_clients.get(msg_id) {
            None => false,
            Some(clients) => {
                if clients.contains(client_id) {
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pipe {
    name: String,
    broker_id: String,
    msgs: Vec<Msg>,
    subs: HashSet<String>,
}

impl Pipe {
    pub fn new(name: String, broker_id: String) -> Self {
        Self {
            name,
            broker_id,
            msgs: vec![],
            subs: HashSet::new(),
        }
    }

    pub fn post(&mut self, msg: Msg) {
        self.msgs.push(msg);
    }

    pub fn sub(&mut self, client_id: &str) {
        self.subs.insert(client_id.to_string());
    }

    pub fn unsub(&mut self, client_id: &str) {
        self.subs.remove(&client_id.to_string());
    }

    pub fn has_sub(&self, client_id: &String) -> bool {
        match self.subs.get(client_id) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn fetch(&self, client_id: &String) -> Option<Vec<Msg>> {
        if self.msgs.is_empty() {
            return None;
        }
        match self.subs.get(client_id) {
            Some(_) => Some(self.msgs.clone()),
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Msg {
    id: String,
    payload: String,
    timestamp: DateTime<Utc>,
    pipe_name: String,
    tag: String,
}

impl Msg {
    pub fn new(payload: &str, pipe_name: &str, tag: Option<String>) -> Self {
        Self {
            payload: payload.to_string(),
            timestamp: Utc::now(),
            id: Uuid::new_v4().to_string(),
            pipe_name: pipe_name.to_string(),
            tag: tag.unwrap_or("".to_string()),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_broker_creation() {
        let broker = Broker::new("broker1");
        assert_eq!(broker.id, "broker1");
        assert!(broker.pipes.is_empty());
    }

    #[test]
    fn test_pipe_creation() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        assert!(broker.pipes.contains_key("pipe1"));
    }

    #[test]
    fn test_post_message_to_pipe() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        let result = broker.post("pipe1", "payload", None);
        assert!(result.is_ok());
        let pipe = broker.pipes.get("pipe1").unwrap();
        assert_eq!(pipe.msgs.len(), 1);
    }

    #[test]
    fn test_post_message_to_nonexistent_pipe() {
        let mut broker = Broker::new("broker1");
        let result = broker.post("pipe1", "payload", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_subscribe_and_unsubscribe() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        broker.sub("pipe1", "client1");
        let pipe = broker.pipes.get("pipe1").unwrap();
        assert!(pipe.subs.contains("client1"));

        broker.unsub("pipe1", "client1");
        let pipe = broker.pipes.get("pipe1").unwrap();
        assert!(!pipe.subs.contains("client1"));
    }

    #[test]
    fn test_fetch_all_messages_for_subscribed_client() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        broker.new_pipe("pipe2");

        broker.sub("pipe1", "client1");
        broker.sub("pipe2", "client1");

        broker.post("pipe1", "payload1", None).unwrap();
        broker.post("pipe2", "payload2", None).unwrap();

        let msgs = broker.fetch_all("client1").unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn test_fetch_messages_for_non_subscribed_client() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");

        broker.post("pipe1", "payload1", None).unwrap();

        let msgs = broker.fetch_all("client1");
        assert!(msgs.is_none());
    }

    #[test]
    fn test_message_structure() {
        let msg = Msg::new("payload", "pipe1", Some("tag1".to_string()));
        assert_eq!(msg.payload, "payload");
        assert_eq!(msg.pipe_name, "pipe1");
        assert_eq!(msg.tag, "tag1");
    }

    #[test]
    fn test_multiple_subscribe_unsubscribe() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");

        broker.sub("pipe1", "client1");
        broker.sub("pipe1", "client1");
        broker.unsub("pipe1", "client1");
        broker.unsub("pipe1", "client1");

        let pipe = broker.pipes.get("pipe1").unwrap();
        assert!(!pipe.subs.contains("client1"));
    }

    #[test]
    fn test_fetch_empty_pipe() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        broker.sub("pipe1", "client1");

        let msgs = broker.fetch_all("client1");
        assert!(msgs.is_none());
    }

    #[test]
    fn test_consumed_ack_and_fetch() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        broker.sub("pipe1", "client1");

        broker.post("pipe1", "payload1", None).unwrap();
        let msgs = broker.fetch_all("client1").unwrap();
        assert_eq!(msgs.len(), 1);

        broker.consumed_ack(&msgs[0].id, "client1");

        let msgs = broker.fetch_all("client1");
        assert!(msgs.is_none());
    }

    #[test]
    fn test_was_consumed() {
        let mut broker = Broker::new("broker1");
        broker.new_pipe("pipe1");
        broker.sub("pipe1", "client1");

        broker.post("pipe1", "payload1", None).unwrap();
        let msgs = broker.fetch_all("client1").unwrap();

        broker.consumed_ack(&msgs[0].id, "client1");
        assert!(broker.was_consumed(&msgs[0].id, "client1"));
        assert!(!broker.was_consumed(&msgs[0].id, "client2"));
    }
}
