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

impl Default for Broker {
    fn default() -> Self {
        Broker {
            id: String::new(),
            pipes: HashMap::new(),
            msgs_clients: HashMap::new(),
        }
    }
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

        let msg = Msg::new(payload, &pipe.name.clone());
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
}

impl Msg {
    pub fn new(payload: &str, pipe_name: &str) -> Self {
        Self {
            payload: payload.to_string(),
            timestamp: Utc::now(),
            id: Uuid::new_v4().to_string(),
            pipe_name: pipe_name.to_string(),
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
}
