use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};

use chrono::prelude::*;

#[derive(Debug)]
struct Broker {
    id: String,
    pipes: HashMap<String, Pipe>,
}

impl Broker {
    fn new(id: String) -> Self {
        Self { pipes: HashMap::new(), id }
    }

    fn new_pipe(&mut self, name: String) {
        let new_pipe = Pipe::new(name, self.id.clone());
        self.pipes.insert(new_pipe.name.clone(), new_pipe);
    }

    fn post(&mut self, pipe_name: String, payload: String) -> Result<(), std::io::Error> {
        match self.pipes.get_mut(&pipe_name) {
            Some(pipe) => {
                pipe.post(Msg::new(payload));
                Ok(())
            }
            None => Err(Error::new(ErrorKind::NotFound, "Pipe not found"))
        }
    }
} 

#[derive(Debug)]
struct Pipe {
    name: String,
    broker_id: String,
    msgs: Vec<Msg>,
}

impl Pipe {
    fn new(name: String, broker_id: String) -> Self {
        Self { name, broker_id, msgs: vec![] }
    }
    
    fn post(&mut self, msg: Msg) {
        self.msgs.push(msg);
    }
}

#[derive(Debug)]
struct Msg {
    payload: String,
    timestamp: DateTime<Utc>,
}

impl Msg {
    fn new(payload: String) -> Self {
        Self { payload, timestamp: Utc::now()}
    }
}

fn main() -> Result<(), std::io::Error> {
    let mut broker1 = Broker::new("first".to_string());
    broker1.new_pipe("input_pipe".to_string());
    broker1.new_pipe("output_pipe".to_string());
    println!("Broker: {:#?}", broker1);
    broker1.post("input_pipe".to_string(), "payload 1".to_string())?;
    broker1.post("input_pipe".to_string(), "payload 11".to_string())?;
    broker1.post("output_pipe".to_string(), "payload 2".to_string())?;
    println!("Broker: {:#?}", broker1);
    broker1.post("another_pipe".to_string(), "payload 3".to_string())?;
    Ok(())
}
