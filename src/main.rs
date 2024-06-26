
mod pipes_service {
    tonic::include_proto!("pipes_service");
}

use pipes_service::pipes_service_server::{PipesService, PipesServiceServer};
use pipes_service::{SubReply, SubRequest, UnsubRequest, UnsubReply, PostRequest, PostReply, AckRequest, AckReply, FetchRequest, FetchReply};
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use chrono::{DateTime, Utc};
use prost_types::Timestamp;

mod pipes;

use pipes::{Broker};
use crate::pipes::Msg;

#[derive(Debug, Default)]
pub struct MyPipesService {
    broker: Arc<Mutex<Broker>>,
}

#[tonic::async_trait]
impl PipesService for MyPipesService {
    async fn subscribe(&self, request: Request<SubRequest>) -> Result<Response<SubReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply_content = match self.broker.lock() {
            Ok(mut broker) => broker
                .sub(&request.into_inner().pipe_name, "client1")
                .map(|_| "Subscription successful".to_string())
                .unwrap_or_else(|e| e.to_string()),
            Err(_) => "Failed to acquire lock".to_string(),
        };

        let reply = SubReply {
            sub_reply: reply_content,
        };

        Ok(Response::new(reply))
    }

    async fn unsubscribe(&self, request: Request<UnsubRequest>) -> Result<Response<UnsubReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply_content = match self.broker.lock() {
            Ok(mut broker) => broker
                .unsub(&request.into_inner().pipe_name, "client1")
                .map(|_| "Unsubscription successful".to_string())
                .unwrap_or_else(|e| e.to_string()),
            Err(_) => "Failed to acquire lock".to_string(),
        };

        let reply = UnsubReply {
            unsub_reply: reply_content,
        };

        Ok(Response::new(reply))
    }

    async fn post(&self, request: Request<PostRequest>) -> Result<Response<PostReply>, Status> {
        println!("Got a request: {:?}", request);
        let inner_request = request.into_inner();
        let reply_content = match self.broker.lock() {
            Ok(mut broker) => broker
                .post(&inner_request.pipe_name, &inner_request.payload)
                .map(|msg_id| msg_id)
                .unwrap_or_else(|e| e.to_string()),
            Err(_) => "Failed to acquire lock".to_string(),
        };

        let reply = PostReply {
            msg_id: reply_content,
        };

        Ok(Response::new(reply))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckReply>, Status> {
        println!("Got a request: {:?}", request);
        let inner_request = request.into_inner();
        let reply_content = match self.broker.lock() {
            Ok(mut broker) => broker
                .consumed_ack(&inner_request.msg_id, &inner_request.client_id)
                .map(|_| "Ack successful".to_string())
                .unwrap_or_else(|e| e.to_string()),
            Err(_) => "Failed to acquire lock".to_string(),
        };

        let reply = AckReply {
            ack_reply: reply_content,
        };

        Ok(Response::new(reply))
    }

    async fn fetch(
        &self,request: Request<FetchRequest>,) -> Result<Response<FetchReply>, Status> {
        let inner_request = request.into_inner();
        let client_id = inner_request.client_id;
        let mut messages = Vec::new();

        match self.broker.lock() {
            Ok(mut broker) => match broker.fetch(&client_id) {
                Some(msgs) => messages.extend(msgs.into_iter().map(pipes_service::Msg::from)),
                None => return Err(Status::internal("No messages to fetch".to_string())),
            },
            Err(_) => return Err(Status::internal("Failed to acquire lock".to_string())),
        };


        Ok(Response::new(FetchReply { messages }))
    }
}

impl From<pipes::Msg> for pipes_service::Msg {
    fn from(msg: pipes::Msg) -> Self {
        pipes_service::Msg {
            id: msg.id.to_string(),
            payload: msg.payload.to_string(),
            timestamp: Some(datetime_to_prost_timestamp(msg.timestamp)),
        }
    }
}

fn datetime_to_prost_timestamp(datetime: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = Broker::new();
    broker.new_pipe("pipe.name");
    let addr = "127.0.0.1:5005".parse()?;
    let pipes_service = MyPipesService {
        broker: Arc::new(Mutex::new(broker)),
    };
    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(PipesServiceServer::new(pipes_service))
        .serve(addr)
        .await?;

    Ok(())
}
