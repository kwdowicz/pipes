mod pipes_service {
    tonic::include_proto!("pipes_service");
}
use pipes_service::pipes_service_server::{PipesService, PipesServiceServer};
use pipes_service::{SubReply, SubRequest};
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod pipes;
use pipes::Broker;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = Broker::new("broker1");
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

