use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::io::{split, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedWriteHalf, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
mod pipes;
use lazy_static::lazy_static;
use pipes::Broker;

lazy_static! {
    static ref BROKER: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new("local")));
}

#[tokio::main]
async fn main() {
    {
        let mut broker = BROKER.lock().await;
        broker.new_pipe("pipe.name");
    }

    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .expect("Can't create listener on port 8080");

    loop {
        let (socket, _addr) = listener.accept().await.unwrap();
        handle_client(socket).await;
    }
}

async fn handle_client(mut socket: TcpStream) {
    tokio::spawn(async move {
        let (read_half, mut write_half) = socket.split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            let mut response = String::new();
            let read_bytes = match reader.read_line(&mut line).await {
                Ok(bytes) if bytes == 0 => break,
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Failed to read from socket: {}", e);
                    break;
                }
            };
            response = match prefix(&line).await {
                Prefix::Sub => handle_sub(line.clone())
                    .await
                    .unwrap_or_else(|e| e.to_string()),
                Prefix::Unsub => handle_unsub(line.clone())
                    .await
                    .unwrap_or_else(|e| e.to_string()),
                Prefix::Post => handle_post(line.clone())
                    .await
                    .unwrap_or_else(|e| e.to_string()),
                Prefix::Unknown => handle_unknown().await,
            };
            println!("Responding with: {}", response);
            {
                let broker = BROKER.lock().await;
                println!("Broker: {:#?}", broker);
            }
            write_half.write_all(response.as_bytes()).await.unwrap();
            line.clear();
        }
    });
}

async fn handle_unknown() -> String {
    String::from("Unknown command")
}

async fn handle_post(line: String) -> Result<String, Error> {
    let args: Vec<&str> = line.split("|").collect();
    match (args.get(1), args.get(2)) {
        (Some(pipe_name), Some(payload)) => {
            let mut broker = BROKER.lock().await;
            broker
                .post(pipe_name, payload, None)
                .map(|_| "OK".to_string())
        }
        _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid input")),
    }
}

async fn handle_unsub(line: String) -> Result<String, Error> {
    let args: Vec<&str> = line.split('|').collect();
    let pipe_name = args
        .get(1)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Invalid input"))?;

    let mut broker = BROKER.lock().await;
    broker.unsub(pipe_name, "client1").map(|_| "OK".to_string())
}

async fn handle_sub(line: String) -> Result<String, Error> {
    let args: Vec<&str> = line.split('|').collect();
    let pipe_name = args
        .get(1)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Invalid input"))?;

    let mut broker = BROKER.lock().await;
    broker.sub(pipe_name, "client1").map(|_| "OK".to_string())
}

async fn prefix(s: &str) -> Prefix {
    if s.starts_with("sub") {
        Prefix::Sub
    } else if s.starts_with("unsub") {
        Prefix::Unsub
    } else if s.starts_with("post") {
        Prefix::Post
    } else {
        Prefix::Unknown
    }
}

enum Prefix {
    Sub,
    Unsub,
    Post,
    Unknown,
}
