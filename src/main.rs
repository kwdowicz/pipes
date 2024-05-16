use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::sync::Mutex;
use std::sync::Arc;
mod pipes;

type SharedClients = Arc<Mutex<Vec<Sender<Vec<u8>>>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").await.expect("Can't create listener on port 8080");
    let (mut socket, addr) = listener.accept().await.unwrap();
    let (read_half, mut write_half) = socket.split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        let read_bytes = reader.read_line(&mut line).await.unwrap();
        if read_bytes == 0 { break }
        let response = format!("Responding with: {}", line);
        write_half.write_all(response.as_bytes()).await.unwrap();
        line.clear();      
    }
}