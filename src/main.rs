use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::sync::Mutex;
use std::sync::Arc;

type SharedClients = Arc<Mutex<Vec<Sender<Vec<u8>>>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let clients: SharedClients = Arc::new(Mutex::new(Vec::new()));

    loop {
        let (mut socket, _) = listener.accept().await?;
        let clients = Arc::clone(&clients);

        // Create a channel to send messages to this client
        let (tx, mut rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel(100);
        clients.lock().await.push(tx);

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                tokio::select! {
                    // Read data from the socket
                    n = socket.read(&mut buf) => {
                        let n = match n {
                            // socket closed
                            Ok(n) if n == 0 => return,
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        // Broadcast the data to all connected clients
                        let message = buf[..n].to_vec();
                        let clients = clients.lock().await;
                        for client in clients.iter() {
                            let _ = client.send(message.clone()).await;
                        }
                    }
                    // Write data to the socket from the channel
                    message = rx.recv() => {
                        if let Some(message) = message {
                            if let Err(e) = socket.write_all(&message).await {
                                eprintln!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                        }
                    }
                }
            }
        });
    }
}