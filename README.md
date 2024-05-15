# pipes

```rust
mod pipes;
use pipes::Broker;
fn main() {
    let mut broker = Broker::new("broker_1");
    broker.new_pipe("pipe_name");
    broker.sub("pipe_name", "client1");
    broker.post("pipe_name", "payload", None).expect("Posting a message");
    match broker.fetch_all("client1") {
        None => println!("Ups..."),
        Some(v) => {
            assert_eq!(v.first().unwrap().payload(), "payload");
        }
    }
}
```