use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let listener = TcpListener::bind("127.0.0.1:7001").await?;

    println!("Broker listening on 127.0.0.1:7001");
    println!("Waiting for consumer...");

    let (mut stream, address) = listener.accept().await?;

    println!("Consumer connected from {address}");

    let message = "Hello from broker";

    stream.write_all(message.as_bytes()).await?;

    println!("Broker sent: {message}");

    Ok(())
}
