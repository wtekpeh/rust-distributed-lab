use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let producer_listener = TcpListener::bind("127.0.0.1:7000").await?;
    let consumer_listener = TcpListener::bind("127.0.0.1:7001").await?;

    println!("Broker listening for producer on 127.0.0.1:7000");
    println!("Broker listening for consumer on 127.0.0.1:7001");

    println!("Waiting for consumer...");

    let (mut consumer_stream, consumer_address) = consumer_listener.accept().await?;

    println!("Consumer connected from {consumer_address}");

    println!("Waiting for producer...");

    let (mut producer_stream, producer_address) = producer_listener.accept().await?;

    println!("Producer connected from {producer_address}");

    let mut buffer = vec![0_u8; 1024];

    loop {
        let bytes_read = producer_stream.read(&mut buffer).await?;

        if bytes_read == 0 {
            println!("Producer closed the connection.");
            break;
        }

        println!("Broker received {bytes_read} bytes from producer.");

        consumer_stream.write_all(&buffer[..bytes_read]).await?;

        println!("Broker forwarded message to consumer.");
    }

    Ok(())
}
