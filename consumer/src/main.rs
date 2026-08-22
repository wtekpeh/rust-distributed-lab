use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Consumer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7001").await?;

    println!("Consumer connected to broker.");
    println!("Waiting for message...");

    let mut buffer = vec![0_u8; 1024];

    let bytes_read = stream.read(&mut buffer).await?;

    println!("Consumer read {bytes_read} bytes.");

    let message = String::from_utf8_lossy(&buffer[..bytes_read]);

    println!("Consumer received: {message}");

    Ok(())
}
