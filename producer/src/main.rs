use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Producer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7000").await?;

    println!("Producer connected to broker.");

    let message = "Hello from producer";

    stream.write_all(message.as_bytes()).await?;

    println!("Producer sent: {message}");

    Ok(())
}
