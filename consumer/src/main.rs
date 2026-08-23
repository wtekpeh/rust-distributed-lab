use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Consumer starting...");

    let stream = TcpStream::connect("127.0.0.1:7001").await?;

    println!("Consumer connected to broker.");
    println!("Waiting for message...");

    let mut reader = BufReader::new(stream);

    loop {
        let mut message = String::new();

        let bytes_read = reader.read_line(&mut message).await?;

        if bytes_read == 0 {
            println!("Broker closed the connection.");
            break;
        }

        let message = message.trim_end();

        println!("Consumer received message: {message}");
    }

    Ok(())
}
