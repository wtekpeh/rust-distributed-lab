use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct Message {
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Consumer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7001").await?;

    println!("Consumer connected to broker.");
    println!("Waiting for message...");

    loop {
        let mut message_id_buffer = [0_u8; 16];

        match stream.read_exact(&mut message_id_buffer).await {
            Ok(_) => {}

            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Broker closed the connection.");
                break;
            }

            Err(error) => return Err(error.into()),
        }

        let broker_message_id = Uuid::from_bytes(message_id_buffer);

        let mut length_buffer = [0_u8; 4];

        match stream.read_exact(&mut length_buffer).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Broker closed the connection.");
                break;
            }
            Err(error) => return Err(error.into()),
        }

        let message_length = u32::from_be_bytes(length_buffer) as usize;

        let mut message_buffer = vec![0_u8; message_length];

        stream.read_exact(&mut message_buffer).await?;

        let message: Message = serde_json::from_slice(&message_buffer)?;

        println!(
            "Consumer received broker message {}: {}",
            broker_message_id, message.payload
        );

        let ack_marker = 1_u8;

        stream.write_all(&[ack_marker]).await?;

        stream.write_all(broker_message_id.as_bytes()).await?;

        println!(
            "Consumer acknowledged broker message {}.",
            broker_message_id
        );
    }

    Ok(())
}
