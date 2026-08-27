use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

#[derive(Debug, Deserialize)]
struct Message {
    id: u64,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Consumer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7001").await?;

    println!("Consumer connected to broker.");
    println!("Waiting for message...");

    /*  Delimiter Framing
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
    */

    loop {
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
            "Consumer received message {}: {}",
            message.id, message.payload
        );
    }

    Ok(())
}
