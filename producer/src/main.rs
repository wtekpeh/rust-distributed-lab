use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use uuid::Uuid;

#[derive(Debug, Serialize)]
struct Message {
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Producer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7000").await?;

    println!("Producer connected to broker.");

    let messages = [
        Message {
            payload: "Message One".to_string(),
        },
        Message {
            payload: "Message Two".to_string(),
        },
        Message {
            payload: "Message Three".to_string(),
        },
        Message {
            payload: "Message Four".to_string(),
        },
        Message {
            payload: "Message Five".to_string(),
        },
    ];

    for message in messages {
        let message_id = Uuid::new_v4();

        let serialized_message = serde_json::to_vec(&message)?;

        let message_id_bytes = message_id.as_bytes();

        let message_length = serialized_message.len() as u32;
        let length_bytes = message_length.to_be_bytes();

        stream.write_all(message_id_bytes).await?;
        stream.write_all(&length_bytes).await?;
        stream.write_all(&serialized_message).await?;

        println!(
            "Producer sent message {} as {} serialized bytes.",
            message_id,
            serialized_message.len()
        );
    }

    Ok(())
}
