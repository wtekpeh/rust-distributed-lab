use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, Serialize)]
struct Message {
    id: u64,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Producer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7000").await?;

    println!("Producer connected to broker.");

    let messages = [
        Message {
            id: 1,
            payload: "Message One".to_string(),
        },
        Message {
            id: 2,
            payload: "Message Two".to_string(),
        },
        Message {
            id: 3,
            payload: "Message Three".to_string(),
        },
        Message {
            id: 4,
            payload: "Message Four".to_string(),
        },
        Message {
            id: 5,
            payload: "Message Five".to_string(),
        },
    ];

    //Delimiter Framing
    /*
    for message in messages {
        let framed_message = format!("{message}\n");
        stream.write_all(framed_message.as_bytes()).await?;

        println!("Producer sent: {message}");
    }
    */

    //Fixed-Size Frames
    for message in messages {
        let serialized_message = serde_json::to_vec(&message)?;

        let message_length = serialized_message.len() as u32;

        let length_bytes = message_length.to_be_bytes();

        stream.write_all(&length_bytes).await?;
        stream.write_all(&serialized_message).await?;

        println!(
            "Producer sent message {} as {} serialized bytes.",
            message.id,
            serialized_message.len()
        );
    }

    Ok(())
}
