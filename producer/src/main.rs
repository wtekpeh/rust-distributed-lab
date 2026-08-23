use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Producer starting...");

    let mut stream = TcpStream::connect("127.0.0.1:7000").await?;

    println!("Producer connected to broker.");

    let messages = [
        "Message One",
        "Message Two",
        "Message Three",
        "Message Four",
        "Message Five",
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
        let payload = message.as_bytes();

        let payload_length = payload.len() as u32;

        let length_bytes = payload_length.to_be_bytes();

        stream.write_all(&length_bytes).await?;
        stream.write_all(payload).await?;

        println!("Producer sent message of {payload_length} bytes: {message}");
    }

    Ok(())
}
