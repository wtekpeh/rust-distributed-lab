use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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

    let (message_sender, mut message_receiver) = mpsc::channel::<Vec<u8>>(3);

    let consumer_task = tokio::spawn(async move {
        while let Some(message_buffer) = message_receiver.recv().await {
            let message_length = message_buffer.len() as u32;
            let length_bytes = message_length.to_be_bytes();

            consumer_stream.write_all(&length_bytes).await?;

            consumer_stream.write_all(&message_buffer).await?;

            println!("Broker sent one queued message of {message_length} bytes to consumer.");
        }

        Ok::<(), std::io::Error>(())
    });

    loop {
        let mut length_buffer = [0_u8; 4];

        match producer_stream.read_exact(&mut length_buffer).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Producer closed the connection.");
                break;
            }
            Err(error) => return Err(error.into()),
        }

        let message_length = u32::from_be_bytes(length_buffer) as usize;

        let mut message_buffer = vec![0_u8; message_length];

        producer_stream.read_exact(&mut message_buffer).await?;

        println!("Broker received one complete message of {message_length} bytes.");

        println!("Broker attempting to queue message...");

        message_sender.send(message_buffer).await?;

        println!("Broker queued one message.");
    }

    drop(message_sender);

    consumer_task.await??;

    Ok(())
}
