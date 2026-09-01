use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let producer_listener = TcpListener::bind("127.0.0.1:7000").await?;
    let consumer_listener = TcpListener::bind("127.0.0.1:7001").await?;

    println!("Broker listening for producers on 127.0.0.1:7000");
    println!("Broker listening for consumer on 127.0.0.1:7001");

    println!("Waiting for consumer...");

    let (consumer_stream, consumer_address) = consumer_listener.accept().await?;

    println!("Consumer connected from {consumer_address}");

    let (message_sender, message_receiver) = mpsc::channel::<Vec<u8>>(3);

    tokio::spawn(handle_consumer(consumer_stream, message_receiver));

    loop {
        println!("Waiting for producer...");

        let (producer_stream, producer_address) = producer_listener.accept().await?;

        println!("Producer connected from {producer_address}");

        let producer_message_sender = message_sender.clone();

        tokio::spawn(async move {
            let result =
                handle_producer(producer_stream, producer_address, producer_message_sender).await;

            if let Err(error) = result {
                eprintln!("Producer {producer_address} handler failed: {error}");
            }
        });
    }
}

async fn handle_consumer(
    mut consumer_stream: TcpStream,
    mut message_receiver: mpsc::Receiver<Vec<u8>>,
) -> Result<(), std::io::Error> {
    while let Some(message_buffer) = message_receiver.recv().await {
        let message_length = message_buffer.len() as u32;

        let length_bytes = message_length.to_be_bytes();

        consumer_stream.write_all(&length_bytes).await?;

        consumer_stream.write_all(&message_buffer).await?;

        println!(
            "Broker sent one queued message of \
             {message_length} bytes to consumer."
        );
    }

    Ok(())
}

async fn handle_producer(
    mut producer_stream: TcpStream,
    producer_address: std::net::SocketAddr,
    message_sender: mpsc::Sender<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let mut length_buffer = [0_u8; 4];

        match producer_stream.read_exact(&mut length_buffer).await {
            Ok(_) => {}

            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!(
                    "Producer {producer_address} \
                     closed the connection."
                );

                break;
            }

            Err(error) => {
                return Err(error.into());
            }
        }

        let message_length = u32::from_be_bytes(length_buffer) as usize;

        let mut message_buffer = vec![0_u8; message_length];

        producer_stream.read_exact(&mut message_buffer).await?;

        println!(
            "Broker received one complete message of \
             {message_length} bytes from producer \
             {producer_address}."
        );

        println!(
            "Producer {producer_address} \
             attempting to queue message..."
        );

        message_sender.send(message_buffer).await?;

        println!(
            "Producer {producer_address} \
             queued one message."
        );
    }

    Ok(())
}
