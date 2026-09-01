use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let producer_listener = TcpListener::bind("127.0.0.1:7000").await?;
    let consumer_listener = TcpListener::bind("127.0.0.1:7001").await?;

    println!("Broker listening for producers on 127.0.0.1:7000");
    println!("Broker listening for consumers on 127.0.0.1:7001");

    let (message_sender, message_receiver) = mpsc::channel::<Vec<u8>>(3);

    let shared_message_receiver = Arc::new(Mutex::new(message_receiver));

    let consumer_message_receiver = Arc::clone(&shared_message_receiver);

    tokio::spawn(async move {
        loop {
            println!("Waiting for consumer...");

            let result = consumer_listener.accept().await;

            let (consumer_stream, consumer_address) = match result {
                Ok(connection) => connection,

                Err(error) => {
                    eprintln!("Failed to accept consumer connection: {error}");

                    continue;
                }
            };

            println!("Consumer connected from {consumer_address}");

            let consumer_receiver = Arc::clone(&consumer_message_receiver);

            tokio::spawn(async move {
                let result =
                    handle_consumer(consumer_stream, consumer_address, consumer_receiver).await;

                if let Err(error) = result {
                    eprintln!("Consumer {consumer_address} handler failed: {error}");
                }
            });
        }
    });

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
    consumer_address: std::net::SocketAddr,
    message_receiver: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
) -> Result<(), std::io::Error> {
    loop {
        let message_buffer = {
            let mut receiver = message_receiver.lock().await;

            receiver.recv().await
        };

        let Some(message_buffer) = message_buffer else {
            break;
        };

        let message_length = message_buffer.len() as u32;

        let length_bytes = message_length.to_be_bytes();

        consumer_stream.write_all(&length_bytes).await?;

        consumer_stream.write_all(&message_buffer).await?;

        println!(
            "Broker sent one queued message of \
             {message_length} bytes to consumer \
             {consumer_address}."
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
