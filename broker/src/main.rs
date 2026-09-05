use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
struct BrokerMessage {
    id: u64,
    payload: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let producer_listener = TcpListener::bind("127.0.0.1:7000").await?;
    let consumer_listener = TcpListener::bind("127.0.0.1:7001").await?;

    println!("Broker listening for producers on 127.0.0.1:7000");
    println!("Broker listening for consumers on 127.0.0.1:7001");

    let (message_sender, message_receiver) = mpsc::channel::<BrokerMessage>(3);

    let shared_message_receiver = Arc::new(Mutex::new(message_receiver));

    let consumer_message_receiver = Arc::clone(&shared_message_receiver);

    let consumer_message_sender = message_sender.clone();

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

            let consumer_sender = consumer_message_sender.clone();

            tokio::spawn(async move {
                let result = handle_consumer(
                    consumer_stream,
                    consumer_address,
                    consumer_receiver,
                    consumer_sender,
                )
                .await;

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
    message_receiver: Arc<Mutex<mpsc::Receiver<BrokerMessage>>>,
    message_sender: mpsc::Sender<BrokerMessage>,
) -> Result<(), std::io::Error> {
    loop {
        let broker_message = {
            let mut receiver = message_receiver.lock().await;

            receiver.recv().await
        };

        let Some(broker_message) = broker_message else {
            break;
        };

        let message_id_bytes = broker_message.id.to_be_bytes();

        let message_length = broker_message.payload.len() as u32;

        let length_bytes = message_length.to_be_bytes();

        consumer_stream.write_all(&message_id_bytes).await?;

        consumer_stream.write_all(&length_bytes).await?;

        consumer_stream.write_all(&broker_message.payload).await?;

        println!(
            "Broker sent message {} with {} payload bytes \
     to consumer {}.",
            broker_message.id, message_length, consumer_address
        );

        let mut ack_marker_buffer = [0_u8; 1];

        if let Err(error) = consumer_stream.read_exact(&mut ack_marker_buffer).await {
            println!(
                "Consumer {consumer_address} disconnected before \
         acknowledging message {}.",
                broker_message.id
            );

            println!("Requeueing message {}.", broker_message.id);

            message_sender
                .send(broker_message)
                .await
                .map_err(|send_error| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        format!(
                            "Failed to requeue message after consumer failure: \
                     {send_error}"
                        ),
                    )
                })?;

            return Err(error);
        }

        let ack_marker = ack_marker_buffer[0];

        if ack_marker != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Consumer {consumer_address} sent invalid ACK marker {ack_marker}"),
            ));
        }

        let mut ack_message_id_buffer = [0_u8; 8];

        consumer_stream
            .read_exact(&mut ack_message_id_buffer)
            .await?;

        let ack_message_id = u64::from_be_bytes(ack_message_id_buffer);

        if ack_message_id != broker_message.id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Consumer {consumer_address} acknowledged message \
             {ack_message_id}, but broker was waiting for \
             message {}",
                    broker_message.id
                ),
            ));
        }

        println!(
            "Broker received ACK for message {} \
     from consumer {}.",
            ack_message_id, consumer_address
        );
    }

    Ok(())
}

async fn handle_producer(
    mut producer_stream: TcpStream,
    producer_address: std::net::SocketAddr,
    message_sender: mpsc::Sender<BrokerMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let mut message_id_buffer = [0_u8; 8];

        match producer_stream.read_exact(&mut message_id_buffer).await {
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

        let message_id = u64::from_be_bytes(message_id_buffer);

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
            "Broker received message {} with {} payload bytes \
     from producer {}.",
            message_id, message_length, producer_address
        );

        println!(
            "Producer {producer_address} \
     attempting to queue message {message_id}..."
        );

        let broker_message = BrokerMessage {
            id: message_id,
            payload: message_buffer,
        };

        message_sender.send(broker_message).await?;

        println!(
            "Producer {producer_address} \
     queued message {message_id}."
        );
    }

    Ok(())
}
