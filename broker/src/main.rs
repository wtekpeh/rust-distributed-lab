use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Debug)]
struct BrokerMessage {
    id: Uuid,
    payload: Vec<u8>,
}

#[derive(Debug, serde::Serialize)]
struct MessageLogRecord<'a> {
    #[serde(rename = "type")]
    record_type: &'a str,
    id: Uuid,

    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<&'a [u8]>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Broker starting...");

    let data_directory = "broker-data";

    create_dir_all(data_directory).await?;

    let log_path = format!("{data_directory}/messages.log");

    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await?;

    let shared_log_file = Arc::new(Mutex::new(log_file));

    println!("Broker persistence log: {log_path}");

    let producer_bind_address =
        env::var("BROKER_PRODUCER_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:7000".to_string());

    let consumer_bind_address =
        env::var("BROKER_CONSUMER_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:7001".to_string());

    let producer_listener = TcpListener::bind(&producer_bind_address).await?;
    let consumer_listener = TcpListener::bind(&consumer_bind_address).await?;

    println!("Broker listening for producers on {producer_bind_address}");
    println!("Broker listening for consumers on {consumer_bind_address}");

    let (message_sender, message_receiver) = mpsc::channel::<BrokerMessage>(3);

    let shared_message_receiver = Arc::new(Mutex::new(message_receiver));

    let consumer_message_receiver = Arc::clone(&shared_message_receiver);

    let consumer_message_sender = message_sender.clone();

    let consumer_log_file = Arc::clone(&shared_log_file);

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

            let consumer_log = Arc::clone(&consumer_log_file);

            tokio::spawn(async move {
                let result = handle_consumer(
                    consumer_stream,
                    consumer_address,
                    consumer_receiver,
                    consumer_sender,
                    consumer_log,
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

        let producer_log_file = Arc::clone(&shared_log_file);

        tokio::spawn(async move {
            let result = handle_producer(
                producer_stream,
                producer_address,
                producer_message_sender,
                producer_log_file,
            )
            .await;

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
    log_file: Arc<Mutex<tokio::fs::File>>,
) -> Result<(), std::io::Error> {
    loop {
        let broker_message = {
            let mut receiver = message_receiver.lock().await;

            receiver.recv().await
        };

        let Some(broker_message) = broker_message else {
            break;
        };

        let delivery_result =
            deliver_and_wait_for_ack(&mut consumer_stream, consumer_address, &broker_message).await;

        match delivery_result {
            Ok(()) => {
                let ack_record = MessageLogRecord {
                    record_type: "ACK",
                    id: broker_message.id,
                    payload: None,
                };

                let mut serialized_record = serde_json::to_vec(&ack_record)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

                serialized_record.push(b'\n');

                {
                    let mut file = log_file.lock().await;

                    file.write_all(&serialized_record).await?;
                }

                println!("Persisted ACK record for message {}.", broker_message.id);
            }

            Err(error) => {
                println!(
                    "Delivery of message {} to consumer {} failed: {}",
                    broker_message.id, consumer_address, error
                );

                println!("Requeueing message {}.", broker_message.id);

                message_sender
                    .send(broker_message)
                    .await
                    .map_err(|send_error| {
                        std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            format!(
                                "Failed to requeue message after delivery failure: \
                             {send_error}"
                            ),
                        )
                    })?;

                return Err(error);
            }
        }
    }

    Ok(())
}

async fn deliver_and_wait_for_ack(
    consumer_stream: &mut TcpStream,
    consumer_address: std::net::SocketAddr,
    broker_message: &BrokerMessage,
) -> Result<(), std::io::Error> {
    let message_id_bytes = broker_message.id.as_bytes();

    let message_length = broker_message.payload.len() as u32;

    let length_bytes = message_length.to_be_bytes();

    consumer_stream.write_all(message_id_bytes).await?;

    consumer_stream.write_all(&length_bytes).await?;

    consumer_stream.write_all(&broker_message.payload).await?;

    println!(
        "Broker sent message {} with {} payload bytes \
         to consumer {}.",
        broker_message.id, message_length, consumer_address
    );

    let mut ack_marker_buffer = [0_u8; 1];

    let ack_result = timeout(
        Duration::from_secs(3),
        consumer_stream.read_exact(&mut ack_marker_buffer),
    )
    .await;

    match ack_result {
        Ok(read_result) => {
            read_result?;
        }

        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "Timed out waiting for ACK marker from consumer \
                 {consumer_address} for message {}",
                    broker_message.id
                ),
            ));
        }
    }

    let ack_marker = ack_marker_buffer[0];

    if ack_marker != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Consumer {consumer_address} sent invalid ACK marker {ack_marker}"),
        ));
    }

    let mut ack_message_id_buffer = [0_u8; 16];

    let ack_id_result = timeout(
        Duration::from_secs(3),
        consumer_stream.read_exact(&mut ack_message_id_buffer),
    )
    .await;

    match ack_id_result {
        Ok(read_result) => {
            read_result?;
        }

        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "Timed out waiting for ACK message ID from consumer \
                 {consumer_address} for message {}",
                    broker_message.id
                ),
            ));
        }
    }

    let ack_message_id = Uuid::from_bytes(ack_message_id_buffer);

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

    Ok(())
}

async fn handle_producer(
    mut producer_stream: TcpStream,
    producer_address: std::net::SocketAddr,
    message_sender: mpsc::Sender<BrokerMessage>,
    log_file: Arc<Mutex<tokio::fs::File>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let mut message_id_buffer = [0_u8; 16];

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

        let message_id = Uuid::from_bytes(message_id_buffer);

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

        // Build the durable representation of this event.
        //
        // The broker still treats the payload as opaque bytes.
        // It only understands the broker-level message ID and
        // the fact that this is a MESSAGE journal record.
        let log_record = MessageLogRecord {
            record_type: "MESSAGE",
            id: broker_message.id,
            payload: Some(&broker_message.payload),
        };

        // Serialize one complete journal record.
        let mut serialized_record = serde_json::to_vec(&log_record)?;

        // JSON Lines format:
        // every journal record ends with '\n' so that records
        // can later be replayed one line at a time.
        serialized_record.push(b'\n');

        // Only one task may append to the shared log file at a time.
        {
            let mut file = log_file.lock().await;

            file.write_all(&serialized_record).await?;
        }

        println!(
            "Persisted MESSAGE record for message {}.",
            broker_message.id
        );

        // Only make the message available for delivery after
        // its MESSAGE record has been written to the journal.
        message_sender.send(broker_message).await?;

        println!(
            "Producer {producer_address} \
     queued message {message_id}."
        );
    }

    Ok(())
}
