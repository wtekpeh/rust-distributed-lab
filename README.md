# Rust Distributed Services Lab

A practical Rust learning project for understanding distributed systems
by building the mechanisms from first principles.

The goal is not just to use production brokers, but to first understand
the problems they solve by implementing a small broker and observing its
behaviour under normal and failure conditions.

The learning cycle used in this project is:

1.  Understand one distributed-systems concept.
2.  Implement the simplest working version.
3.  Run it.
4.  Observe the behaviour.
5.  Deliberately break or stress it.
6.  Understand the failure mode.
7.  Improve the design.
8.  Repeat.

------------------------------------------------------------------------

## Workspace

The project is a Cargo workspace containing three independent processes:

``` text
rust-distributed-lab/
├── broker/
├── producer/
├── consumer/
├── Cargo.toml
└── README.md
```

### Producer

Creates application messages, serializes them and sends them to the
broker over TCP.

### Broker

Accepts messages from producers and delivers them to consumers.

As the project progresses, the broker will gain capabilities such as
queues, multiple producers, multiple consumers, acknowledgements,
retries, persistence, pub/sub, consumer groups, and dead-letter queues.

### Consumer

Receives framed messages from the broker, deserializes them and
processes them.

------------------------------------------------------------------------

# Current Wire Protocol

Messages currently travel over TCP using a length-prefixed frame:

``` text
┌──────────────────────┬─────────────────────────────┐
│ 4-byte length prefix │ serialized message payload  │
└──────────────────────┴─────────────────────────────┘
```

The length prefix is a big-endian `u32`. The payload is currently JSON.

Example logical message:

``` json
{
  "id": 1,
  "payload": "Message One"
}
```

The producer serializes this structure using `serde_json`. The consumer
reconstructs the TCP frame and then deserializes the JSON back into its
own Rust `Message` structure.

------------------------------------------------------------------------

# Stage Progress

## V1 --- Basic TCP Producer → Broker → Consumer

**Status: ✅ Complete**

Initial architecture:

``` text
Producer
   │
   │ TCP :7000
   ▼
 Broker
   │
   │ TCP :7001
   ▼
Consumer
```

The broker listens on:

``` text
Producer port: 127.0.0.1:7000
Consumer port: 127.0.0.1:7001
```

The first implementation successfully sent `Hello from producer` through
the broker to the consumer.

### Lessons learned

The producer, broker and consumer are independent operating-system
processes.

A TCP connection does not mean the processes share memory or execution
state.

`TcpListener::accept()` creates a connected TCP stream. It does not
automatically create an application thread.

An observed client-side port such as `127.0.0.1:38788` is an ephemeral
port allocated by the operating system. The broker's ports remain fixed.

------------------------------------------------------------------------

## V2 --- Multiple Messages and Message Framing

**Status: ✅ Complete**

The producer was changed to send multiple messages over one TCP
connection.

Initially, the producer performed multiple `write_all()` calls:

``` text
Message One
Message Two
Message Three
Message Four
Message Five
```

The assumption was deliberately tested that one TCP write might
correspond to one TCP read.

It did not.

The broker observed reads such as 22 bytes and 48 bytes despite the
producer performing five separate writes. The consumer also received
multiple logical messages merged together.

### Key lesson

TCP is a byte stream.

TCP preserves byte ordering, but it does not preserve application
message boundaries.

This means:

``` text
write()
write()
write()
```

does not imply:

``` text
read()
read()
read()
```

A distributed application must define its own framing protocol.

### Temporary newline framing

A newline delimiter was introduced temporarily:

``` text
Message One\n
Message Two\n
```

The consumer used a buffered reader and `read_line()` to reconstruct
messages.

This worked but introduced an ambiguity: what happens if the message
payload itself contains a newline?

Therefore newline framing was replaced.

### Length-prefixed framing

The protocol was changed to:

``` text
[length][payload]
```

using a 4-byte unsigned integer.

The receiver first reads exactly four bytes:

``` rust
read_exact(&mut length_buffer)
```

then reconstructs the payload length:

``` rust
u32::from_be_bytes(length_buffer)
```

and finally reads exactly that many bytes.

### Lessons learned

Framing answers:

> Where does one message end and the next message begin?

TCP itself does not answer that question.

------------------------------------------------------------------------

# V3 --- Structured Messages and Serialization

**Status: ✅ Complete**

Messages were changed from plain strings to structured Rust data.

Example:

``` rust
struct Message {
    id: u64,
    payload: String,
}
```

The producer serializes the structure using Serde and `serde_json`
before applying length-prefix framing.

The consumer performs the reverse process:

``` text
TCP bytes
   ↓
frame reconstruction
   ↓
JSON bytes
   ↓
deserialization
   ↓
Rust Message
```

### Important distinction

Framing and serialization solve different problems.

Framing answers:

> Where does this message end?

Serialization answers:

> What do these bytes mean?

They are separate layers.

### Producer and consumer types

The producer and consumer each define their own Rust `Message`
structure. They do not share the same in-memory object.

Their compatibility comes from agreeing on the wire format.

This is the beginning of an important distributed-systems concept:

``` text
Schema = contract between independent services
```

------------------------------------------------------------------------

# V4 --- Bounded Broker Queue and Backpressure

**Status: ✅ Complete**

The original broker directly coupled producer reads with consumer
writes:

``` text
read producer message
        ↓
write consumer message
        ↓
read next producer message
```

A bounded Tokio MPSC queue was introduced:

``` rust
mpsc::channel::<Vec<u8>>(3)
```

The architecture became:

``` text
Producer TCP
     │
     ▼
Broker producer-side handler
     │
     ▼
Bounded Queue
 capacity = 3
     │
     ▼
Broker consumer-side task
     │
     ▼
Consumer TCP
```

The producer-facing side places complete message payloads into the
queue. The consumer-side task independently removes them and sends them
to the consumer.

The broker is currently framing-aware but schema-unaware. It understands
message boundaries but does not need to understand the JSON fields.

------------------------------------------------------------------------

# Backpressure Experiment

To make backpressure visible, the broker's consumer-side task was
deliberately slowed.

A two-second delay was temporarily added before draining queue entries.
The queue capacity remained three messages.

The producer attempted to send messages faster than the queue could
drain.

The broker reached:

``` text
Broker attempting to queue message...
```

and paused before printing:

``` text
Broker queued one message.
```

This showed that:

``` rust
message_sender.send(message).await
```

was suspended because the bounded queue had reached capacity.

### First backpressure chain

``` text
slow queue consumer
        ↓
bounded queue fills
        ↓
send().await waits
        ↓
producer-handling task stops progressing
```

------------------------------------------------------------------------

# End-to-End TCP Backpressure Experiment

A second experiment tested whether backpressure could propagate all the
way to the producer.

The producer temporarily sent approximately 100 messages of about 1 MB
each while the broker drained one queued message approximately every two
seconds.

Initially producer writes completed in roughly 200--500 microseconds
because the operating system's TCP buffers absorbed the data.

Around message 14, write latency suddenly increased:

``` text
message 14 ≈ 1.6 seconds
message 15 ≈ 2.0 seconds
message 17 ≈ 3.9 seconds
message 19 ≈ 3.9 seconds
message 21 ≈ 3.9 seconds
```

This demonstrated full backpressure propagation:

``` text
slow downstream
      ↓
broker queue fills
      ↓
broker send().await blocks
      ↓
broker stops reading producer TCP stream
      ↓
broker TCP receive buffer fills
      ↓
TCP flow control applies pressure
      ↓
producer TCP send buffer fills
      ↓
producer write_all().await blocks
```

### Important lesson

Backpressure does not necessarily appear immediately at the producer.

There are multiple buffering layers:

``` text
Producer application
       ↓
Producer kernel TCP buffer
       ↓
TCP connection
       ↓
Broker kernel TCP buffer
       ↓
Broker application
       ↓
Bounded queue
```

Small workloads can be absorbed by these buffers and make an overloaded
system temporarily appear healthy.

------------------------------------------------------------------------

## Why bounded queues matter

An unbounded queue can allow upstream producers to continue adding work
while downstream consumers cannot keep up.

For example, with 1 MB messages:

``` text
10 messages       ≈ 10 MB
100 messages      ≈ 100 MB
1,000 messages    ≈ 1 GB
10,000 messages   ≈ 10 GB
```

Eventually memory becomes the failure mechanism.

A bounded queue instead forces overload to propagate upstream.

The system effectively says:

> I have finite capacity. If downstream cannot keep up, upstream must
> slow down.

------------------------------------------------------------------------

# Current Architecture

After completing V4:

``` text
                 TCP
Producer ───────────────────► Broker
                              │
                              │ framing
                              ▼
                      ┌─────────────────┐
                      │ Bounded MPSC    │
                      │ Queue           │
                      │ Capacity: 3     │
                      └─────────────────┘
                              │
                              ▼
                         Sender Task
                              │
                              │ TCP
                              ▼
                           Consumer
```

Current characteristics:

-   one producer connection
-   one consumer connection
-   persistent TCP connection during a run
-   length-prefixed framing
-   JSON serialization
-   bounded in-memory queue
-   basic backpressure
-   no persistence
-   no acknowledgements
-   no retry mechanism
-   no multiple producers
-   no multiple consumers

------------------------------------------------------------------------

# Planned Stages

  Version   Capability                              Status
  --------- --------------------------------------- --------
  V1        Basic TCP message delivery              ✅
  V2        Multiple messages and framing           ✅
  V3        Structured messages and serialization   ✅
  V4        Bounded queue and backpressure          ✅
  V5        Multiple producers                      ⏳
  V6        Multiple consumers                      ⏳
  V7        Acknowledgements                        ⏳
  V8        Failure and retry                       ⏳
  V9        Stable message IDs and idempotency      ⏳
  V10       Persistence                             ⏳
  V11       Pub/Sub                                 ⏳
  V12       Consumer groups                         ⏳
  V13       Dead-letter queue                       ⏳
  V14       Graceful failure and recovery           ⏳

------------------------------------------------------------------------

# Future Broker Comparisons

After implementing these mechanisms manually, the same problems will be
explored using production technologies such as:

-   RabbitMQ
-   NATS
-   Kafka
-   MQTT
-   Redis Streams where appropriate

The purpose will be to understand not simply how to use each system, but
why its architecture exists and which distributed-systems problems it
solves.
