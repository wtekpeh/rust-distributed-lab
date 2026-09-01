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
correspond to one TCP read. It did not.

The broker observed reads such as 22 bytes and 48 bytes despite the
producer performing five separate writes. The consumer also received
multiple logical messages merged together.

### Key lesson

TCP is a byte stream. TCP preserves byte ordering, but it does not
preserve application message boundaries.

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

A newline delimiter was introduced temporarily. The consumer used a
buffered reader and `read_line()` to reconstruct messages.

This worked but introduced an ambiguity: what happens if the message
payload itself contains a newline?

Therefore newline framing was replaced.

### Length-prefixed framing

The protocol was changed to:

``` text
[length][payload]
```

using a 4-byte unsigned integer.

The receiver first reads exactly four bytes with `read_exact`,
reconstructs the payload length with `u32::from_be_bytes`, and finally
reads exactly that many payload bytes.

### Lessons learned

Framing answers:

> Where does one message end and the next message begin?

TCP itself does not answer that question.

------------------------------------------------------------------------

# V3 --- Structured Messages and Serialization

**Status: ✅ Complete**

Messages were changed from plain strings to structured Rust data:

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

Framing answers:

> Where does this message end?

Serialization answers:

> What do these bytes mean?

They are separate layers.

### Producer and consumer types

The producer and consumer each define their own Rust `Message`
structure. They do not share the same in-memory object.

Their compatibility comes from agreeing on the wire format.

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

The broker is framing-aware but schema-unaware: it understands message
boundaries but does not need to understand the JSON fields.

## Backpressure Experiment

To make backpressure visible, the broker's consumer-side task was
deliberately slowed by two seconds before draining queue entries.

The broker reached:

``` text
Broker attempting to queue message...
```

and paused before:

``` text
Broker queued one message.
```

This showed that `message_sender.send(message).await` was suspended
because the bounded queue had reached capacity.

``` text
slow queue consumer
        ↓
bounded queue fills
        ↓
send().await waits
        ↓
producer-handling task stops progressing
```

## End-to-End TCP Backpressure Experiment

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

Backpressure does not necessarily appear immediately at the producer
because multiple buffering layers exist between the application
processes.

An unbounded queue can instead allow memory usage to grow without limit.
A bounded queue forces overload to propagate upstream.

> I have finite capacity. If downstream cannot keep up, upstream must
> slow down.

------------------------------------------------------------------------

# V5 --- Multiple Producers

**Status: ✅ Complete**

The broker was changed from accepting only one producer connection to
continuously accepting new producer connections.

Previously, one accepted producer occupied the broker's
producer-handling flow:

``` text
accept producer
      ↓
handle producer
      ↓
producer disconnects
```

The V5 broker instead continuously accepts producers and gives each
connection its own Tokio task:

``` text
accept Producer A
      ↓
spawn handler A

accept Producer B
      ↓
spawn handler B

accept Producer C
      ↓
spawn handler C
```

The producer-specific socket handling was moved into
`handle_producer()`.

The consumer-side queue draining was also separated into
`handle_consumer()`, making the broker responsibilities clearer.

## Shared bounded queue

Each producer task receives a clone of the same `mpsc::Sender<Vec<u8>>`:

``` rust
let producer_message_sender = message_sender.clone();
```

Cloning the sender does **not** create another queue.

``` text
Producer A task ─┐
                 │
Producer B task ─┼──► ONE bounded MPSC queue ──► Consumer task
                 │
Producer C task ─┘
```

All producers therefore compete for the same finite queue capacity and
retain the V4 backpressure behaviour.

## Multi-producer experiment

Two producer processes were started almost simultaneously.

The broker accepted both TCP connections, with separate ephemeral ports:

``` text
Producer connected from 127.0.0.1:59778
Producer connected from 127.0.0.1:59792
```

The logs then showed messages from the two producer handlers
interleaving while both fed the shared queue.

This demonstrated that the broker was no longer processing producer
connections sequentially.

## Lessons learned

`TcpListener::accept()` accepts a connection, but does not itself create
concurrent connection handling.

Concurrency was introduced explicitly with `tokio::spawn()`.

Each producer has:

-   its own TCP connection;
-   its own `TcpStream`;
-   its own Tokio handler task;
-   its own framing state.

The producer tasks share access to one bounded broker queue through
cloned sender handles.

### Ordering with multiple producers

TCP preserves ordering within an individual connection.

If Producer A sends:

``` text
A1 → A2 → A3
```

the broker reads A1 before A2 before A3 from that connection.

Likewise, Producer B can independently send:

``` text
B1 → B2 → B3
```

However, there is no automatic global ordering between independent
producers.

A broker may therefore observe:

``` text
A1
B1
B2
A2
A3
B3
```

depending on arrival timing and task scheduling.

This introduces an important distributed-systems distinction:

> Per-connection ordering does not automatically provide global ordering
> across independent producers.

### Message identity observation

Both test producer processes currently generate message IDs `1` through
`5`.

This means IDs that are unique within one producer are not necessarily
globally unique once multiple independent producers exist.

We will address stable message identity and idempotency in a later
stage.

------------------------------------------------------------------------


# V6 --- Multiple Consumers and Competing Consumption

**Status: ✅ Complete**

V5 allowed many producers to feed one bounded broker queue, but the
consumer side still had an important limitation:

``` text
Producer A ──┐
             │
Producer B ──┼──► Bounded Queue ──► ONE Consumer
             │
Producer C ──┘
```

The broker accepted only one consumer connection, and the queue used a
Tokio `mpsc::Receiver<Vec<u8>>`.

## Why multiple consumers required a new design

In V5, supporting multiple producers was straightforward because
`mpsc::Sender` can be cloned:

``` rust
let producer_message_sender = message_sender.clone();
```

Every producer task could therefore own a sender while all cloned
senders still fed the same underlying bounded queue.

The receiving side is deliberately different. Tokio's `mpsc` is a
**multiple-producer, single-consumer** channel. Its `Receiver` is not
cloneable.

Conceptually:

``` text
Sender A ──┐
Sender B ──┼──► ONE queue ──► ONE Receiver
Sender C ──┘
```

Simply creating several independent receivers would also be wrong for
the behaviour we wanted, because the consumers need to compete for
messages from the same queue rather than receive messages from separate
queues.

## Sharing the single receiver

The receiver was therefore wrapped in:

``` rust
Arc<Mutex<mpsc::Receiver<Vec<u8>>>>
```

`Arc` allows multiple consumer tasks to hold references to the same
underlying receiver. `Mutex` coordinates access so only one consumer
task manipulates that receiver at a time.

Each consumer task performs the queue receive inside a limited scope:

``` rust
let message_buffer = {
    let mut receiver = message_receiver.lock().await;

    receiver.recv().await
};
```

Once a message has been removed from the queue, the mutex guard is
dropped before the consumer task performs its TCP writes. The queue is
therefore not kept locked while a consumer is sending data over the
network.

## Continuously accepting consumers

The previous broker called `consumer_listener.accept()` only once.
V6 moved consumer acceptance into its own asynchronous loop:

``` text
consumer accept loop
      │
      ├── accept Consumer A → spawn handler A
      ├── accept Consumer B → spawn handler B
      └── accept Consumer C → spawn handler C
```

The producer accept loop continues independently:

``` text
producer accept loop
      │
      ├── accept Producer A → spawn handler A
      ├── accept Producer B → spawn handler B
      └── accept Producer C → spawn handler C
```

The broker can therefore accept new producers and new consumers without
one accept loop preventing the other from progressing.

## Competing-consumer experiment

The V6 test used one broker, two consumers, and one producer. The
producer sent five messages.

The first consumer received:

``` text
Message 1
Message 3
Message 5
```

The second consumer received:

``` text
Message 2
Message 4
```

The broker logs confirmed two independent consumer TCP connections:

``` text
Consumer connected from 127.0.0.1:40656
Consumer connected from 127.0.0.1:38918
```

The observed distribution was:

``` text
                 ┌──► Consumer 40656: M1, M3, M5
Bounded Queue ───┤
                 └──► Consumer 38918: M2, M4
```

No round-robin algorithm was explicitly implemented. The consumer tasks
compete for access to the shared receiver. The alternating distribution
observed in this run is therefore an observed scheduling outcome, not a
guaranteed ordering rule.

## Competing consumers are not broadcast

V6 implements work-sharing semantics. A queued message is removed once
and delivered to one competing consumer.

This differs from broadcast/pub-sub, where each subscriber would receive
its own copy. Pub/sub will be introduced separately in a later stage.

## New reliability problem exposed by V6

Multiple consumers improve work distribution, but they expose an
important reliability problem.

The current queue removes a message when a consumer task receives it:

``` text
queue contains M3
      ↓
consumer task receives M3
      ↓
M3 is removed from queue
      ↓
broker sends M3 over TCP
      ↓
consumer processes M3
```

Now consider a failure:

``` text
queue removes M3
      ↓
broker sends M3
      ↓
consumer crashes before safely processing it
      ↓
broker has no confirmation
      ↓
M3 is lost
```

The broker cannot currently distinguish successful processing from a
consumer that received a message and then failed, because the consumer
sends no confirmation back.

This problem motivates the next stage:
**V7 --- Acknowledgements**.

### Lessons learned

Multiple producers and multiple consumers are not symmetrical when using
Tokio MPSC. Senders are cloneable, while the receiver has a single
owner.

`Arc` provides shared ownership of the receiver reference, while `Mutex`
provides coordinated mutable access to that single receiver.

A continuously running accept loop plus `tokio::spawn()` allows the
broker to service multiple independent TCP connections concurrently.

Competing consumers divide work from one queue; they do not broadcast
every message to every consumer.

Observed fair-looking distribution is not the same as a guaranteed
round-robin policy.

Most importantly, removing a message from an in-memory queue is not the
same thing as proving that a remote consumer successfully processed it.
That distinction leads directly to acknowledgements.

------------------------------------------------------------------------

# Current Architecture

After completing V6:

``` text
Producer A ──► handler task A ──┐
                                │
Producer B ──► handler task B ──┼──► Bounded MPSC Queue
                                │           │
Producer C ──► handler task C ──┘           │
                                            ▼
                                      Shared Receiver
                                 Arc<Mutex<Receiver<_>>>
                                      │           │
                                      ▼           ▼
                                  Consumer A   Consumer B
                                  handler      handler
                                   task         task
```

Current characteristics:

-   multiple producer connections
-   one Tokio task per producer connection
-   multiple consumer connections
-   one Tokio task per consumer connection
-   competing-consumer work distribution
-   one shared bounded in-memory queue
-   coordinated access to the single Tokio MPSC receiver
-   length-prefixed framing
-   JSON serialization
-   backpressure
-   per-connection TCP ordering
-   no guaranteed global ordering across producers
-   no guaranteed round-robin distribution across consumers
-   no persistence
-   no acknowledgements
-   no retry mechanism


------------------------------------------------------------------------

# Planned Stages

  Version   Capability                              Status
  --------- --------------------------------------- --------
  V1        Basic TCP message delivery              ✅
  V2        Multiple messages and framing           ✅
  V3        Structured messages and serialization   ✅
  V4        Bounded queue and backpressure          ✅
  V5        Multiple producers                      ✅
  V6        Multiple consumers                      ✅
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
