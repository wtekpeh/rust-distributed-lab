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

Messages now travel over TCP using a broker envelope followed by the
serialized application payload:

``` text
┌───────────────────┬──────────────────────┬─────────────────────────────┐
│ 8-byte message ID │ 4-byte payload length│ serialized message payload  │
└───────────────────┴──────────────────────┴─────────────────────────────┘
```

The broker message ID is a big-endian `u64`. The payload length is a
big-endian `u32`. The payload is currently JSON.

The broker therefore understands the message identity and framing
metadata without needing to deserialize the application payload.

Example application payload:

``` json
{
  "id": 1,
  "payload": "Message One"
}
```

The application-level JSON still temporarily contains its own `id`.
During V7 the broker envelope ID and application ID were deliberately
kept separate so that broker-level delivery identity could be introduced
without simultaneously redesigning the application schema. Stable,
globally useful identity is deferred to V9.

Consumer acknowledgements travel in the reverse direction on the same
full-duplex TCP connection:

``` text
┌───────────────────┬──────────────────────┐
│ 1-byte ACK marker │ 8-byte message ID    │
└───────────────────┴──────────────────────┘
```

The valid ACK marker is currently `1`. The message ID allows the broker
to correlate the acknowledgement with the message it has in flight.

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

The previous broker called `consumer_listener.accept()` only once. V6
moved consumer acceptance into its own asynchronous loop:

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

This problem motivates the next stage: **V7 --- Acknowledgements**.

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

# V7 --- Application-Level Acknowledgements

**Status: ✅ Complete**

V6 exposed a reliability gap: removing a message from the queue only
proved that a broker task had obtained it. It did not prove that the
remote consumer had successfully received and processed it.

V7 introduced an application-level acknowledgement from consumer to
broker.

The delivery sequence became:

``` text
Broker                                  Consumer
  │                                        │
  │ message ID + length + payload          │
  ├───────────────────────────────────────►│
  │                                        │
  │                         reconstruct frame
  │                         deserialize payload
  │                         process message
  │                                        │
  │          ACK marker + message ID       │
  │◄───────────────────────────────────────┤
  │                                        │
```

This ACK is different from TCP's own transport-level acknowledgement.
TCP can confirm that bytes were transported through the connection, but
the broker needs application-level evidence that the consumer reached
the point in its processing at which the message can be considered
complete.

## Broker message envelope

Before V7, the queue stored only opaque JSON bytes. That created a
problem for acknowledgement correlation: the broker would have needed to
deserialize business JSON merely to discover the message ID.

The wire protocol was therefore changed from:

``` text
[length][payload]
```

to:

``` text
[broker message ID][payload length][payload]
```

and the broker queue now stores:

``` rust
struct BrokerMessage {
    id: u64,
    payload: Vec<u8>,
}
```

This keeps broker metadata outside the application payload. The broker
remains schema-unaware while still knowing the identity of the delivery
it is managing.

## One in-flight message per consumer

The current consumer handler sends one message and then waits for that
message's ACK before taking another message for the same consumer.

``` text
dequeue M1
   ↓
send M1
   ↓
wait for ACK M1
   ↓
ACK received
   ↓
dequeue next message
```

This intentionally keeps acknowledgement correlation simple at this
stage.

## Failure experiment

The consumer was deliberately terminated after receiving message 3 but
before sending its ACK.

The broker had already removed message 3 from the queue and was waiting
for acknowledgement. The TCP connection closed and the broker observed
`early eof`.

Messages 4 and 5 remained queued, but message 3 was lost because V7 had
not yet implemented a retry path.

This experiment established the distinction between:

``` text
QUEUED
IN_FLIGHT
ACKED
```

and directly motivated V8.

### Lessons learned

A successful socket write is not proof of successful remote application
processing.

Acknowledgements must be part of the application protocol when the
broker needs evidence of consumer completion.

The point at which a real consumer sends an ACK matters. If it
acknowledges before performing durable or business-critical work, a
later consumer failure can still lose the work.

Broker-level metadata should not require the broker to understand the
business payload.

------------------------------------------------------------------------

# V8 --- Failure Detection, Retry and Requeue

**Status: ✅ Complete**

V7 allowed the broker to know when a consumer acknowledged a message,
but a failed in-flight delivery could still lose the message.

V8 introduced a requeue path.

The broker's message state can now be viewed as:

``` text
QUEUED
  ↓
IN_FLIGHT
  ↓        ↘
ACKED      FAILED
             ↓
           QUEUED
```

If delivery succeeds and the expected ACK arrives, the message is
complete. If delivery fails before a valid ACK is accepted, the
`BrokerMessage` is returned to the bounded queue.

## Centralized delivery boundary

The complete send-and-acknowledge exchange was extracted into:

``` rust
deliver_and_wait_for_ack(...)
```

This function owns the delivery protocol for one in-flight message:

``` text
write broker message ID
write payload length
write payload
wait for ACK marker
validate ACK marker
wait for ACK message ID
validate ACK message ID
```

It returns `Ok(())` only when the expected acknowledgement has been
received.

Any delivery error becomes one `Err` path in `handle_consumer()`:

``` text
deliver_and_wait_for_ack()
          ↓
       Result
       /    \
     Ok      Err
              ↓
           requeue
```

This avoids scattering retry decisions across individual socket
operations.

## Retry experiment: consumer disconnect

The consumer was deliberately stopped after receiving message 3 but
before acknowledging it.

The broker observed:

``` text
Delivery of message 3 ... failed: early eof
Requeueing message 3.
```

A later consumer could receive the requeued message.

Because requeueing currently places the failed message at the tail,
messages that were already queued can be delivered before the retry. The
experiment therefore demonstrated that retry can change observed
delivery order.

It also demonstrated a future poison-message problem: when the same
deterministic failure was left in the consumer, every consumer that
received message 3 failed again. Retry limits and dead-letter handling
are intentionally deferred to later stages.

## ACK timeout

A consumer does not have to disconnect in order to fail.

It can remain connected while becoming stuck and never send an ACK.
Without a deadline, the broker would wait forever and the message would
remain in flight indefinitely.

V8 therefore introduced an application-level ACK timeout using Tokio's
`timeout()` and a three-second experimental deadline.

The nested result represents two different failure layers:

``` text
Ok(Ok(...))       ACK read completed successfully
Ok(Err(io_error)) TCP read completed with an I/O failure
Err(elapsed)      operation did not complete before the deadline
```

Timeouts were applied to both the ACK marker read and the ACK message ID
read.

The experiment deliberately kept the consumer connection alive for ten
seconds without acknowledging message 3. The broker independently
detected the missing acknowledgement after three seconds:

``` text
Delivery of message 3 ... failed:
Timed out waiting for ACK marker ...
Requeueing message 3.
```

The broker then closed that consumer handler's connection. This proved
that failure detection no longer depends solely on the remote process
terminating.

## Invalid-ACK experiment

The consumer was temporarily changed to send ACK marker `2` for message
3 instead of the valid marker `1`.

The consumer locally reported that it had written its acknowledgement,
but the broker rejected the protocol message:

``` text
Consumer ... sent invalid ACK marker 2
Requeueing message 3.
```

This demonstrated an important distributed distinction:

``` text
consumer wrote ACK bytes
        ≠
broker accepted the ACK
```

A live TCP connection is not sufficient. The peer must also obey the
application protocol.

## Final regression run

After removing all artificial failures, messages 1 through 5 were sent,
received and acknowledged successfully in order.

This confirmed that the V8 reliability machinery does not disturb the
normal successful delivery path.

## Delivery semantics after V8

V8 moves the in-memory broker toward **at-least-once delivery within the
lifetime of the broker process**.

That qualification is important. The queue is still in memory, so a
broker process crash can lose queued and in-flight state. Durable
recovery is deferred to V10 persistence.

Retries also introduce the possibility of duplicate processing. A
consumer might successfully perform its work and then lose the
connection before the broker receives the ACK. The broker cannot know
whether the work happened; it can only know that proof of completion did
not arrive before the delivery failed or timed out.

The safe response is to retry, which means the consumer may see the same
logical message again.

That uncertainty motivates V9: stable message identity and idempotent
consumer processing.

### Lessons learned

Failure detection needs more than noticing closed TCP connections.

Remote operations need bounded waiting; a healthy-looking connection can
still contain a stuck peer.

A timeout is an application policy about how long the broker is willing
to wait for proof of completion.

Centralizing the send/ACK exchange gives all delivery failures one
consistent retry path.

Retries improve reliability but can reorder messages and create
duplicates.

At-least-once delivery and idempotent processing are therefore closely
related.

------------------------------------------------------------------------

# Current Architecture

After completing V8:

``` text
Producer A ──► handler task A ──┐
                                │
Producer B ──► handler task B ──┼──► Bounded MPSC Queue
                                │       BrokerMessage
Producer C ──► handler task C ──┘           │
                                            ▼
                                      Shared Receiver
                                 Arc<Mutex<Receiver<_>>>
                                      │           │
                                      ▼           ▼
                                  Consumer A   Consumer B
                                  handler      handler
                                      │           │
                                      ▼           ▼
                                send one message + wait
                                for application ACK
                                      │
                           ┌──────────┴──────────┐
                           ▼                     ▼
                         ACK                   failure
                           │                     │
                        complete              requeue
```

Current characteristics:

-   multiple producer connections
-   one Tokio task per producer connection
-   multiple consumer connections
-   one Tokio task per consumer connection
-   competing-consumer work distribution
-   one shared bounded in-memory queue
-   `BrokerMessage { id, payload }` broker envelope
-   broker-level message identity on the wire
-   JSON application payload remains opaque to the broker
-   application-level consumer acknowledgements
-   one in-flight message per consumer connection
-   ACK marker and ACK message-ID validation
-   ACK deadlines
-   requeue after delivery failure
-   retry-induced reordering is possible
-   duplicate delivery is possible
-   backpressure
-   per-connection TCP ordering
-   no guaranteed global ordering across producers
-   no guaranteed round-robin distribution across consumers
-   no durable persistence
-   no globally stable message identity yet
-   no consumer idempotency yet
-   no retry limit or dead-letter queue yet

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
  V7        Acknowledgements                        ✅
  V8        Failure and retry                       ✅
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
