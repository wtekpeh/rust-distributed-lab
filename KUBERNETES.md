# Kubernetes Learning and Deployment Notes

This document records the Kubernetes learning and deployment work for the
`rust-distributed-lab` project.

The purpose is not to replace the distributed-systems work documented in
`README.md`. Instead, Kubernetes is being learned by deploying successive,
meaningful versions of the real Rust producer, broker, and consumer system.

Distributed-system concerns such as framing, message identity, acknowledgements,
retry, idempotency, ordering, queueing, persistence, replication, and delivery
semantics remain application-level concerns. Kubernetes does not automatically
solve them.

---

## 1. Project Relationship

The repository contains two parallel learning tracks:

```text
Distributed-services track
        |
        | develops application behaviour
        v
Producer -> Broker -> Consumer
        |
        | packaged and deployed by
        v
Kubernetes track
```

The distributed-services work continues independently. Kubernetes should reuse
successive versions of that real system rather than forcing application
architecture changes merely for orchestration.

The application should remain environment-agnostic. Runtime configuration
provides network addresses appropriate to the environment.

---

## 2. Core Kubernetes Mental Model

The hierarchy used throughout this project is:

```text
Cluster
  |
  +-- Node
       |
       +-- Pod
            |
            +-- Container
                 |
                 +-- Linux process
```

### Cluster

A Kubernetes cluster is the complete Kubernetes system: its Nodes plus the
control-plane components that manage workloads.

### Node

A Node is a physical or virtual machine belonging to the cluster and capable of
running Kubernetes workloads.

### Pod

A Pod is the smallest workload unit Kubernetes schedules and manages. A Pod
contains one or more closely related containers.

For this project, the normal mapping is deliberately simple:

```text
Broker Pod   -> Broker container   -> Rust broker process
Consumer Pod -> Consumer container -> Rust consumer process
```

Producer, broker, and consumer should not all be placed in one Pod because they
are independent services with different lifecycles and scaling requirements.

### Container and process

A container is not a separate virtual machine. The Rust program running inside a
container is still an ordinary Linux process using the Node's Linux kernel.

Linux namespaces provide isolated views of resources such as process IDs,
networking, and mounts. Control groups (cgroups) provide resource accounting and
limits such as CPU and memory.

A useful shorthand is:

```text
namespaces -> What can the process see?
cgroups    -> What resources can the process use?
```

---

## 3. Why Kubernetes Is Being Used

The producer, broker, and consumer can form a distributed system without
Kubernetes. They could simply run as processes on different machines and
communicate over TCP.

Kubernetes addresses a different class of problems, including:

- workload placement;
- desired-state management;
- process/container recovery;
- scaling;
- service discovery;
- stable networking abstractions;
- rolling deployment;
- rollback;
- resource management;
- coordinating workloads across machines.

The important separation is:

```text
Distributed system:
How should messages move and behave?

Kubernetes:
Where and how should the application processes run?
```

Kubernetes can restart a failed broker container, for example, but restarting an
in-memory broker does not make its queue durable. Persistence remains an
application/distributed-system concern.

---

## 4. Local Kubernetes Environment

The learning environment uses WSL2 Ubuntu with Docker Desktop integration.

Tools installed during the practical work include:

```text
Docker Desktop / Docker Engine
kubectl
kind
```

The Kubernetes cluster is a local `kind` cluster named:

```text
rust-distributed-lab
```

The cluster currently has one Node:

```text
rust-distributed-lab-control-plane
```

Although this Node has the control-plane role, the one-node development cluster
also runs application workloads.

The cluster was created using a Kubernetes v1.34 kind node image.

---

## 5. Control Plane Components Observed

System Pods inspected in the cluster included components such as:

```text
kube-apiserver
kube-controller-manager
kube-scheduler
etcd
CoreDNS
kube-proxy
kindnet
local-path-provisioner
```

### Scheduler

The scheduler chooses a suitable Node for a newly created Pod.

Scheduling occurs at the Pod level.

### Controller and reconciliation

Kubernetes works around desired state versus actual state.

For example:

```text
Desired replicas = 3
Actual Pods      = 2
        |
        v
controller detects difference
        |
        v
replacement Pod created
        |
        v
Actual Pods = 3
```

This continual effort to make actual state match declared desired state is
reconciliation.

---

## 6. Initial Nginx Experiments

Before deploying the Rust system, Nginx was used to make Kubernetes controller
behaviour visible without introducing application-specific complications.

A Deployment was created:

```bash
kubectl create deployment nginx-lab --image=nginx
```

This resulted conceptually in:

```text
Deployment
    |
    v
ReplicaSet
    |
    v
Pod
    |
    v
Nginx container/process
```

### Pod replacement

A Pod was deliberately deleted.

The ReplicaSet observed that the actual number of Pods had fallen below the
desired replica count and created a replacement.

The lesson was:

> Kubernetes did not resurrect the old Pod. It created a new Pod to restore the
> desired state.

Pods should therefore be treated as disposable identities.

### Scaling

The Nginx Deployment was scaled to three replicas.

This demonstrated:

```text
replicas = 3
     |
     v
three Pods
```

A replica normally means another Pod instance of the workload, not another
container placed into the same Pod.

### Rolling update

The Nginx image was later changed to `nginx:1.27`.

A new ReplicaSet was created from the changed Pod template while the old
ReplicaSet was scaled down.

This established the relationship:

```text
Deployment
    |
    +-- old ReplicaSet -> old Pod template
    |
    +-- new ReplicaSet -> new Pod template
```

A Deployment therefore manages rollout history above ReplicaSets.

---

## 7. Labels and Selectors

Labels are arbitrary key/value metadata attached to Kubernetes objects.

Example:

```yaml
labels:
  app: broker
```

A selector asks Kubernetes to find objects carrying matching labels.

The mental model used throughout the project is:

```text
LABEL    -> What am I?
SELECTOR -> What am I looking for?
```

The same label mechanism can serve different Kubernetes objects.

For example:

```text
Deployment/ReplicaSet selector:
Which Pods count as my replicas?

Service selector:
Which Pods should receive traffic for this Service?
```

These responsibilities are different even when both use:

```yaml
app: broker
```

---

## 8. Services and Disposable Pod IPs

Pods receive network identities, including Pod IP addresses, but Pods are
replaceable. A replacement Pod may have a different IP.

Applications should therefore not normally depend on another workload's current
Pod IP.

A Kubernetes Service provides a stable network abstraction for a logical set of
Pods.

The conceptual relationship is:

```text
Service name
    |
    | DNS
    v
Service
    |
    | selector / maintained endpoints
    v
current matching Pods
```

A Service is not the Rust broker process and should not be imagined as a
`service.exe` sitting between applications.

---

## 9. DNS and Service Discovery

DNS and Service selectors perform different jobs.

For a Service named:

```text
broker
```

a client in the same Kubernetes namespace can use a destination such as:

```text
broker:7000
```

Conceptually:

```text
NAME
 broker
   |
   | DNS
   v
SERVICE
   |
   | selector / endpoint tracking
   v
PODS
   |
   v
CONTAINERS
   |
   v
PROCESSES
```

DNS helps the client locate the Service by name.

The Service selector determines which Pods currently belong behind that Service.

---

## 10. EndpointSlice

Kubernetes maintains concrete backend information for Services rather than
scanning Pod labels for every packet.

EndpointSlice objects represent Service backends.

Conceptually:

```text
Service
   |
   | selector: app=broker
   v
matching Pod
   |
   | current Pod IP
   v
EndpointSlice
```

If the Broker Pod is replaced, the Service identity can remain stable while the
EndpointSlice is updated to point to the replacement backend.

---

## 11. Service Networking

A normal internal Service receives a virtual ClusterIP.

The ClusterIP is a stable Service-level network identity; it is distinct from a
Pod IP.

Traditional Kubernetes Service networking commonly involves `kube-proxy`
watching Services and endpoints and programming Node networking rules. The
Linux kernel then handles packet forwarding/translation according to those
rules.

The conceptual path is therefore not:

```text
Producer -> kube-proxy process -> Broker
```

Instead, kube-proxy commonly configures the networking datapath and the kernel
handles packets.

Modern Kubernetes networking implementations may use mechanisms such as eBPF
instead of the traditional kube-proxy datapath.

---

## 12. CNI and Pod Networking

CNI stands for Container Network Interface.

Kubernetes uses CNI-compatible networking software to configure Pod networking.

A Pod receives its own network namespace. A common Linux mechanism used to
connect that namespace to Node networking is a virtual Ethernet pair (`veth`).

A simplified path is:

```text
Rust process
    |
    v
socket
    |
    v
Pod network namespace
    |
    v
Pod eth0
    |
    v
veth
    |
    v
Node Linux networking
    |
    v
cluster Pod network
    |
    v
destination Pod
```

The exact implementation depends on the CNI implementation.

The important distinction is:

```text
network namespace -> gives the Pod an isolated network view
CNI               -> connects that network view to the cluster network
```

---

## 13. Containerizing the Real Rust Broker

After the Kubernetes fundamentals were explored with Nginx, the practical work
moved to the real Rust distributed-services application.

The repository includes:

```text
rust-distributed-lab/
├── broker/
├── producer/
├── consumer/
├── docker/
├── k8s/
├── Cargo.toml
├── Cargo.lock
├── README.md
└── KUBERNETES.md
```

The broker uses a multi-stage Docker build.

`docker/broker.Dockerfile`:

```dockerfile
FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p broker


FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/broker /usr/local/bin/broker

ENTRYPOINT ["/usr/local/bin/broker"]
```

The builder stage contains Rust, Cargo, source code, and build artifacts.

The runtime stage receives the compiled broker binary and avoids carrying the
compiler and complete source tree into the final runtime image.

The repository also uses `.dockerignore` entries such as:

```text
target/
**/target/
.git/
```

---

## 14. Image Versus Container

An important distinction established during containerization is:

```text
Rust source
    |
    | cargo build
    v
executable binary
    |
    | docker build
    v
container image
    |
    | run
    v
container
    |
    v
ordinary Linux process
```

A container image is a packaged blueprint. It is not running.

A container is a running isolated environment created from an image.

Kubernetes normally starts application containers from images; it does not run
`cargo run` against the project's source code.

---

## 15. Broker V1 and the Loopback Problem

The first broker image was built as:

```text
rust-distributed-broker:v1
```

The broker application still listened on:

```text
127.0.0.1:7000
127.0.0.1:7001
```

Publishing Docker ports alone did not change where the Rust process was
listening inside the container.

This established an important distinction:

```text
Application bind address
        !=
Docker host port publication
```

`TcpListener::bind()` creates the application listener.

Docker `-p` creates a host-to-container networking path.

Publishing a port cannot make an application listen on an interface on which it
did not bind.

Inspection of `/proc/net/tcp` confirmed that the V1 broker listeners were bound
to loopback.

---

## 16. Runtime-Configurable Broker Bind Addresses

The broker was changed so its server-side addresses could be supplied through
environment variables:

```text
BROKER_PRODUCER_BIND_ADDRESS
BROKER_CONSUMER_BIND_ADDRESS
```

The local defaults remain:

```text
127.0.0.1:7000
127.0.0.1:7001
```

The broker can therefore behave differently according to its runtime
environment without recompilation.

For container/Kubernetes operation, the values can be:

```text
0.0.0.0:7000
0.0.0.0:7001
```

`0.0.0.0` is a server bind address meaning all local IPv4 interfaces. It is not
an address a client should use as its destination.

The architecture became:

```text
Environment
    |
    v
std::env::var(...)
    |
    v
TcpListener::bind(...)
    |
    v
Linux listening socket
```

A new image was then built:

```text
rust-distributed-broker:v2
```

Both V1 and V2 can coexist because image tags identify different packaged
versions.

---

## 17. Testing Broker V2 with Docker

Broker V2 was run with environment variables equivalent to:

```text
BROKER_PRODUCER_BIND_ADDRESS=0.0.0.0:7000
BROKER_CONSUMER_BIND_ADDRESS=0.0.0.0:7001
```

and Docker port publication for ports 7000 and 7001.

The host-side Rust producer and consumer successfully communicated through the
containerized broker.

This demonstrated the separation:

```text
Docker -p
creates the network path

environment variable + Rust bind
makes the application listen at the destination
```

The distributed-system behaviour, including the bounded queue and ACK flow,
continued to operate through the containerized broker.

---

## 18. Docker Image Store Versus kind Image Store

The local Docker image store and the container runtime inside the kind Node are
separate.

Building:

```text
rust-distributed-broker:v2
```

with Docker did not automatically make that image available to Kubernetes
inside kind.

The image was explicitly loaded:

```bash
kind load docker-image rust-distributed-broker:v2 --name rust-distributed-lab
```

The Node's CRI/containerd image store was inspected using `crictl`.

This established another important distinction:

```text
Image exists on Docker host
        !=
Image exists on Kubernetes Node
        !=
Pod exists
        !=
Container is running
        !=
Rust process is running
```

---

## 19. Broker Deployment

The broker Deployment is defined in:

```text
k8s/broker-deployment.yaml
```

Current manifest:

```yaml
apiVersion: apps/v1
kind: Deployment

metadata:
  name: broker

spec:
  replicas: 1

  selector:
    matchLabels:
      app: broker

  template:
    metadata:
      labels:
        app: broker

    spec:
      containers:
        - name: broker
          image: rust-distributed-broker:v2

          ports:
            - name: producer
              containerPort: 7000

            - name: consumer
              containerPort: 7001

          env:
            - name: BROKER_PRODUCER_BIND_ADDRESS
              value: "0.0.0.0:7000"

            - name: BROKER_CONSUMER_BIND_ADDRESS
              value: "0.0.0.0:7001"
```

The Deployment was applied with:

```bash
kubectl apply -f k8s/broker-deployment.yaml
```

The observed Deployment reached:

```text
READY       1/1
UP-TO-DATE  1
AVAILABLE   1
```

---

## 20. Why the Broker Has One Replica

The broker currently contains one bounded in-memory Tokio MPSC queue.

Running several Broker Pods would create several independent broker processes
with independent memory:

```text
Broker Pod A -> Queue A
Broker Pod B -> Queue B
Broker Pod C -> Queue C
```

A Kubernetes Service could distribute connections among those Pods, but that
would not create replicated broker state, shared queues, consensus, leadership,
or correct distributed delivery semantics.

Therefore the current broker Deployment deliberately uses:

```yaml
replicas: 1
```

Scaling the broker is deferred until the distributed-services architecture
actually supports the required semantics.

This is an important example of the boundary between orchestration and
distributed-system design.

---

## 21. Broker ReplicaSet and Pod

Applying the Deployment automatically created a ReplicaSet.

Observed ReplicaSet:

```text
broker-6d4549c644
```

The ReplicaSet then created the Broker Pod:

```text
broker-6d4549c644-lkfvc
```

At the recorded checkpoint, the Pod had:

```text
Status:   Running
Ready:    1/1
Restarts: 0
Pod IP:   10.244.0.8
Node:     rust-distributed-lab-control-plane
```

The generated Pod name should not be used as application identity.

The Pod IP is also considered temporary.

---

## 22. Kubernetes Environment Variables Reaching Rust

The broker Deployment injects:

```yaml
env:
  - name: BROKER_PRODUCER_BIND_ADDRESS
    value: "0.0.0.0:7000"

  - name: BROKER_CONSUMER_BIND_ADDRESS
    value: "0.0.0.0:7001"
```

The running broker logs showed:

```text
Broker starting...
Broker listening for producers on 0.0.0.0:7000
Broker listening for consumers on 0.0.0.0:7001
Waiting for producer...
Waiting for consumer...
```

This proved the complete configuration path:

```text
Deployment YAML
      |
      v
Pod specification
      |
      v
container environment
      |
      v
Rust std::env::var()
      |
      v
TcpListener::bind()
      |
      v
Linux listening socket
```

---

## 23. `containerPort` Does Not Create a Listener

The Deployment declares:

```yaml
ports:
  - name: producer
    containerPort: 7000

  - name: consumer
    containerPort: 7001
```

`containerPort` does not cause Linux or Kubernetes to open those ports.

The Rust application still creates the actual listening sockets with
`TcpListener::bind()`.

The port declarations document/name the expected container ports and can be
referenced by other Kubernetes configuration.

In this project, the Service uses the named ports as its `targetPort` values.

---

## 24. Broker Service

The Broker Service is defined in:

```text
k8s/broker-service.yaml
```

Current manifest:

```yaml
apiVersion: v1
kind: Service

metadata:
  name: broker

spec:
  selector:
    app: broker

  ports:
    - name: producer
      port: 7000
      targetPort: producer

    - name: consumer
      port: 7001
      targetPort: consumer
```

The Service was applied with:

```bash
kubectl apply -f k8s/broker-service.yaml
```

Kubernetes created the Service successfully.

Because no explicit Service `type` was provided, the Service uses the default
internal `ClusterIP` type.

---

## 25. Service Port, Target Port, and Container Port

For the producer-facing connection:

```yaml
port: 7000
targetPort: producer
```

and the Broker Pod declares:

```yaml
name: producer
containerPort: 7000
```

Conceptually:

```text
broker:7000
    |
    v
Service port 7000
    |
    v
targetPort "producer"
    |
    v
Pod named port "producer"
    |
    v
container port 7000
    |
    v
Rust TcpListener on :7000
```

Similarly:

```text
broker:7001
    |
    v
Service port 7001
    |
    v
targetPort "consumer"
    |
    v
Pod named port "consumer"
    |
    v
container port 7001
    |
    v
Rust TcpListener on :7001
```

The protocol defaults to TCP, which matches the Rust application's
`TcpListener`/`TcpStream` protocol.

---

## 26. Observed Broker Service

The Service was inspected with:

```bash
kubectl get service broker
```

At the recorded checkpoint:

```text
NAME     TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)
broker   ClusterIP   10.96.149.118   <none>        7000/TCP,7001/TCP
```

This gives two distinct network identities:

```text
Service ClusterIP: 10.96.149.118
Current Pod IP:    10.244.0.8
```

The Service IP belongs to the stable Service abstraction.

The Pod IP belongs to the current Broker Pod.

Applications inside the cluster should ultimately use the Service identity,
such as `broker:7000`, rather than hardcoding either IP.

---

## 27. Observed Broker EndpointSlice

The Service's EndpointSlice was inspected with:

```bash
kubectl get endpointslices -l kubernetes.io/service-name=broker
```

Observed result:

```text
NAME           ADDRESSTYPE   PORTS       ENDPOINTS
broker-bjsf6   IPv4          7001,7000   10.244.0.8
```

A detailed inspection showed:

```text
Ports:
  Name      Port  Protocol
  consumer  7001  TCP
  producer  7000  TCP

Endpoints:
  - Addresses:  10.244.0.8
    Conditions:
      Ready:    true
    TargetRef:  Pod/broker-6d4549c644-lkfvc
    NodeName:   rust-distributed-lab-control-plane
```

This is direct evidence that Kubernetes translated:

```text
Service selector
app=broker
     |
     v
matching Broker Pod
     |
     v
Pod IP 10.244.0.8
     |
     v
EndpointSlice backend
```

The Service manifest itself never hardcoded `10.244.0.8`.

---

## 28. Current Broker Network Path

At the current checkpoint, the intended in-cluster producer path is:

```text
Producer Pod
     |
     | broker:7000
     v
DNS
     |
     v
Broker Service
ClusterIP 10.96.149.118
     |
     v
EndpointSlice
10.244.0.8:7000
     |
     v
Broker Pod
     |
     v
Rust TcpListener
```

The consumer path is equivalent through port 7001:

```text
Consumer Pod
     |
     | broker:7001
     v
Broker Service
     |
     v
EndpointSlice
10.244.0.8:7001
     |
     v
Broker Pod
     |
     v
Rust TcpListener
```

The producer and consumer are not yet running as Kubernetes workloads at this
checkpoint.

---

## 29. Runtime-Configurable Client Addresses

The producer and consumer originally contained hardcoded loopback destinations:

```text
Producer -> 127.0.0.1:7000
Consumer -> 127.0.0.1:7001
```

This is correct when all processes run locally, but it is incorrect when
producer, broker, and consumer run in separate Pods.

Inside a Producer Pod:

```text
127.0.0.1
```

refers to the Producer Pod's own network namespace, not the Broker Pod.

The clients were therefore made runtime-configurable.

### Producer

The producer now reads:

```text
BROKER_PRODUCER_ADDRESS
```

with fallback:

```text
127.0.0.1:7000
```

Conceptually:

```rust
let broker_address =
    env::var("BROKER_PRODUCER_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:7000".to_string());

let mut stream = TcpStream::connect(&broker_address).await?;
```

The change successfully passed:

```bash
cargo check -p producer
```

### Consumer

The consumer now reads:

```text
BROKER_CONSUMER_ADDRESS
```

with fallback:

```text
127.0.0.1:7001
```

The address is resolved once before the consumer's reconnect loop and reused for
subsequent connection attempts.

Conceptually:

```rust
let broker_address =
    env::var("BROKER_CONSUMER_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:7001".to_string());

loop {
    let mut stream = TcpStream::connect(&broker_address).await?;

    // existing consumer protocol...
}
```

The change successfully passed:

```bash
cargo check -p consumer
```

No ACK, framing, UUID, retry, or deduplication behaviour was changed.

---

## 30. Application Configuration Model

The complete network configuration is now:

```text
SERVER SIDE

Broker producer listener:
BROKER_PRODUCER_BIND_ADDRESS

Broker consumer listener:
BROKER_CONSUMER_BIND_ADDRESS


CLIENT SIDE

Producer destination:
BROKER_PRODUCER_ADDRESS

Consumer destination:
BROKER_CONSUMER_ADDRESS
```

Local execution can use the defaults:

```text
Broker bind producer: 127.0.0.1:7000
Broker bind consumer: 127.0.0.1:7001
Producer destination: 127.0.0.1:7000
Consumer destination: 127.0.0.1:7001
```

The Kubernetes environment will provide:

```text
Broker:
BROKER_PRODUCER_BIND_ADDRESS=0.0.0.0:7000
BROKER_CONSUMER_BIND_ADDRESS=0.0.0.0:7001

Producer:
BROKER_PRODUCER_ADDRESS=broker:7000

Consumer:
BROKER_CONSUMER_ADDRESS=broker:7001
```

This keeps Kubernetes-specific service discovery out of the Rust source code.

---

## 31. Current Kubernetes Architecture

The implemented portion currently looks like:

```text
                    Kubernetes Cluster

                +-----------------------+
                | Service: broker       |
                | ClusterIP             |
                | 10.96.149.118         |
                |                       |
                | :7000          :7001  |
                +-----------+-----------+
                            |
                       EndpointSlice
                            |
                       10.244.0.8
                            |
                            v
                +-----------------------+
                | Broker Pod            |
                |                       |
                | broker:v2             |
                |                       |
                | Rust broker process   |
                | :7000 / :7001         |
                +-----------------------+
```

The intended next architecture is:

```text
Producer workload
       |
       | broker:7000
       v
+----------------+
| Broker Service |
+----------------+
       |
       v
+----------------+
| Broker Pod     |
| Rust broker    |
+----------------+
       ^
       |
       | broker:7001
Consumer workload
```

---

## 32. Important Kubernetes Non-Goals

The experiments so far reinforce several boundaries.

Kubernetes does not automatically provide:

- durable broker queues;
- exactly-once delivery;
- application-level acknowledgements;
- idempotent message processing;
- ordering across independent producers;
- broker state replication;
- leader election for the custom broker;
- distributed consensus;
- poison-message handling;
- dead-letter queues;
- retry semantics;
- application backpressure.

Those remain distributed-system/application design concerns.

Likewise:

```text
3 Broker Pods
```

does not automatically mean:

```text
one correctly replicated three-node broker
```

Kubernetes can orchestrate replicas, but the application must define how those
replicas coordinate state.

---

## 33. Consumer Containerization

The next practical step was to package the real Rust consumer as a container image.

`docker/consumer.Dockerfile`:

```dockerfile
FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p consumer


FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/consumer /usr/local/bin/consumer

ENTRYPOINT ["/usr/local/bin/consumer"]
```

The image was built as:

```text
rust-distributed-consumer:v1
```

and then loaded into the kind Node:

```bash
kind load docker-image rust-distributed-consumer:v1 \
  --name rust-distributed-lab
```

Inspection with `crictl images` inside the kind Node confirmed that the image was
available to the Node's container runtime.

This reinforced the distinction:

```text
Image available on Node
        !=
Pod exists
        !=
container is running
        !=
Rust process is running
```

---

## 34. Consumer Deployment

The consumer is a long-running process. It connects to the broker, waits for
messages, processes them, acknowledges them, and continues waiting. Its lifecycle
therefore fits a Kubernetes Deployment.

`k8s/consumer-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment

metadata:
  name: consumer

spec:
  replicas: 1

  selector:
    matchLabels:
      app: consumer

  template:
    metadata:
      labels:
        app: consumer

    spec:
      containers:
        - name: consumer
          image: rust-distributed-consumer:v1

          env:
            - name: BROKER_CONSUMER_ADDRESS
              value: "broker:7001"
```

The Consumer Deployment does not declare a `ports` section because the consumer
does not listen for inbound application connections. It initiates an outbound TCP
connection to the Broker Service.

For the same reason, the current architecture does not require a Consumer Service.
No other workload needs a stable Kubernetes endpoint through which to initiate a
connection to the consumer.

The Deployment was applied with:

```bash
kubectl apply -f k8s/consumer-deployment.yaml
```

The observed Consumer Pod was:

```text
consumer-8c5c7b49b-mvcvh
```

with Pod IP:

```text
10.244.0.9
```

and status:

```text
Ready:    1/1
Status:   Running
Restarts: 0
```

---

## 35. First Real Pod-to-Service-to-Pod Connection

The Consumer logs showed:

```text
Consumer starting...
Consumer will connect to broker at broker:7001.
Consumer connecting to broker...
Consumer connected to broker.
Waiting for message...
```

The Broker logs independently showed:

```text
Consumer connected from 10.244.0.9:55396
```

The address `10.244.0.9` matched the Consumer Pod IP. The port `55396` was the
consumer TCP connection's ephemeral source port.

This provided the first real proof of the complete path:

```text
Consumer Pod
10.244.0.9
    |
    | TcpStream::connect("broker:7001")
    v
Kubernetes DNS
    |
    v
Broker Service
ClusterIP 10.96.149.118
    |
    | Service port 7001
    v
EndpointSlice
10.244.0.8:7001
    |
    v
Broker Pod
10.244.0.8
    |
    v
Rust TcpListener::accept()
```

The Consumer application needed to know only the stable logical destination:

```text
broker:7001
```

It did not need to know the Broker Pod IP or Service ClusterIP.

---

## 36. Docker Desktop, kind, containerd, and `crictl`

During the practical work, Docker Desktop showed the kind Node container but did
not show the individual Kubernetes application Pods as top-level Docker
containers.

The reason is that kind creates Kubernetes Nodes as Docker containers, while the
Kubernetes Node itself uses containerd to run Kubernetes workload containers.

The observed layering is:

```text
Windows / WSL
    |
    v
Docker
    |
    v
rust-distributed-lab-control-plane
(kind Node Docker container)
    |
    v
containerd
    |
    +-- Broker application container
    +-- Consumer application container
    +-- Nginx application containers
    +-- Kubernetes system containers
```

Therefore the different inspection commands answer different questions:

```text
docker ps / Docker Desktop
    -> host Docker containers, including the kind Node

kubectl get pods
    -> Kubernetes Pod objects/workloads

crictl ps inside the kind Node
    -> containers managed by the Node's CRI/containerd runtime
```

The command:

```bash
docker exec rust-distributed-lab-control-plane \
  crictl ps
```

showed both real Rust application containers, including:

```text
NAME       POD
consumer   consumer-8c5c7b49b-mvcvh
broker     broker-6d4549c644-lkfvc
```

This also reinforced that a Pod is not itself identical to a container. In the
current application Pods there is one application container per Pod, but the
concepts remain distinct.

---

## 37. Deployment Versus Job

Introducing the producer exposed an important workload-lifecycle distinction.

The Broker and Consumer are intended to remain alive:

```text
Broker:
start -> listen -> continue running

Consumer:
start -> connect -> wait/process -> continue running
```

A Deployment therefore fits their desired state:

```text
Desired state: RUNNING
```

The current Producer behaves differently:

```text
start
  |
  v
connect to broker
  |
  v
send five messages
  |
  v
exit successfully
```

For this workload, process termination is success rather than failure. A
Kubernetes Job is designed for work that should run to successful completion.

The core distinction established was:

```text
Deployment
    -> maintain running workload replicas

Job
    -> achieve successful completion of work
```

The workload type should therefore be chosen according to the process lifecycle,
not simply because an application has been containerized.

If a future Producer version becomes a continuously running service, that process
lifecycle may fit a Deployment instead. That would be a different workload model,
not merely a normal rolling update of the existing Job into a Deployment.

---

## 38. Producer Containerization

The real Rust producer was packaged with the same multi-stage pattern.

`docker/producer.Dockerfile`:

```dockerfile
FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p producer


FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/producer /usr/local/bin/producer

ENTRYPOINT ["/usr/local/bin/producer"]
```

The image was built as:

```text
rust-distributed-producer:v1
```

and loaded into kind:

```bash
kind load docker-image rust-distributed-producer:v1 \
  --name rust-distributed-lab
```

Loading the image did not execute the producer. It only made the image available
to the Node's container runtime.

---

## 39. Producer Job

The Producer Job is defined in:

```text
k8s/producer-job.yaml
```

Current manifest:

```yaml
apiVersion: batch/v1
kind: Job

metadata:
  name: producer

spec:
  template:
    metadata:
      labels:
        app: producer

    spec:
      restartPolicy: Never

      containers:
        - name: producer
          image: rust-distributed-producer:v1

          env:
            - name: BROKER_PRODUCER_ADDRESS
              value: "broker:7000"
```

Jobs belong to the Kubernetes `batch/v1` API group. This API version is unrelated
to the `v1` container-image tag.

The Job's Pod receives:

```text
BROKER_PRODUCER_ADDRESS=broker:7000
```

so the Rust producer remains environment-agnostic and obtains its Kubernetes
network destination through runtime configuration.

The Pod uses:

```yaml
restartPolicy: Never
```

For this first Job experiment, this makes the container lifecycle particularly
clear. Successful process termination allows the Pod to reach `Completed`, while
Job-level completion/retry behaviour remains a separate Kubernetes concern to be
explored later.

---

## 40. Successful Job Completion

The Producer Job was observed as:

```text
NAME       STATUS     COMPLETIONS   DURATION
producer   Complete   1/1           3s
```

Its Pod was observed as:

```text
NAME             READY   STATUS      RESTARTS
producer-sz9fc   0/1     Completed   0
```

For a Job, `Completed` is success rather than an error condition.

The lifecycle was:

```text
Job
 |
 v
Producer Pod created
 |
 v
container starts
 |
 v
/usr/local/bin/producer
 |
 v
connect broker:7000
 |
 v
send five messages
 |
 v
Rust process exits successfully
 |
 v
container terminates
 |
 v
Pod: Completed
 |
 v
Job: Complete 1/1
```

This establishes a deeper distinction than simply "continuous versus one-time":

```text
Deployment desired state -> running
Job desired state        -> successful completion
```

The Job already existed when a later `kubectl apply` was issued, so Kubernetes
reported:

```text
job.batch/producer unchanged
```

Reapplying the same completed Job object does not mean "execute this command
again." The named Job already represents a completed execution.

---

## 41. Producer Service Discovery and Logs

The completed Job retained logs that could be inspected with:

```bash
kubectl logs job/producer
```

Observed output included:

```text
Producer starting...
Producer connecting to broker at broker:7000...
Producer connected to broker.
Producer sent message 5f0553cf-3457-4aae-a92f-da603b424dcf as 25 serialized bytes.
Producer sent message 0c6fd7e8-ff99-496d-9d65-78c154fbfc28 as 25 serialized bytes.
Producer sent message 2bdba34b-e617-4fc2-bdfd-fef6099d065f as 27 serialized bytes.
Producer sent message cf4d6284-d8e4-4373-a497-792263aef921 as 26 serialized bytes.
Producer sent message f53b9699-49be-4f80-a974-80970d5a497c as 26 serialized bytes.
```

The Producer therefore successfully used Kubernetes DNS and the Broker Service
through port 7000.

The Broker observed the Producer connection from:

```text
10.244.0.10:40610
```

where `10.244.0.10` was the Producer Pod's network identity and `40610` was the
connection's ephemeral source port.

---

## 42. Full Producer -> Broker -> Consumer Proof

The Broker logs showed all five Producer message UUIDs arriving from the Producer
Pod, entering the bounded queue, being delivered to the Consumer, and receiving
ACKs.

For example, the first message followed this application-level path:

```text
Producer
5f0553cf-3457-4aae-a92f-da603b424dcf
        |
        v
Broker receives message
        |
        v
bounded Tokio MPSC queue
        |
        v
Broker sends message to Consumer
        |
        v
Consumer processes message
        |
        v
Consumer sends ACK with same UUID
        |
        v
Broker validates/receives ACK
```

The Consumer independently logged:

```text
Consumer received broker message 5f0553cf-3457-4aae-a92f-da603b424dcf: Message One
Consumer acknowledged broker message 5f0553cf-3457-4aae-a92f-da603b424dcf.

Consumer received broker message 0c6fd7e8-ff99-496d-9d65-78c154fbfc28: Message Two
Consumer acknowledged broker message 0c6fd7e8-ff99-496d-9d65-78c154fbfc28.

Consumer received broker message 2bdba34b-e617-4fc2-bdfd-fef6099d065f: Message Three
Consumer acknowledged broker message 2bdba34b-e617-4fc2-bdfd-fef6099d065f.

Consumer received broker message cf4d6284-d8e4-4373-a497-792263aef921: Message Four
Consumer acknowledged broker message cf4d6284-d8e4-4373-a497-792263aef921.

Consumer received broker message f53b9699-49be-4f80-a974-80970d5a497c: Message Five
Consumer acknowledged broker message f53b9699-49be-4f80-a974-80970d5a497c.
```

This provides end-to-end evidence from all three independent Rust processes.

---

## 43. Backpressure Still Works Under Kubernetes

The Broker logs also exposed the existing bounded-channel backpressure behaviour.

The broker's Tokio MPSC channel has capacity three. During the five-message run,
a Producer handler reached an `attempting to queue` log for a later message before
space was immediately available. A message was then removed from the queue for
consumer delivery, after which the producer handler could log that the waiting
message had been queued.

Conceptually:

```text
Producer handler
    |
    v
bounded queue full
    |
    | await capacity
    v
Consumer side removes/delivers a queued message
    |
    v
capacity becomes available
    |
    v
Producer handler continues queueing
```

Kubernetes did not implement this backpressure. The behaviour remains part of the
Rust distributed-services application and continued to function after the
processes were containerized and orchestrated.

---

## 44. Current End-to-End Kubernetes Architecture

The real application now runs as:

```text
                         Kubernetes Cluster

+-----------------------+
| Producer Job          |
| producer:v1           |
| finite workload       |
+-----------+-----------+
            |
            | broker:7000
            v
+-------------------------------------------+
| Service: broker                           |
| ClusterIP: 10.96.149.118                  |
|                                           |
| producer :7000              consumer :7001|
+-------------------+-----------------------+
                    |
                    v
          +-----------------------+
          | Broker Deployment     |
          | replicas: 1           |
          | Broker Pod            |
          | broker:v2             |
          | Rust broker process   |
          +-----------+-----------+
                      |
                      | existing consumer TCP connection
                      v
          +-----------------------+
          | Consumer Deployment   |
          | replicas: 1           |
          | Consumer Pod          |
          | consumer:v1           |
          | Rust consumer process |
          +-----------------------+
```

The Producer Pod completes after sending its finite batch. The Broker and Consumer
remain running under their Deployments.

---

## 45. Kubernetes Versus Distributed-System Responsibilities

The completed experiment makes the architectural boundary concrete.

Kubernetes currently provides or manages:

```text
- workload creation;
- Deployment reconciliation;
- Job completion tracking;
- Pod/container execution;
- service discovery;
- DNS;
- stable Broker Service identity;
- Pod networking;
- runtime environment injection;
- application image placement on the Node.
```

The Rust distributed-services application still provides:

```text
- TCP application protocol;
- length-prefix framing;
- UUID message identity;
- bounded Tokio MPSC queue;
- backpressure;
- message delivery;
- acknowledgement markers;
- acknowledgement UUID validation;
- retry/requeue behaviour;
- consumer-side duplicate detection.
```

The important principle remains:

```text
Kubernetes orchestrates the processes.

The distributed application defines message semantics and correctness.
```

---

## 46. Current Checkpoint — 23 September 2026

Completed:

- Kubernetes process/container/Pod/Node/Cluster mental model;
- namespaces and cgroups;
- desired state and reconciliation;
- Deployment, ReplicaSet, and Pod relationships;
- Pod replacement;
- scaling;
- rolling updates;
- labels and selectors;
- Services and ClusterIP;
- DNS/service discovery;
- EndpointSlice;
- kube-proxy/service datapath mental model;
- CNI and Pod networking fundamentals;
- Docker multi-stage Rust builds;
- Broker V1 loopback experiment;
- runtime-configurable Broker bind addresses;
- Broker V2 image;
- real Broker Deployment;
- real Broker Service exposing ports 7000 and 7001;
- runtime-configurable Producer and Consumer destinations;
- Consumer V1 container image;
- Consumer image loaded into kind;
- Consumer Deployment;
- successful Consumer Pod -> `broker:7001` -> Broker Pod connection;
- direct inspection of Kubernetes containers using `crictl` inside the kind Node;
- Docker Desktop / kind / containerd runtime-layer distinction;
- Deployment-versus-Job workload lifecycle model;
- Producer V1 container image;
- Producer image loaded into kind;
- Producer Job using `batch/v1`;
- successful Producer Job completion;
- successful Producer Pod -> `broker:7000` -> Broker Pod connection;
- complete five-message Producer -> Broker -> Consumer flow;
- verified application ACKs for all five UUIDs;
- observed bounded-queue backpressure continuing to operate under Kubernetes.

The major practical milestone is now:

```text
Real Rust Producer
      |
      | Kubernetes Job
      | broker:7000
      v
Broker Service
      |
      v
Real Rust Broker
      |
      | application queue / ACK protocol
      v
Real Rust Consumer
      |
      v
five messages processed and acknowledged
```

The original near-term steps of containerizing/deploying the Consumer,
containerizing/running the Producer, and proving the complete real application
path have therefore been completed.

---

## 47. Exact Resume Point

The next Kubernetes work should begin from this known-good state rather than
repeating the basic deployment path.

A natural next practical stage is to exercise Kubernetes recovery and
reconciliation using the real Rust workloads rather than Nginx.

For example, future experiments can deliberately replace/fail an application Pod
and observe:

```text
Deployment desired state
        |
        v
Pod failure/deletion
        |
        v
ReplicaSet / controller reconciliation
        |
        v
replacement Pod
        |
        v
Service EndpointSlice update
        |
        v
application reconnect behaviour
```

This must be interpreted together with the current application's own reconnect
and in-memory-state semantics. Kubernetes can replace a process, but replacement
does not preserve the Broker's in-memory queue.

After the appropriate resilience/deployment exercises, the planned learning path
continues toward NATS and then deployment of NATS together with the Rust
applications on Kubernetes.

The distributed-services implementation continues independently in parallel.

---

## 48. Updated Mental Model Summary

The workload lifecycle now has two concrete branches:

```text
LONG-RUNNING WORKLOAD

Rust source
    |
    v
container image
    |
    v
Deployment
    |
    v
ReplicaSet
    |
    v
Pod
    |
    v
container
    |
    v
Rust process kept running

Examples:
Broker, Consumer
```

and:

```text
FINITE WORKLOAD

Rust source
    |
    v
container image
    |
    v
Job
    |
    v
Pod
    |
    v
container
    |
    v
Rust process performs work
    |
    v
exit code 0
    |
    v
Pod Completed
    |
    v
Job Complete

Example:
Producer V1
```

The current network path is:

```text
Producer Job Pod
    |
    | TcpStream::connect("broker:7000")
    v
Kubernetes DNS
    |
    v
Broker Service
    |
    v
EndpointSlice / service datapath
    |
    v
Broker Pod
    |
    | Rust application protocol
    v
bounded MPSC queue
    |
    v
Consumer connection
    |
    v
Consumer Pod
    |
    v
process message + ACK
    |
    v
Broker validates ACK
```

The architectural boundary remains:

```text
Kubernetes
    manages where/how/lifecycle of processes

Distributed-services code
    manages what messages mean and how delivery behaves
```
