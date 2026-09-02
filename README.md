# 🚀 Celer Engine

[![Go Reference](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Docker Compose](https://img.shields.io/badge/docker--compose-ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Status](https://img.shields.io/badge/status-in--development-yellow?style=flat)](#-project-status)

**celer-engine** is a high-performance, low-latency event processing engine developed in **Go**. It is designed to consume robot and distributed device telemetry via **Apache Kafka**, apply strict schema validation using **Protocol Buffers (Protobuf)**, aggregate events in in-memory sliding time windows, and emit real-time alerts/warnings.

---

## 🏗️ Architecture and Concurrency Model

The engine uses a concurrent processing pipeline based on **3 dedicated Goroutines / Threads**, interconnected through **buffered Go Channels**. This design isolates network I/O from CPU/memory-intensive tasks and provides native *backpressure*, preventing *Out-Of-Memory (OOM)* conditions under high load.

```text
[ NETWORK / KAFKA (Input Topic - Protobuf) ]
               │
               │ (Raw bytes)
               ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 1: Consumer Loop (internal/kafka/consumer.go)    │
│ -> Reads raw bytes from Kafka at network speed          │
│ -> Feeds the Ingestion Channel (without parsing payload)│
└────────────────────────┬────────────────────────────────┘
                         │
                         │ (Go Channel - Ingestion Buffer / Backpressure)
                         ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 2: Worker + Validator + Aggregator               │
│ -> Consumes raw bytes from the Channel                  │
│ -> Validates and deserializes Protobuf in CPU           │
│ -> On Syntax Error -> Sends JSON to the DLQ             │
│ -> If Valid -> Feeds the In-Memory Aggregator           │
│    ├── Minor anomaly   -> Generates Warning (Protobuf)  │
│    └── Threshold hit   -> Generates Alert (Protobuf)    │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ (Go Channel - Egress Buffer)
                         ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 3: Producer (internal/kafka/producer.go)         │
│ -> Listens to the Egress Channel                        │
│ -> Routes DLQ (JSON) to the "DLQ" topic                 │
│ -> Routes Warnings (Protobuf) to the "Warnings" topic   │
│ -> Routes Alerts (Protobuf) to the "Alerts" topic       │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
[ NETWORK / KAFKA (Topics: DLQ / Warnings / Alerts) ]
```

---

## 🚦 Topic Classification

| Topic | Format | Description / Trigger |
| :--- | :--- | :--- |
| **`Input`** | Protobuf | Raw telemetry emitted by robots / load generator. |
| **`DLQ`** | JSON | Events with schema failures, corrupted payloads, or events rejected by the `Validator`. |
| **`Warnings`** | Protobuf | Syntactically valid events that exhibit deviations or isolated anomalies. |
| **`Alerts`** | Protobuf | Triggered when the thresholds configured in the `Aggregator`'s sliding window are reached. |

---

## 📁 Repository Structure

```text
celer-engine/
├── cmd/
│   ├── engine/       # Celer Engine entry point (main.go)
│   └── chaos-gen/    # Load/chaos generator for telemetry simulation
├── internal/
│   ├── domain/       # Domain interfaces and data models
│   ├── kafka/        # Kafka Consumer and Producer implementation
│   ├── proto/        # .proto definitions and generated Go code
│   ├── validator/    # Payload validation and deserialization
│   ├── aggregator/   # Sliding window and in-memory aggregation
│   └── worker/       # Internal pipeline orchestration
└── docker-compose.yml # Complete isolated environment (Kafka KRaft, Engine, Chaos Gen)
```

---

## ⚙️ How to Run the Project

### Prerequisites

* [Go 1.22+](https://go.dev/)
* [Docker](https://www.docker.com/) and Docker Compose

### Starting the Environment

To start the entire infrastructure (Kafka in KRaft mode + Chaos Generator + Celer Engine):

```bash
docker compose up --build
```

---

## 🚧 Project Status

> ⚠️ **Note:** This project is under active development.

- [x] Decoupled multi-threaded architecture using Go Channels
- [x] Decoupled domain contracts and interfaces
- [x] Protocol Buffers schemas (`event.proto`)
- [x] Load Generator (Chaos Generator)
- [ ] Kafka Consumer and Producer implementation
- [ ] Completion of the Worker Loop and Aggregator
- [ ] Load and end-to-end (E2E) integration testing
```
