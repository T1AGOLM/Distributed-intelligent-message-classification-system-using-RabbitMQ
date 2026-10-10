# Distributed Intelligent Message Classification System using RabbitMQ

A distributed system built with **Java**, **Docker**, **RabbitMQ** and **embedded Artificial Intelligence** that automatically generates, routes and classifies messages through two independent consumers.

Messages of two different kinds — product reviews and support requests — are published to a RabbitMQ **Topic Exchange** and consumed by two separate services, each one with its own AI-based classification mechanism.

---

## 📋 Table of Contents

- [Architecture](#-architecture)
- [How It Works](#-how-it-works)
- [Tech Stack](#-tech-stack)
- [Prerequisites](#-prerequisites)
- [Getting Started](#-getting-started)
- [The AIs](#-the-ais)
- [Message Flow](#-message-flow)
- [Reliability Features](#-reliability-features)
- [Project Structure](#-project-structure)
- [Demonstration](#-demonstration)
- [License](#-license)

---

## 🏗 Architecture

The system is composed of **4 containers** connected to the same Docker network, orchestrated by Docker Compose:

```
┌──────────────────────┐        ┌─────────────────────────────────────┐
│  Message Generator   │        │              RabbitMQ               │
│      (Java)          │        │         Topic Exchange              │
│                      │  publishes    routing keys:                  │
│  ≥ 5 messages/sec    ├───────►•  review.product                    │
│  - product reviews   │        │  support.request                   │
│  - support requests  │        │                                     │
└──────────────────────┘        │  ┌──────────────┐ ┌──────────────┐  │
                                │  │ Queue:       │ │ Queue:       │  │
                                │  │ reviews      │ │ support      │  │
                                │  │ + DLQ        │ │ + DLQ        │  │
                                │  └──────┬───────┘ └──────┬───────┘  │
                                └─────────┼────────────────┼──────────┘
                                          │                │
                     ┌────────────────────▼──┐   ┌─────────▼──────────────┐
                     │  Consumer 1 (Java)    │   │  Consumer 2 (Java)     │
                     │  Sentiment Analysis   │   │  Support Classification│
                     │  positive/negative/   │   │  access/performance/   │
                     │  neutral              │   │  connection            │
                     │  (Smile ML library)   │   │  (Smile ML library)    │
                     └───────────────────────┘   └────────────────────────┘
```

| Container | Role |
|-----------|------|
| **Message Generator** | Continuously produces at least 5 messages/second and publishes them to RabbitMQ, tagging each message with its type. |
| **RabbitMQ** | Routes messages through a Topic Exchange using the routing keys `review.product` and `support.request`. One queue per consumer, plus a Dead Letter Queue for failed messages. Management UI enabled. |
| **Consumer 1 – Sentiment Analysis** | Consumes product reviews and classifies each one as **positive**, **negative** or **neutral** using an embedded AI model. Results are printed to the console. |
| **Consumer 2 – Support Request Classification** | Consumes support requests and classifies each one as **access problem**, **performance problem** or **connection problem** using an embedded AI model. Results are printed to the console. |

---

## ⚙️ How It Works

1. The **Generator** creates textual messages of two types — product reviews and support requests — at a rate of at least 5 messages per second.
2. Each message carries a **unique ID**, **generation timestamp**, **type** and **content**.
3. Messages are published to a **Topic Exchange** with the routing key `review.product` or `support.request`.
4. The exchange forwards each message to the queue of the corresponding consumer.
5. Each consumer processes messages **intentionally slower than the arrival rate**, so the queue growth can be observed in the RabbitMQ management UI.
6. Classification results (`positive / negative / neutral` or `access / performance / connection`) are displayed in each consumer's console.
7. Consumption uses **manual ACK**: a message is only acknowledged after successful processing. If processing fails, the message is routed to a **Dead Letter Queue (DLQ)** instead of being discarded.

---

## 🛠 Tech Stack

| Technology | Purpose |
|------------|---------|
| **Java** | Implementation of the generator and both consumers |
| **RabbitMQ** | Message broker with Topic Exchange, DLQ and management UI |
| **Docker / Docker Compose** | Containerization and orchestration of all 4 services |
| **Smile** | Embedded machine-learning library used by both consumers for classification |
| **SLF4J / Logback** *(planned)* | Structured console logging of classification results |

---

## 📦 Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose v2
- Java JDK 17+ *(only needed if running services outside Docker)*

---

## 🚀 Getting Started

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/<repository-name>.git
cd <repository-name>

# 2. Build and start all 4 containers
docker compose up --build

# 3. Open the RabbitMQ management UI (default credentials: guest/guest)
#    http://localhost:15672

# 4. Watch the classification results
docker compose logs -f consumer-1   # sentiment analysis
docker compose logs -f consumer-2   # support request classification

# 5. Stop the system
docker compose down
```

---

## 🤖 The AIs

Both consumers embed their own independent AI mechanism, powered by the [Smile](https://haifengl.github.io/smile/) machine-learning library for Java.

### Consumer 1 — Sentiment Analysis
- **Task:** multi-class classification of free-text product reviews.
- **Classes:** `positive`, `negative`, `neutral`.
- **Approach:** text features are extracted from the review content and fed into an embedded Smile classifier trained on a labeled review dataset.

### Consumer 2 — Support Request Classification
- **Task:** categorization of technical support requests.
- **Classes:** `access problem`, `performance problem`, `connection problem`.
- **Approach:** each request is converted into feature vectors and classified by its own embedded Smile model trained on labeled support tickets.

> Each consumer owns its classifier — no external AI service or network call is required at runtime.

---

## 📨 Message Flow

```
Generator ──► Topic Exchange ──┬──► review.queue   ──► Consumer 1 ──► console
                               │         │ (on error)
                               │         └──────► review.dlq
                               │
                               └──► support.queue ──► Consumer 2 ──► console
                                          │ (on error)
                                          └──────► support.dlq
```

**Routing keys:** `review.product` · `support.request`

---

## 🛡 Reliability Features

- **Manual ACK** — messages are acknowledged only after successful processing, preventing accidental message loss.
- **Dead Letter Queue (DLQ)** — messages that raise processing errors are redirected to a DLQ instead of being dropped, allowing later inspection and reprocessing.
- **Slower consumers** — processing latency is deliberately higher than the production rate so queue buildup is visible in the RabbitMQ management UI.
- **Unique message identity** — every message carries an ID, timestamp, type and content for full traceability.

---

## 📁 Project Structure

```
.
├── generator/                  # Message generator service
│   ├── Dockerfile
│   └── src/
├── consumer-1/                 # Sentiment analysis consumer
│   ├── Dockerfile
│   └── src/
├── consumer-2/                 # Support request classification consumer
│   └── Dockerfile
├── rabbitmq/                   # Broker configuration (exchange, queues, DLQ)
├── docker-compose.yml          # Orchestration of all containers
├── docs/                       # Screenshots and evidence of execution
└── README.md
```

---

## 📄 License

This project is licensed under the terms of the [LICENSE](LICENSE) file.
