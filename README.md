# Redis-Lite

A minimal Redis-compatible in-memory key-value store implemented in C++17, supporting TCP networking, RESP protocol parsing, concurrent clients, and core data structures. Built from scratch with no third-party dependencies — only the C++ Standard Library and POSIX sockets.

This project was built to explore systems design fundamentals including network programming, protocol parsing, concurrency, and low-latency state management in a single-node datastore.

> ~700 LOC C++17 implementation focused on clarity, correctness, and core systems concepts

---

## Overview

Redis-Lite implements a subset of Redis functionality with a focus on:

- Handling multiple client connections over TCP
- Parsing and executing commands using the RESP protocol
- Supporting common data structures (strings, lists, streams)
- Managing in-memory state with optional expiration (TTL)
- Ensuring correctness under concurrent access

The goal is not to fully replicate Redis, but to build a compact, understandable system that captures many of the core ideas behind in-memory data stores.

---

## Features

### Networking

- TCP server supporting multiple simultaneous client connections
- Thread-per-connection model for simplicity and isolation

### Protocol

- RESP (Redis Serialization Protocol) parsing and serialization
- Support for basic command routing and argument handling

### Data Structures

- String key-value store (`SET`, `GET`, `DEL`)
- Lists (push/pop operations)
- Streams (append + read semantics)

### Expiration

- TTL support via time-based key expiration
- Lazy expiration on access

### Concurrency

- Safe handling of concurrent client requests
- Shared in-memory store with synchronization

---

## C++ Standard Library

The implementation relies exclusively on the C++17 standard library — no external frameworks, no Boost, no hand-rolled allocators. Key components and their roles:


| STL Component                                         | Usage                                                                                                   |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `std::unordered_map`                                  | O(1) average-case key lookup for the KV store and list registry                                         |
| `std::deque`                                          | Doubly-ended list structure enabling O(1) push/pop at both ends (`LPUSH`, `RPUSH`, `LPOP`)              |
| `std::mutex` / `std::lock_guard` / `std::unique_lock` | RAII-scoped mutual exclusion guarding all shared store state                                            |
| `std::condition_variable`                             | Blocking notification for `BLPOP` — waiters sleep without spinning until a producer pushes              |
| `std::thread`                                         | Lightweight per-connection threads; each client runs independently                                      |
| `std::shared_ptr`                                     | Shared ownership of `BLPOP` waiter state across multiple key queues without use-after-free risk         |
| `std::optional`                                       | Expressive nullable TTL representation and optional return values — no sentinel integers                |
| `std::string_view`                                    | Zero-copy buffer slicing during RESP parsing; avoids heap allocation on the hot path                    |
| `std::chrono`                                         | Monotonic (`steady_clock`) deadlines for `BLPOP` timeouts; wall-clock (`system_clock`) for TTL expiry   |
| `std::from_chars`                                     | Locale-independent, exception-free integer parsing (`<charconv>`) — faster and safer than `atoi`/`stoi` |
| `std::vector`                                         | Command argument storage with pre-reserved capacity to minimize reallocations                           |
| `std::algorithm`                                      | `std::min` for bounded LPOP count; `std::remove` for erase-remove on waiter cleanup                     |


These choices reflect the same priorities that matter in production low-latency systems: predictable allocations, explicit ownership, and avoiding implicit overhead.

---

## Architecture

At a high level:

Client
↓
TCP Server
↓
Connection Handler (per client)
↓
RESP Parser
↓
Command Dispatcher
↓
In-Memory Store
↓
Response Encoder → Client

Core components:

- **Server**: Accepts and manages client connections  
- **Parser**: Converts RESP messages into executable commands  
- **Command Layer**: Dispatches operations to the datastore  
- **Store**: Maintains in-memory state and data structures  
- **Execution Path**: Applies operations and returns responses

---

## Design Decisions

### Thread-per-connection

A simple threading model was chosen to prioritize clarity over maximum performance. This avoids the complexity of async I/O while still supporting concurrent clients.

### Coarse-grained synchronization

The datastore uses coarse locking to ensure correctness under concurrent access. While this limits throughput under high contention, it simplifies reasoning about consistency.

### Lazy expiration

Expired keys are removed upon access rather than via a background sweeper. This keeps the implementation simple while still supporting TTL semantics.

### In-memory only

Persistence (AOF/RDB) and replication are intentionally omitted to keep the system focused on core in-memory behavior.

---

## Future Work

Potential extensions include:

- Optimistic concurrency / transaction semantics  
- Fine-grained locking or lock-free data structures  
- Background expiration / eviction policies  
- Persistence mechanisms  
- Event-driven or async networking model

---

## Running

```bash
# build and run server (hosted on default port 6379)
./run.sh

# connect via redis-cli
redis-cli
```

## Motivation

This project was built to deepen understanding of:

- Systems programming in C++
- Network protocol design and parsing
- Concurrency and synchronization
- Tradeoffs between simplicity, correctness, and performance

