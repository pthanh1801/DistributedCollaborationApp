# Assignment 1: Distributed and Streaming-based Client-Server Collaboration Application

**Course:** Network Application Development  
**Duration:** 6 Weeks (Take-home Project)  
**Project Type:** Individual / Pair Project  

---

## 1. Introduction & Background

### 1.1 Current Practices
With the rapid global shift toward remote work, distributed teams, and decentralized systems, reliance on robust, real-time network applications has never been higher. Modern organizations require seamless data exchange over local and wide-area networks while navigating challenges like latency, packet loss, and data security. Understanding how to build resilient network software from the ground up is a critical skill for today’s software and network engineers.

### 1.2 The Need for This Application
While commercial solutions (like Slack, Microsoft Teams, or cloud storage) exist, they often route data through centralized third-party servers, which can raise privacy, compliance, and bandwidth concerns for closed-network environments. There is a distinct need for a lightweight, secure, and self-hosted network application that allows users on the same network (or VPN) to communicate and share data directly without relying on external corporate infrastructure.

### 1.3 Goal of the Application
The goal of this project is to build a **Distributed and Streaming-based Client-Server Collaboration Application.** Students will design and implement a custom application layer protocol over **UDP STREAMING** that enables real-time messaging, active user discovery, and reliable file transfer between multiple clients over a network. Due to the native requirement of messaging applications, **reliable communication** must be provided.

---

## 2. Core Features of the Application

* **User Authentication & Discovery:** Users can log in, broadcast their presence, and see a live list of other active peers on the network.
* **Streaming-based Messaging:** Support for both broadcast (global chat) and unicast (private direct messaging). Inspired by *Steam GameNetworkingSockets* and *Google QUIC* for message-oriented transport layer design.
* **Reliable File Transfer:** The ability to send and receive files of various formats and sizes without data corruption.
* **Connection Resilience:** The application gracefully handles sudden client disconnects, timeouts, and network interruptions.
* **Secure Data Transmission:** Basic encryption applied to the payloads to prevent packet-sniffing vulnerabilities.

---

## 3. Target Users & Integrated Technologies

### 3.1 Target Users
* Internal corporate teams operating on secure, air-gapped, or isolated local area networks (LAN).
* Students or study groups needing a fast, peer-to-peer way to collaborate and share files without internet access.
* Privacy-conscious individuals who prefer self-hosted communication tools.

### 3.2 Techniques and Technologies Integrated
* **Socket Programming:** Implementation of TCP (for reliable file/message transfer) and UDP (for heartbeat/discovery broadcasts).
* **Concurrency:** Multi-threading or Asynchronous I/O to handle multiple simultaneous client connections without blocking the main thread.
* **Application-Layer Protocol Design:** Designing custom packet structures (Headers, Payloads, End-of-Message flags).
* **Sequencing and Ordering:** Maintaining monotonic sequence numbers with a window-based sliding window for flow control and in-order delivery.
* **Acknowledgement and Retransmission:** Efficient retransmitting of only missing segments.
* **Congestion Control:** Support streaming specific mechanisms with BBR / CUBIC streaming support.
* **Cryptography:** Integration of TLS/SSL sockets or symmetric encryption (e.g., AES) for data payloads.
* **Serialization:** Encoding and decoding structured data (JSON, XML, or custom binary packing) over the wire.

---

## 4. Design & Technical Requirements

### 4.1 Design Requirements
* **Architecture:** Must clearly separate network logic, data processing, and the user interface.
* **Time-sensitive Streaming:** The system must provide an async mechanism to fetch data from a stream without blocking. Streaming handles byte chunks in UDP and leaves protocol logic in the application layer to eliminate Head-of-Line (HoL) blocking.
* **User Interface:** Can be a clean Command Line Interface (CLI) or a Graphical User Interface (GUI/HTTP-based). It must be intuitive and display network status clearly.
* **Scalability:** The server/tracker must be capable of handling at least 10–20 concurrent clients efficiently.

### 4.2 Technical Requirements
* **Programming Language:** Python, Java, C++, or Go.
* **No High-Level Networking Libraries:** Students must use raw sockets (`socket` library in Python, `java.net.Socket` in Java, etc.). High-level HTTP frameworks (like Flask/Django) or WebSockets are strictly prohibited to ensure core networking concepts are practiced.
* **Environment:** Must run successfully on standard operating systems (Windows, macOS, or Linux) using localhost or local IP addressing.

---

## 5. Development Process & Grading Rubric (10 Points Total)

This project is broken down into a 10-step development lifecycle. Each successfully implemented step is worth **1 point**.

| Step | Milestone | Score | Description |
| :---: | :--- | :---: | :--- |
| **1** | **Setup & Protocol Design** | 1 pt | Define custom packet structure, message types (e.g., `AUTH`, `MSG`, `FILE`), and state diagrams in documentation before coding. |
| **2** | **Basic Client-Server Handshake** | 1 pt | Implement basic TCP socket connection where a client can connect to the server and receive an acknowledgment. |
| **3** | **Authentication & State Management** | 1 pt | Implement a login mechanism. The server must track connected users, their IP addresses, and active ports. |
| **4** | **Concurrency Implementation** | 1 pt | Integrate multi-threading or Async I/O so the server can handle multiple clients connecting and disconnecting simultaneously. |
| **5** | **Time-sensitive Streaming** | 1 pt | Implement routing logic for clients to send messages to the server, and server to broadcast or forward to the correct recipient. |
| **6** | **Reliable File Transfer** | 1 pt | Implement file serialization, chunking (for large files), and reassembly over UDP without data corruption. |
| **7** | **Network Resilience & Error Handling** | 1 pt | Implement try/except blocks to handle socket timeouts, broken pipes, and graceful client exits without crashing the server. |
| **8** | **Security Layer** | 1 pt | Encrypt data payloads (using a standard cryptographic library) before sending, and decrypt upon receipt. |
| **9** | **HTTP-based UI (Client-side JS)** | 1 pt | Build a user-friendly interface displaying incoming messages, file transfer progress bars, and active user lists. |
| **10** | **Testing & Code Quality** | 1 pt | Code must be cleanly formatted, commented, and pass local network simulation tests (connecting from multiple terminal windows). |
| **TOTAL** | | **10 pts** | |

---

## 6. Submission Guidelines

### 6.1 Suggested Deadline
* **Sunday, November 17, 2026, at 11:59 PM**

### 6.2 Required Submission Content
Students must submit a `.zip` file or a link to a private GitHub repository containing:

1. **Source Code:**
   * All client and server scripts.
   * A `README.md` file with strict, step-by-step instructions on how to install dependencies, run the server, and connect the clients.

2. **Technical Explanation Report (PDF):**
   * **System Architecture:** A high-level diagram showing how clients and the server interact.
   * **Protocol Specification:** A breakdown of the custom packet/message structures designed for the application.
   * **Sequence Diagram:** A flowchart showing the lifecycle of a message and a file transfer.
   * **Challenges & Solutions:** A brief 1-page reflection on network challenges faced (e.g., resolving deadlocks, handling packet fragmentation) and how they were solved.
