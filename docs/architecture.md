# Architecture

```mermaid
flowchart LR
    A[Browser UI A] --> CA[Client A]
    B[Browser UI B] --> CB[Client B]
    CA -->|TCP auth and control| S[Server]
    CB -->|TCP auth and control| S
    CA -->|UDP streaming| S
    CB -->|UDP streaming| S
    S -->|UDP routing| CA
    S -->|UDP routing| CB
```

The application separates the browser UI, client network logic, server state, protocol encoding and cryptography.

The browser communicates only with its local client process. Client to server networking uses raw TCP and UDP sockets.

## Connection state

```mermaid
stateDiagram-v2
    [*] --> Disconnected
    Disconnected --> Connecting: TCP connect
    Connecting --> Authenticating: HELLO received
    Authenticating --> Active: AUTH_OK
    Authenticating --> Disconnected: AUTH_FAIL / timeout
    Active --> Stale: heartbeat timeout
    Active --> Closing: user exits / TCP closes
    Stale --> Disconnected: server cleanup
    Closing --> Disconnected: sockets closed
```

## File receive state

```mermaid
stateDiagram-v2
    [*] --> AwaitingMetadata
    AwaitingMetadata --> Receiving: valid FILE_META
    Receiving --> Receiving: valid new chunk
    Receiving --> Receiving: duplicate chunk
    Receiving --> Complete: all chunks and SHA-256 match
    Receiving --> Failed: invalid size, disk error, hash mismatch or timeout
    Complete --> [*]
    Failed --> [*]
```

Per-recipient/per-stream relay workers and selective-repeat windows keep loss recovery independent between separate chats and file transfers. The server targets the assignment's 10-20 concurrent-client scale; larger deployments would need bounded queues, admission limits and persistent user/storage services.
