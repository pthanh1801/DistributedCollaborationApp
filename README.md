# Distributed Collaboration Demo

A self-hosted teaching application for the Network Application Development assignment. It uses raw TCP sockets for authentication/control and raw UDP sockets for discovery, chat and reliable chunked file delivery. The browser UI is served locally by Python's standard-library HTTP server; it does not use a web framework or WebSockets.

## Implemented assignment milestones

1. Custom packet header, message types, protocol specification and state diagrams.
2. TCP `HELLO` / encrypted-password authentication handshake.
3. Authenticated user state with TCP connection, UDP endpoint and heartbeat timestamp.
4. Threaded TCP handling, UDP processing and per-recipient/per-stream relay workers.
5. UDP broadcast and private messaging.
6. Encrypted UDP file chunks, SHA-256 verification, selective ACK/retry and sliding windows.
7. Timeouts, stale-client cleanup, input validation and graceful socket shutdown.
8. Fernet encryption for UDP payloads and the password token on the TCP control channel.
9. Local browser UI showing active users, chat and file-transfer status/progress.
10. Automated protocol and transfer tests plus multi-client run instructions.

## Requirements

- Python 3.10 or newer
- A local network that permits TCP port `9000` and UDP port `9001`
- Windows PowerShell, macOS Terminal or Linux shell

## Install (Windows PowerShell)

Run from the project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks venv activation, invoke `.\.venv\Scripts\python.exe` directly.

## Install (macOS / Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Generate and configure the shared encryption key

Generate one key after installing dependencies:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the generated value. In **each** PowerShell window used for the server and clients, set the same key:

```powershell
$env:COLLAB_FERNET_KEY = "paste-the-generated-key-here"
```

On macOS/Linux, export the same generated value in each terminal:

```bash
export COLLAB_FERNET_KEY='paste-the-generated-key-here'
```

The application intentionally refuses to start without this key. Do not commit or share the key in source control. The key is a pre-shared teaching key; there is no key exchange or per-user key management.

## Run the server and clients

In a server terminal (with the shared key set):

```powershell
python server.py
```

In separate client terminals (also set the same key in each one):

```powershell
python client.py --username alice --password alice123 --ui-port 8001
python client.py --username bob --password bob123 --ui-port 8002
```

When running multiple clients on the same computer, give each client a different `--ui-port`. The UI port is checked before login so a port conflict does not leave a client briefly registered as online.

On macOS/Linux, replace `python` with `python3` if that is the Python command for your environment.

Open the local URLs printed by the clients:

```text
http://127.0.0.1:8001
http://127.0.0.1:8002
```

Choose **Everyone** for broadcast or a username for a private message. Received files are stored in `downloads/`; duplicate filenames are saved with a numeric suffix. The default demo accounts are `alice / alice123`, `bob / bob123`, `carol / carol123`, and `user04 / demo04` through `user20 / demo20`, configured in `users.json`. These credentials are deliberately public demo data and must be replaced for any real deployment.

## LAN demo

1. Start the server on the host machine.
2. Find its LAN IPv4 address (for example, `ipconfig` on Windows).
3. Allow inbound TCP `9000` and UDP `9001` in the host firewall.
4. On each client machine, install the dependencies and set the same shared key.
5. Connect with the server's LAN address:

```powershell
python client.py --username alice --password alice123 --server 192.168.1.20 --ui-port 8001
```

The browser UI binds to `127.0.0.1` only; each user opens the UI on their own machine.

## Protocol and limits

- TCP `9000`: `HELLO`, authentication and periodic `PING` / `PONG`.
- UDP `9001`: encrypted heartbeat, user list, messages, ACKs, file metadata and file chunks.
- UDP chunks are 900 bytes before encryption; complete application datagrams are capped at 1,400 bytes to avoid IP fragmentation on common 1,500-byte LAN MTUs.
- Sender-side window slides forward as ACKs arrive and grows using a bounded CUBIC-style congestion window; relays use an eight-packet selective-repeat window.
- File size is limited to 50 MiB; message text is limited to 4,000 characters.
- UDP transfers use sequence numbers, message IDs, selective ACKs and bounded retransmission. File chunks may arrive out of order and are reassembled by index; SHA-256 is checked before the file is saved.
- The sender's `sent to server` status means the server accepted all packets; the receiver shows `done` only after reassembly and hash verification. Incomplete incoming transfers time out after 15 seconds.
- Server relays use an independent worker per recipient and message/file stream, so a delayed packet on one stream does not block unrelated streams.

## Tests

```powershell
python -m pytest -q
```

The tests cover packet validation/encryption, server routing, chunk validation and out-of-order file reassembly. The multi-terminal steps above are still needed to validate firewall and real network behavior on the target machines.

## Packet capture

```powershell
tcpdump -i any tcp port 9000
tcpdump -i any udp port 9001
```

Fernet encrypts payloads, not packet timing, sizes, addresses or the custom header. This project is a teaching demo, not a production secure messenger: demo passwords are stored as plaintext in `users.json`, the shared encryption key is manually provisioned, and there is no TLS certificate validation, account provisioning or key rotation.
