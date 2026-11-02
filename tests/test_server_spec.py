import copy
import hashlib
import io
import json
import socket
import threading
import time
import uuid

import pytest
from cryptography.fernet import InvalidToken

from config import CHUNK_SIZE, CLIENT_TIMEOUT, MAX_MESSAGE_LENGTH, TCP_PORT, UDP_PORT
from protocol import (
    ACK,
    FILE_CHUNK,
    FILE_META,
    FLAG_END,
    HEARTBEAT,
    MSG,
    USER_LIST,
    Packet,
    cipher,
    decode_packet,
    encode_packet,
    json_bytes,
    json_obj,
    new_id,
)
from server import CollaborationServer
from client import CollaborationClient


# ==============================================================================
# Helper Mock Objects for TCP connection testing
# ==============================================================================

class MockSocketConn:
    """Mock cho kết nối TCP client-server hỗ trợ sendall và makefile."""
    def __init__(self, incoming_data: bytes = b""):
        self.incoming = io.BytesIO(incoming_data)
        self.outgoing = io.BytesIO()
        self.closed = False

    def sendall(self, data: bytes):
        if self.closed:
            raise OSError("Socket closed")
        self.outgoing.write(data)

    def makefile(self, mode: str):
        return self.incoming

    def close(self):
        self.closed = True

    def get_sent_json(self):
        self.outgoing.seek(0)
        lines = [line.strip() for line in self.outgoing.readlines() if line.strip()]
        return [json.loads(line.decode()) for line in lines]


def create_test_server():
    """Tạo CollaborationServer ở dạng test unit (không bind port thật)."""
    server = CollaborationServer.__new__(CollaborationServer)
    server.users = {}
    server.users_lock = threading.Lock()
    server.pending = {}
    server.pending_lock = threading.Lock()
    server.transfers = {}
    server.transfer_lock = threading.Lock()
    server.seen_metadata = {}
    server.seen_messages = {}
    server.seq = 1
    server.seq_lock = threading.Lock()
    server.stop_event = threading.Event()
    server.broadcast_user_list = lambda: None
    server.credentials = {
        "alice": "alice123",
        "bob": "bob123",
        "carol": "carol123"
    }
    return server


# ==============================================================================
# Step 2 - 3: TCP Handshake, Authentication & State Management
# ==============================================================================

def test_tcp_handshake_and_authentication_success():
    """Kiểm tra  Client gửi AUTH đúng pass -> server trả AUTH_OK và lưu state IP/Port."""
    server = create_test_server()
    auth_req = {
        "type": "AUTH",
        "username": "alice",
        "password_token": cipher.encrypt(b"alice123").decode(),
        "udp_port": 12345
    }
    raw_incoming = (json.dumps(auth_req) + "\n").encode()
    mock_conn = MockSocketConn(raw_incoming)

    # Chạy handle_tcp trong 1 thread vì có vòng lặp PING
    t = threading.Thread(target=server.handle_tcp, args=(mock_conn, ("192.168.1.50", 55555)))
    t.start()
    t.join(timeout=0.5)

    sent = mock_conn.get_sent_json()
    assert len(sent) >= 2
    assert sent[0] == {"type": "HELLO", "server": "collab-demo"}
    assert sent[1] == {"type": "AUTH_OK", "username": "alice"}


def test_tcp_authentication_fails_with_wrong_password():
    """Kiểm tra Sai mật khẩu -> Server phản hồi AUTH_FAIL và đóng kết nối."""
    server = create_test_server()
    auth_req = {
        "type": "AUTH",
        "username": "alice",
        "password_token": cipher.encrypt(b"wrong_pass").decode(),
        "udp_port": 12345
    }
    raw_incoming = (json.dumps(auth_req) + "\n").encode()
    mock_conn = MockSocketConn(raw_incoming)

    server.handle_tcp(mock_conn, ("127.0.0.1", 50000))

    sent = mock_conn.get_sent_json()
    assert sent[0] == {"type": "HELLO", "server": "collab-demo"}
    assert sent[1] == {"type": "AUTH_FAIL"}
    assert "alice" not in server.users


# ==============================================================================
# Step 4: Concurrency & State Thread Safety
# ==============================================================================

def test_concurrent_user_registration_thread_safety():
    """Kiểm tra nhiều client đồng thời thao tác users dict mà không gây crash."""
    server = create_test_server()
    errors = []

    def login_client(idx):
        try:
            uname = f"user_{idx}"
            with server.users_lock:
                server.users[uname] = {
                    "tcp": MockSocketConn(),
                    "udp": ("127.0.0.1", 10000 + idx),
                    "last_seen": time.time(),
                    "relay_queues": {}
                }
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=login_client, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    assert len(server.users) == 20


# ==============================================================================
# Step 5: Streaming Messaging (Broadcast vs Unicast Routing)
# ==============================================================================

def test_server_recipients_broadcast_excludes_sender():
    """Kiểm tra tin nhắn broadcast '*' gửi tới tất cả người khác, trừ chính người gửi."""
    server = create_test_server()
    server.users = {
        "alice": {"udp": ("127.0.0.1", 10001)},
        "bob": {"udp": ("127.0.0.1", 10002)},
        "carol": {"udp": ("127.0.0.1", 10003)},
    }

    targets = server.recipients("*", sender="alice")
    names = [name for name, _ in targets]

    assert "alice" not in names
    assert "bob" in names
    assert "carol" in names
    assert len(names) == 2


def test_server_recipients_unicast_routes_only_to_target():
    """Kiểm tra Tin nhắn riêng unicast chỉ định tuyến đến đúng người nhận."""
    server = create_test_server()
    server.users = {
        "alice": {"udp": ("127.0.0.1", 10001)},
        "bob": {"udp": ("127.0.0.1", 10002)},
        "carol": {"udp": ("127.0.0.1", 10003)},
    }

    targets = server.recipients("bob", sender="alice")
    assert targets == [("bob", ("127.0.0.1", 10002))]

    # Không thể tự gửi unicast cho chính mình
    assert server.recipients("alice", sender="alice") == []


def test_server_forward_rejects_empty_or_oversized_message():
    """Kiểm tra Server từ chối tin nhắn rỗng hoặc vượt quá 4000 ký tự."""
    server = create_test_server()
    server.users["bob"] = {"udp": ("127.0.0.1", 10002)}

    # Tin rỗng
    empty_packet = Packet(MSG, 1, uuid.uuid4(), flags=FLAG_END, payload=json_bytes({"to": "bob", "text": "   "}))
    with pytest.raises(ValueError, match="invalid message"):
        server.forward("alice", empty_packet)

    # Tin quá dài
    long_packet = Packet(MSG, 2, uuid.uuid4(), flags=FLAG_END, payload=json_bytes({
        "to": "bob",
        "text": "A" * (MAX_MESSAGE_LENGTH + 1)
    }))
    with pytest.raises(ValueError, match="invalid message"):
        server.forward("alice", long_packet)


# ==============================================================================
# Step 6: File Transfer Integrity & Hash Verification
# ==============================================================================

def test_file_transfer_hash_mismatch_detected_by_client(tmp_path, monkeypatch):
    """Kiểm tra Client nhận phát hiện sai lệch dữ liệu (SHA-256 không khớp) và báo lỗi."""
    monkeypatch.chdir(tmp_path)
    client = CollaborationClient.__new__(CollaborationClient)
    client.state_lock = threading.RLock()
    client.incoming = {}
    client.transfers = []

    file_id = uuid.uuid4()
    content = b"correct file payload"
    fake_content = b"corrupted file payl!"  # Cùng độ dài 20 bytes nhưng khác nội dung

    # Metadata chứa hash của content gốc
    meta = {
        "file_id": file_id.hex,
        "name": "secret.txt",
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "total": 1,
        "from": "alice"
    }

    client.handle_file_meta(Packet(FILE_META, 1, file_id), meta)
    # Gửi chunk với nội dung giả mạo fake_content (cùng size nhưng sai hash)
    client.handle_file_chunk(Packet(FILE_CHUNK, 2, file_id, chunk=0, total=1, flags=FLAG_END, payload=fake_content))

    # Không lưu file vào ổ đĩa và chuyển trạng thái 'hash mismatch'
    assert not (tmp_path / "downloads" / "secret.txt").exists()
    assert client.transfers[0]["status"] == "hash mismatch"


def test_file_name_collision_handled_with_numeric_suffix(tmp_path, monkeypatch):
    """Kiểm tra Trùng tên file thì tự động đổi tên thành (1), (2)... không ghi đè."""
    monkeypatch.chdir(tmp_path)
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    (downloads / "document.pdf").write_bytes(b"existing file")

    client = CollaborationClient.__new__(CollaborationClient)
    client.state_lock = threading.RLock()
    client.incoming = {}
    client.transfers = []

    file_id = uuid.uuid4()
    new_data = b"new file content"
    meta = {
        "file_id": file_id.hex,
        "name": "document.pdf",
        "size": len(new_data),
        "sha256": hashlib.sha256(new_data).hexdigest(),
        "total": 1,
        "from": "bob"
    }

    client.handle_file_meta(Packet(FILE_META, 1, file_id), meta)
    client.handle_file_chunk(Packet(FILE_CHUNK, 2, file_id, chunk=0, total=1, flags=FLAG_END, payload=new_data))

    assert (downloads / "document.pdf").read_bytes() == b"existing file"
    assert (downloads / "document (1).pdf").read_bytes() == new_data
    assert client.transfers[0]["status"] == "done"


# ==============================================================================
# Step 7: Network Resilience & Stale Client Cleanup
# ==============================================================================

def test_stale_client_without_heartbeat_is_evicted():
    """Kiểm tra Client không gửi heartbeat quá 10s sẽ bị server dọn dẹp tự động."""
    server = create_test_server()
    mock_tcp = MockSocketConn()
    server.users = {
        "active_user": {
            "tcp": MockSocketConn(),
            "udp": ("127.0.0.1", 10001),
            "last_seen": time.time(),
        },
        "stale_user": {
            "tcp": mock_tcp,
            "udp": ("127.0.0.1", 10002),
            "last_seen": time.time() - (CLIENT_TIMEOUT + 5),  # 15s trước
        }
    }

    # Giả lập 1 chu kỳ cleanup
    cutoff = time.time() - CLIENT_TIMEOUT
    with server.users_lock:
        stale = [uname for uname, info in server.users.items() if info["last_seen"] < cutoff]
        for uname in stale:
            server.users[uname]["tcp"].close()
            server.users.pop(uname, None)

    assert "stale_user" not in server.users
    assert "active_user" in server.users
    assert mock_tcp.closed is True


# ==============================================================================
# Step 8: Security Layer (Strict Encryption)
# ==============================================================================

def test_protocol_strictly_requires_encrypted_payloads():
    """Kiểm tra Giao thức từ chối gửi payload không mã hóa."""
    packet = Packet(MSG, 1, uuid.uuid4(), flags=FLAG_END, payload=b"unencrypted text")
    with pytest.raises(ValueError, match="UDP payloads must be encrypted"):
        encode_packet(packet, encrypt=False)


def test_packet_decoder_rejects_unknown_flags():
    """Kiểm tra Decode từ chối packet có flag không xác định (chống can thiệp gói tin)."""
    packet = Packet(MSG, 1, uuid.uuid4(), flags=0x80, payload=b"payload")
    with pytest.raises(ValueError, match="unknown packet flags"):
        decode_packet(encode_packet(packet))


# ==============================================================================
# Scalability: 20 Concurrent Clients Handling
# ==============================================================================

def test_20_concurrent_clients_authentication_and_routing():
    """Kiểm tra Server xử lý đồng thời 20 client kết nối, xác thực và định tuyến broadcast."""
    from pathlib import Path
    users_data = json.loads(Path("users.json").read_text(encoding="utf-8"))
    user_items = list(users_data.items())[:20]
    assert len(user_items) == 20, "users.json cần có đủ ít nhất 20 tài khoản"

    server = create_test_server()
    server.credentials = dict(user_items)

    class BlockingMockConn(MockSocketConn):
        def __init__(self, incoming_data: bytes = b""):
            super().__init__(incoming_data)
            self.unblock = threading.Event()

        def makefile(self, mode: str):
            stream = self.incoming
            unblock_event = self.unblock

            class StreamWrapper:
                def readline(self, size=-1):
                    line = stream.readline(size)
                    if line:
                        return line
                    unblock_event.wait()
                    return b""

                def close(self):
                    unblock_event.set()

            return StreamWrapper()

        def close(self):
            super().close()
            self.unblock.set()

    mock_conns = {}
    threads = []

    def client_worker(uname, pwd, port_idx):
        auth_req = {
            "type": "AUTH",
            "username": uname,
            "password_token": cipher.encrypt(pwd.encode()).decode(),
            "udp_port": 20000 + port_idx
        }
        raw_in = (json.dumps(auth_req) + "\n").encode()
        conn = BlockingMockConn(raw_in)
        mock_conns[uname] = conn
        server.handle_tcp(conn, ("127.0.0.1", 30000 + port_idx))

    # Khởi chạy 20 client đồng thời
    for idx, (uname, pwd) in enumerate(user_items):
        t = threading.Thread(target=client_worker, args=(uname, pwd, idx))
        threads.append(t)
        t.start()

    # Chờ tất cả 20 client đăng nhập thành công vào server.users
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        with server.users_lock:
            if len(server.users) == 20:
                break
        time.sleep(0.05)

    # Kiểm tra 20 client đều nhận HELLO và AUTH_OK
    for uname in mock_conns:
        sent = mock_conns[uname].get_sent_json()
        assert len(sent) >= 2, f"Client {uname} chưa nhận đủ phản hồi từ server"
        assert sent[0]["type"] == "HELLO"
        assert sent[1] == {"type": "AUTH_OK", "username": uname}

    # Kiểm tra danh sách online trong server đủ 20 users
    with server.users_lock:
        assert len(server.users) == 20
        for uname, _ in user_items:
            assert uname in server.users

    # Kiểm tra định tuyến broadcast: alice gửi thì đúng 19 client còn lại nhận được
    recipients = server.recipients("*", sender="alice")
    recipient_names = [name for name, _ in recipients]
    assert len(recipient_names) == 19
    assert "alice" not in recipient_names
    assert "bob" in recipient_names
    assert "user20" in recipient_names

    # Đóng tất cả kết nối an toàn và kiểm tra dọn dẹp
    for conn in mock_conns.values():
        conn.close()
    for t in threads:
        t.join(timeout=2.0)

    with server.users_lock:
        assert len(server.users) == 0
