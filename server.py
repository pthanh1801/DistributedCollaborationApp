import json
import logging
import queue
import re
import socket
import threading
import time
from pathlib import Path

from config import (
    CHUNK_SIZE,
    CLIENT_TIMEOUT,
    MAX_FILE_SIZE,
    MAX_MESSAGE_LENGTH,
    MAX_RETRIES,
    MAX_CONTROL_LINE,
    RETRY_TIMEOUT,
    TCP_PORT,
    UDP_PORT,
    WINDOW_SIZE,
)
from cryptography.fernet import InvalidToken
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

HOST = "0.0.0.0"
logger = logging.getLogger(__name__)


class CollaborationServer:
    """Quản lý đăng nhập, danh sách người dùng và định tuyến UDP."""

    def __init__(self):
        self.users = {}
        self.users_lock = threading.Lock()
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.transfers = {}
        self.transfer_lock = threading.Lock()
        self.seen_metadata = {}
        self.seen_messages = {}
        self.seq = 1
        self.seq_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.credentials = json.loads(Path("users.json").read_text(encoding="utf-8"))

        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((HOST, TCP_PORT))
        self.tcp.listen(32)

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((HOST, UDP_PORT))

    def next_seq(self):
        with self.seq_lock:
            value = self.seq
            self.seq += 1
            return value

    @staticmethod
    def send_json_line(conn, obj):
        conn.sendall((json.dumps(obj) + "\n").encode())

    @staticmethod
    def recv_json_line(file):
        line = file.readline(MAX_CONTROL_LINE + 1)
        if not line:
            return None
        if len(line) > MAX_CONTROL_LINE or not line.endswith(b"\n"):
            raise ValueError("invalid control message length")
        value = json.loads(line.decode())
        if not isinstance(value, dict):
            raise ValueError("control message must be a JSON object")
        return value

    def handle_tcp(self, conn, addr):
        """Thực hiện handshake/auth và giữ kết nối TCP điều khiển của user."""
        username = None
        file = conn.makefile("rb")
        try:
            self.send_json_line(conn, {"type": "HELLO", "server": "collab-demo"})
            req = self.recv_json_line(file)
            if not req or req.get("type") != "AUTH":
                return

            username = req.get("username", "")
            try:
                token = req.get("password_token")
                if not isinstance(token, str):
                    raise ValueError("missing password token")
                password = cipher.decrypt(token.encode()).decode()
                udp_port = int(req.get("udp_port", 0))
            except (KeyError, TypeError, ValueError, UnicodeDecodeError, InvalidToken):
                self.send_json_line(conn, {"type": "AUTH_FAIL"})
                return

            if (
                not isinstance(username, str)
                or not username
                or self.credentials.get(username) != password
                or not 1 <= udp_port <= 65535
            ):
                self.send_json_line(conn, {"type": "AUTH_FAIL"})
                return

            with self.users_lock:
                duplicate = username in self.users
                if not duplicate:
                    self.users[username] = {
                        "tcp": conn,
                        "udp": (addr[0], udp_port),
                        "last_seen": time.time(),
                        "relay_queues": {},
                    }
            if duplicate:
                self.send_json_line(conn, {"type": "AUTH_FAIL"})
                return

            self.send_json_line(conn, {"type": "AUTH_OK", "username": username})
            self.broadcast_user_list()

            while not self.stop_event.is_set():
                req = self.recv_json_line(file)
                if req is None:
                    break
                if req.get("type") == "PING":
                    self.send_json_line(conn, {"type": "PONG"})
        except (OSError, ValueError, json.JSONDecodeError):
            logger.info("Control connection from %s closed", addr)
        finally:
            if username:
                removed = False
                with self.users_lock:
                    current = self.users.get(username)
                    if current and current.get("tcp") is conn:
                        self.users.pop(username, None)
                        removed = True
                if removed:
                    self.broadcast_user_list()
            try:
                file.close()
            except OSError:
                logger.exception("Unable to close control stream from %s", addr)
            try:
                conn.close()
            except OSError:
                logger.exception("Unable to close control connection from %s", addr)

    def tcp_loop(self):
        """Chấp nhận kết nối TCP; mỗi client được phục vụ trong thread riêng."""
        while not self.stop_event.is_set():
            try:
                conn, addr = self.tcp.accept()
                threading.Thread(
                    target=self.handle_tcp, args=(conn, addr), daemon=True
                ).start()
            except OSError as exc:
                if not self.stop_event.is_set():
                    logger.error("TCP accept failed: %s", exc)
                break

    def username_for_udp(self, addr):
        """Ánh xạ endpoint UDP đã đăng nhập về username tương ứng."""
        with self.users_lock:
            for name, info in self.users.items():
                if info["udp"] == addr:
                    return name
        return None

    def ack(self, addr, packet):
        """Gửi ACK chứa sequence và message ID của packet đã nhận."""
        payload = json_bytes({"ack": packet.seq, "msg_id": packet.msg_id.hex})
        ack_packet = Packet(ACK, self.next_seq(), new_id(), payload=payload)
        self.udp.sendto(encode_packet(ack_packet), addr)

    def mark_ack(self, addr, packet):
        try:
            body = json_obj(packet.payload)
            key = (addr, body["msg_id"], int(body["ack"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return
        with self.pending_lock:
            event = self.pending.get(key)
        if event:
            event.set()

    def send_reliable_window(self, items):
        """Gửi một cửa sổ packet và chỉ truyền lại những packet thiếu ACK."""
        events = {}
        raws = {}
        for addr, packet, raw in items:
            key = (addr, packet.msg_id.hex, packet.seq)
            event = threading.Event()
            events[key] = event
            raws[key] = raw
            with self.pending_lock:
                self.pending[key] = event

        try:
            for _ in range(MAX_RETRIES):
                for addr, packet, _ in items:
                    key = (addr, packet.msg_id.hex, packet.seq)
                    if not events[key].is_set():
                        self.udp.sendto(raws[key], addr)
                deadline = time.monotonic() + RETRY_TIMEOUT
                while time.monotonic() < deadline:
                    if all(event.is_set() for event in events.values()):
                        return True
                    time.sleep(0.01)
                if all(event.is_set() for event in events.values()):
                    return True
            failed = [key for key, event in events.items() if not event.is_set()]
            logger.warning("Delivery window timed out for %d packet(s)", len(failed))
            return False
        except OSError as exc:
            logger.warning("UDP delivery window failed: %s", exc)
            return False
        finally:
            with self.pending_lock:
                for key in events:
                    self.pending.pop(key, None)

    def recipients(self, target, sender):
        """Lấy endpoint của người nhận, loại người gửi khỏi broadcast."""
        with self.users_lock:
            if target == "*":
                return [
                    (name, info["udp"])
                    for name, info in self.users.items()
                    if name != sender
                ]
            if target in self.users and target != sender:
                info = self.users[target]
                return [(target, info["udp"])]
        return []

    def user_is_active(self, username):
        with self.users_lock:
            return username in self.users

    def queue_for_recipients(self, sender, target, packet):
        """Đưa packet vào hàng đợi riêng của từng người nhận/luồng."""
        targets = self.recipients(target, sender)
        for username, addr in targets:
            forwarded = Packet(
                packet.ptype,
                self.next_seq(),
                packet.msg_id,
                packet.chunk,
                packet.total,
                flags=packet.flags & FLAG_END,
                payload=packet.payload,
            )
            encode_packet(forwarded)
            start_worker = False
            with self.users_lock:
                info = self.users.get(username)
                if not info or info["udp"] != addr:
                    continue
                relay_queue = info["relay_queues"].get(packet.msg_id.hex)
                if relay_queue is None:
                    relay_queue = queue.Queue()
                    info["relay_queues"][packet.msg_id.hex] = relay_queue
                    start_worker = True
                relay_queue.put((addr, forwarded, encode_packet(forwarded)))
                conn = info["tcp"]
            if start_worker:
                threading.Thread(
                    target=self.relay_loop,
                    args=(username, conn, packet.msg_id.hex, relay_queue),
                    daemon=True,
                ).start()

    def forward(self, sender, packet):
        """Kiểm tra nội dung rồi định tuyến tin nhắn hoặc metadata/chunk tệp."""
        if packet.ptype == MSG:
            body = json_obj(packet.payload)
            if not packet.flags & FLAG_END:
                raise ValueError("message packet is missing end flag")
            target = body.get("to", "*")
            text = body.get("text")
            if (
                not isinstance(target, str)
                or not isinstance(text, str)
                or not text.strip()
                or len(text) > MAX_MESSAGE_LENGTH
            ):
                raise ValueError("invalid message")
            if target == sender or (
                target != "*" and not self.user_is_active(target)
            ):
                raise ValueError("unknown recipient")
            message_key = (sender, packet.msg_id.hex, packet.seq)
            with self.transfer_lock:
                self._expire_transfers()
                if message_key in self.seen_messages:
                    return
            body["from"] = sender
            self.queue_for_recipients(
                sender,
                target,
                Packet(
                    packet.ptype,
                    packet.seq,
                    packet.msg_id,
                    flags=packet.flags & FLAG_END,
                    payload=json_bytes(body),
                ),
            )
            with self.transfer_lock:
                self.seen_messages[message_key] = time.monotonic() + 600
            return

        if packet.ptype == FILE_META:
            body = json_obj(packet.payload)
            target = body.get("to", "*")
            name = body.get("name")
            size = body.get("size")
            total = body.get("total")
            if (
                packet.msg_id.hex != body.get("file_id")
                or not isinstance(target, str)
                or target == sender
                or (target != "*" and not self.user_is_active(target))
                or not isinstance(name, str)
                or not name.strip()
                or not isinstance(size, int)
                or isinstance(size, bool)
                or not 0 <= size <= MAX_FILE_SIZE
                or not isinstance(total, int)
                or isinstance(total, bool)
                or total != max(1, (size + CHUNK_SIZE - 1) // CHUNK_SIZE)
                or not isinstance(body.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", body["sha256"])
            ):
                raise ValueError("invalid file metadata")

            key = (sender, packet.msg_id.hex)
            with self.transfer_lock:
                self._expire_transfers()
                if key in self.seen_metadata:
                    return
                self.seen_metadata[key] = time.monotonic() + 600
                self.transfers[key] = {
                    "target": target,
                    "size": size,
                    "total": total,
                    "received": set(),
                    "created": time.monotonic(),
                }
            body["from"] = sender
            self.queue_for_recipients(
                sender,
                target,
                Packet(
                    packet.ptype,
                    packet.seq,
                    packet.msg_id,
                    packet.chunk,
                    packet.total,
                    payload=json_bytes(body),
                ),
            )
            return

        if packet.ptype == FILE_CHUNK:
            key = (sender, packet.msg_id.hex)
            with self.transfer_lock:
                transfer = self.transfers.get(key)
                if not transfer:
                    raise ValueError("unknown file transfer")
                index = packet.chunk
                total = transfer["total"]
                expected_size = min(
                    CHUNK_SIZE,
                    max(0, transfer["size"] - index * CHUNK_SIZE),
                )
                if (
                    packet.total != total
                    or index >= total
                    or len(packet.payload) != expected_size
                    or bool(packet.flags & FLAG_END) != (index == total - 1)
                ):
                    raise ValueError("invalid file chunk")
                if index in transfer["received"]:
                    return
                transfer["received"].add(index)
                target = transfer["target"]
            self.queue_for_recipients(sender, target, packet)
            return

        raise ValueError("unsupported packet type")

    def _expire_transfers(self):
        """Xóa trạng thái deduplication và transfer đã hết thời gian lưu."""
        now = time.monotonic()
        for key, expiry in list(self.seen_metadata.items()):
            if expiry <= now:
                self.seen_metadata.pop(key, None)
        for key, expiry in list(self.seen_messages.items()):
            if expiry <= now:
                self.seen_messages.pop(key, None)
        for key, transfer in list(self.transfers.items()):
            if now - transfer["created"] > 600:
                self.transfers.pop(key, None)

    def relay_loop(self, username, conn, stream_id, relay_queue):
        """Gửi hàng đợi theo cửa sổ ACK mà không chặn các luồng khác."""
        while not self.stop_event.is_set():
            try:
                addr, packet, raw = relay_queue.get(timeout=5)
            except queue.Empty:
                with self.users_lock:
                    current = self.users.get(username)
                    if not current or current["tcp"] is not conn:
                        return
                    if (
                        current["relay_queues"].get(stream_id) is relay_queue
                        and relay_queue.empty()
                    ):
                        current["relay_queues"].pop(stream_id, None)
                        return
                continue

            items = [(addr, packet, raw)]
            while len(items) < WINDOW_SIZE:
                try:
                    next_addr, next_packet, next_raw = relay_queue.get_nowait()
                except queue.Empty:
                    break
                items.append((next_addr, next_packet, next_raw))

            with self.users_lock:
                current = self.users.get(username)
                if not current or current["tcp"] is not conn:
                    return
            self.send_reliable_window(items)
            for _ in items:
                relay_queue.task_done()
            if packet.ptype == MSG:
                with self.users_lock:
                    current = self.users.get(username)
                    if (
                        current
                        and current["tcp"] is conn
                        and current["relay_queues"].get(stream_id) is relay_queue
                    ):
                        current["relay_queues"].pop(stream_id, None)
                return

    def send_user_list(self, addr):
        """Gửi danh sách username online và toàn bộ tài khoản hệ thống."""
        with self.users_lock:
            names = sorted(self.users.keys())
        all_names = sorted(self.credentials.keys())
        packet = Packet(
            USER_LIST,
            self.next_seq(),
            new_id(),
            payload=json_bytes({"users": names, "all_users": all_names}),
        )
        try:
            raw = encode_packet(packet)
        except ValueError:
            packet = Packet(
                USER_LIST,
                self.next_seq(),
                new_id(),
                payload=json_bytes({"users": names}),
            )
            raw = encode_packet(packet)
        self.udp.sendto(raw, addr)

    def broadcast_user_list(self):
        """Cập nhật danh sách online cho tất cả client đang đăng nhập."""
        with self.users_lock:
            targets = [info["udp"] for info in self.users.values()]
        for addr in targets:
            try:
                self.send_user_list(addr)
            except OSError:
                logger.exception("Unable to broadcast active user list")

    def udp_loop(self):
        """Nhận UDP, xác thực endpoint, xử lý heartbeat, ACK và dữ liệu."""
        while not self.stop_event.is_set():
            try:
                raw, addr = self.udp.recvfrom(65535)
                packet = decode_packet(raw)
            except OSError as exc:
                if not self.stop_event.is_set():
                    logger.warning("UDP receive failed: %s", exc)
                break
            except ValueError as exc:
                logger.warning("Discarding invalid UDP packet: %s", exc)
                continue

            if packet.ptype == ACK:
                self.mark_ack(addr, packet)
                continue

            sender = self.username_for_udp(addr)
            if not sender:
                continue

            try:
                if packet.ptype == HEARTBEAT:
                    self.ack(addr, packet)
                    with self.users_lock:
                        if sender in self.users:
                            self.users[sender]["last_seen"] = time.time()
                    self.send_user_list(addr)
                elif packet.ptype in (MSG, FILE_META, FILE_CHUNK):
                    self.forward(sender, packet)
                    self.ack(addr, packet)
                    with self.users_lock:
                        if sender in self.users:
                            self.users[sender]["last_seen"] = time.time()
            except (OSError, ValueError, KeyError, TypeError) as exc:
                logger.warning("Rejected UDP packet from %s: %s", addr, exc)

    def cleanup_loop(self):
        """Gỡ client không gửi heartbeat trong khoảng timeout quy định."""
        while not self.stop_event.wait(2):
            cutoff = time.time() - CLIENT_TIMEOUT
            with self.users_lock:
                stale = [
                    username
                    for username, info in self.users.items()
                    if info["last_seen"] < cutoff
                ]
                for username in stale:
                    try:
                        self.users[username]["tcp"].close()
                    except OSError:
                        logger.exception("Unable to close stale connection for %s", username)
                    self.users.pop(username, None)
            if stale:
                self.broadcast_user_list()

    def run(self):
        """Khởi chạy các vòng lặp TCP, UDP và dọn client stale."""
        print(f"TCP control : {HOST}:{TCP_PORT}")
        print(f"UDP stream : {HOST}:{UDP_PORT}")
        threading.Thread(target=self.tcp_loop, daemon=True).start()
        threading.Thread(target=self.udp_loop, daemon=True).start()
        threading.Thread(target=self.cleanup_loop, daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_event.set()
            self.tcp.close()
            self.udp.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CollaborationServer().run()
