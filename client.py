import argparse
import base64
import binascii
import copy
import hashlib
import json
import logging
import math
import re
import socket
import threading
import time
import uuid
import webbrowser
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from config import (
    SERVER_HOST, TCP_PORT, UDP_PORT, HEARTBEAT_INTERVAL,
    RETRY_TIMEOUT, MAX_RETRIES, WINDOW_SIZE, INITIAL_WINDOW_SIZE, CHUNK_SIZE,
    CUBIC_BETA, CUBIC_C, MAX_FILE_SIZE, MAX_HTTP_BODY, MAX_MESSAGE_LENGTH,
    MAX_CONTROL_LINE,
    TRANSFER_TIMEOUT,
)
from protocol import (
    Packet, ACK, HEARTBEAT, MSG, FILE_META, FILE_CHUNK, USER_LIST, FLAG_END,
    cipher, decode_packet, encode_packet, json_bytes, json_obj, new_id
)

logger = logging.getLogger(__name__)


class CollaborationClient:
    """Xử lý kết nối mạng, tin nhắn, truyền tệp và trạng thái giao diện."""

    def __init__(self, username, password, server_host):
        self.username = username
        self.server_addr = (socket.gethostbyname(server_host), UDP_PORT)
        self.users = []
        self.all_users = []
        self.messages = []
        self.transfers = []
        self.incoming = {}
        self.state_lock = threading.RLock()
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.congestion_window = INITIAL_WINDOW_SIZE
        self.congestion_lock = threading.Lock()
        self.cubic_epoch = None
        self.cubic_wmax = None
        self.received_packets = OrderedDict()
        self.seq = 1
        self.seq_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("0.0.0.0", 0))
        self.udp_port = self.udp.getsockname()[1]

        # TCP dùng cho đăng nhập/điều khiển; dữ liệu chat và file đi qua UDP.
        self.tcp = None
        self.tcp_file = None
        try:
            self.tcp = socket.create_connection((server_host, TCP_PORT), timeout=5)
            self.tcp_file = self.tcp.makefile("rb")
            hello = self.recv_json_line()
            if not hello or hello.get("type") != "HELLO":
                raise RuntimeError("bad server handshake")
            self.send_json_line({
                "type": "AUTH",
                "username": username,
                "password_token": cipher.encrypt(password.encode()).decode(),
                "udp_port": self.udp_port,
            })
            reply = self.recv_json_line()
            if not reply or reply.get("type") != "AUTH_OK":
                raise RuntimeError("authentication failed")
        except (OSError, ValueError, RuntimeError):
            if self.tcp_file:
                self.tcp_file.close()
            if self.tcp:
                self.tcp.close()
            self.udp.close()
            raise

        self.tcp.settimeout(None)
        threading.Thread(target=self.udp_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.tcp_ping_loop, daemon=True).start()

    def close(self):
        """Đóng an toàn tất cả socket và luồng của client."""
        self.stop_event.set()
        if self.tcp_file:
            try:
                self.tcp_file.close()
            except OSError:
                pass
        if self.tcp:
            try:
                self.tcp.close()
            except OSError:
                pass
        try:
            self.udp.close()
        except OSError:
            pass

    def next_seq(self):
        with self.seq_lock:
            value = self.seq
            self.seq += 1
            return value

    def send_json_line(self, obj):
        self.tcp.sendall((json.dumps(obj) + "\n").encode())

    def recv_json_line(self):
        line = self.tcp_file.readline(MAX_CONTROL_LINE + 1)
        if not line:
            return None
        if len(line) > MAX_CONTROL_LINE or not line.endswith(b"\n"):
            raise ValueError("invalid control message length")
        value = json.loads(line.decode())
        if not isinstance(value, dict):
            raise ValueError("control message must be a JSON object")
        return value

    def ack(self, packet):
        """Xác nhận packet server gửi để server có thể kết thúc retry."""
        body = {"ack": packet.seq, "msg_id": packet.msg_id.hex}
        ack = Packet(ACK, self.next_seq(), new_id(), payload=json_bytes(body))
        self.udp.sendto(encode_packet(ack), self.server_addr)

    def mark_ack(self, packet):
        try:
            body = json_obj(packet.payload)
            key = (body["msg_id"], int(body["ack"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return
        with self.pending_lock:
            event = self.pending.get(key)
        if event:
            event.set()

    def update_congestion_window(self, loss):
        """Điều chỉnh giới hạn số packet đang bay theo kiểu CUBIC đơn giản."""
        with self.congestion_lock:
            now = time.monotonic()
            if loss:
                self.cubic_wmax = max(
                    self.congestion_window,
                    self.cubic_wmax or self.congestion_window,
                )
                self.congestion_window = max(
                    1,
                    math.floor(self.congestion_window * CUBIC_BETA),
                )
                self.cubic_epoch = now
                return

            if self.cubic_epoch is None or self.cubic_wmax is None:
                self.congestion_window = min(
                    WINDOW_SIZE,
                    self.congestion_window + 1,
                )
                return

            distance = max(0.0, self.cubic_wmax - self.congestion_window)
            k = (distance / CUBIC_C) ** (1 / 3)
            target = self.cubic_wmax + CUBIC_C * (now - self.cubic_epoch - k) ** 3
            self.congestion_window = min(
                WINDOW_SIZE,
                max(self.congestion_window + 1, math.ceil(target)),
            )

    def send_window(self, packets):
        """Duy trì cửa sổ trượt, nhận ACK độc lập và retry packet bị mất."""
        next_packet = 0
        in_flight = {}
        retransmitted = False

        try:
            while next_packet < len(packets) or in_flight:
                with self.congestion_lock:
                    window_size = self.congestion_window
                while next_packet < len(packets) and len(in_flight) < window_size:
                    packet = packets[next_packet]
                    next_packet += 1
                    key = (packet.msg_id.hex, packet.seq)
                    event = threading.Event()
                    raw = encode_packet(packet)
                    in_flight[key] = {
                        "packet": packet,
                        "event": event,
                        "raw": raw,
                        "attempts": 1,
                        "deadline": time.monotonic() + RETRY_TIMEOUT,
                    }
                    with self.pending_lock:
                        self.pending[key] = event
                    self.udp.sendto(raw, self.server_addr)

                now = time.monotonic()
                for key, state in list(in_flight.items()):
                    if state["event"].is_set():
                        in_flight.pop(key)
                        with self.pending_lock:
                            self.pending.pop(key, None)
                    elif now >= state["deadline"]:
                        if state["attempts"] >= MAX_RETRIES:
                            self.update_congestion_window(loss=True)
                            raise TimeoutError("UDP transfer timed out")
                        self.udp.sendto(state["raw"], self.server_addr)
                        state["attempts"] += 1
                        state["deadline"] = now + RETRY_TIMEOUT
                        retransmitted = True

                if in_flight:
                    time.sleep(0.005)
        finally:
            with self.pending_lock:
                for key in in_flight:
                    self.pending.pop(key, None)

        self.update_congestion_window(loss=retransmitted)

    def send_message(self, target, text):
        """Kiểm tra nội dung, tạo packet MSG và gửi tới server."""
        if not isinstance(target, str) or not isinstance(text, str):
            raise ValueError("message and recipient must be text")
        if not text.strip() or len(text) > MAX_MESSAGE_LENGTH:
            raise ValueError(f"message must contain 1 to {MAX_MESSAGE_LENGTH} characters")
        mid = new_id()
        packet = Packet(
            MSG,
            self.next_seq(),
            mid,
            flags=FLAG_END,
            payload=json_bytes({"to": target, "text": text}),
        )
        self.send_window([packet])
        with self.state_lock:
            self.messages.append({
                "from": self.username,
                "to": target,
                "text": text,
                "time": time.strftime("%H:%M:%S"),
                "timestamp": time.time(),
            })
            del self.messages[:-500]

    def prepare_file_transfer(self, target, name, data):
        """Kiểm tra tệp, tạo metadata/checksum và đăng ký bản ghi tiến trình."""
        if not isinstance(target, str) or not target:
            raise ValueError("recipient must be provided")
        if not isinstance(name, str):
            raise ValueError("file name must be text")
        if not isinstance(data, bytes) or len(data) > MAX_FILE_SIZE:
            raise ValueError(f"file must be no larger than {MAX_FILE_SIZE} bytes")
        safe_name = Path(name.replace("\\", "/")).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("invalid file name")

        file_uuid = uuid.uuid4()
        file_id = file_uuid.hex
        digest = hashlib.sha256(data).hexdigest()
        chunks = [
            data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)
        ] or [b""]
        total = len(chunks)
        if total > 65535:
            raise ValueError("file requires too many UDP chunks")

        record = {
            "direction": "send",
            "name": safe_name,
            "size": len(data),
            "peer": target,
            "from": getattr(self, "username", "?"),
            "to": target,
            "status": "sending",
            "progress": 0,
            "file_id": file_id,
            "time": time.strftime("%H:%M:%S"),
            "timestamp": time.time(),
        }
        with self.state_lock:
            self.transfers.append(record)
            del self.transfers[:-200]

        return {
            "file_uuid": file_uuid,
            "file_id": file_id,
            "target": target,
            "name": safe_name,
            "size": len(data),
            "sha256": digest,
            "chunks": chunks,
            "total": total,
            "record": record,
        }

    def _send_prepared_file(self, transfer):
        """Thực hiện truyền metadata và từng cửa sổ chunk qua UDP."""
        record = transfer["record"]
        file_uuid = transfer["file_uuid"]
        total = transfer["total"]

        try:
            meta = Packet(
                FILE_META,
                self.next_seq(),
                file_uuid,
                payload=json_bytes({
                    "to": transfer["target"],
                    "file_id": transfer["file_id"],
                    "name": transfer["name"],
                    "size": transfer["size"],
                    "sha256": transfer["sha256"],
                    "total": total,
                }),
            )
            self.send_window([meta])

            packets = [
                Packet(
                    FILE_CHUNK,
                    self.next_seq(),
                    file_uuid,
                    chunk=idx,
                    total=total,
                    flags=FLAG_END if idx == total - 1 else 0,
                    payload=chunk,
                )
                for idx, chunk in enumerate(transfer["chunks"])
            ]
            for start in range(0, len(packets), WINDOW_SIZE):
                batch = packets[start:start + WINDOW_SIZE]
                self.send_window(batch)
                with self.state_lock:
                    record["progress"] = min(
                        100,
                        int((start + len(batch)) * 100 / len(packets)),
                    )
        except (OSError, TimeoutError, ValueError) as exc:
            with self.state_lock:
                record["status"] = "failed"
                record["error"] = str(exc)
            logger.exception("File transfer of %s failed", transfer["name"])
            return

        with self.state_lock:
            record["progress"] = 100
            record["status"] = "sent to server"

    def send_file_bytes(self, target, name, data):
        """Tạo metadata/checksum, chia file thành chunk rồi gửi có ACK."""
        transfer = self.prepare_file_transfer(target, name, data)
        self._send_prepared_file(transfer)

    def handle_file_meta(self, packet, body):
        """Kiểm tra metadata và khởi tạo trạng thái nhận file."""
        fid = body.get("file_id")
        name = body.get("name")
        size = body.get("size")
        total = body.get("total")
        digest = body.get("sha256")
        if (
            not isinstance(fid, str)
            or not re.fullmatch(r"[0-9a-f]{32}", fid)
            or packet.msg_id.hex != fid
            or not isinstance(name, str)
            or not name
            or not isinstance(size, int)
            or isinstance(size, bool)
            or not 0 <= size <= MAX_FILE_SIZE
            or not isinstance(total, int)
            or isinstance(total, bool)
            or total != max(1, (size + CHUNK_SIZE - 1) // CHUNK_SIZE)
            or not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise ValueError("invalid file metadata")

        safe_name = Path(name.replace("\\", "/")).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("invalid file name")
        sender = body.get("from", "?")
        target = body.get("to", "*")
        if not isinstance(sender, str) or not sender:
            sender = "?"
        if not isinstance(target, str) or not target:
            target = "*"
        with self.state_lock:
            if fid in self.incoming:
                return
            self.incoming[fid] = {
                "name": safe_name,
                "size": size,
                "sha256": digest,
                "total": total,
                "from": sender,
                "to": target,
                "chunks": {},
                "last_activity": time.monotonic(),
            }
            self.transfers.append({
                "direction": "receive",
                "name": safe_name,
                "size": size,
                "peer": sender,
                "from": sender,
                "to": target,
                "status": "receiving",
                "progress": 0,
                "file_id": fid,
                "time": time.strftime("%H:%M:%S"),
                "timestamp": time.time(),
            })
            del self.transfers[:-200]

    def handle_file_chunk(self, packet):
        """Lưu chunk theo index, ghép file và kiểm tra SHA-256 khi đủ."""
        fid = packet.msg_id.hex
        with self.state_lock:
            item = self.incoming.get(fid)
            if not item:
                raise ValueError("file chunk arrived before metadata")
            idx = packet.chunk
            if packet.total != item["total"] or idx >= item["total"]:
                raise ValueError("invalid file chunk index")
            if bool(packet.flags & FLAG_END) != (idx == item["total"] - 1):
                raise ValueError("invalid file end flag")
            expected_size = min(
                CHUNK_SIZE,
                max(0, item["size"] - idx * CHUNK_SIZE),
            )
            if len(packet.payload) != expected_size:
                raise ValueError("invalid file chunk size")
            item["chunks"].setdefault(idx, packet.payload)
            item["last_activity"] = time.monotonic()
            progress = int(len(item["chunks"]) * 100 / item["total"])
            for record in self.transfers:
                if record.get("file_id") == fid:
                    record["progress"] = progress

            if len(item["chunks"]) != item["total"]:
                return
            data = b"".join(item["chunks"][i] for i in range(item["total"]))
            if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
                for record in self.transfers:
                    if record.get("file_id") == fid:
                        record["status"] = "hash mismatch"
                        record["progress"] = 100
                self.incoming.pop(fid, None)
                return

            download_dir = Path("downloads")
            try:
                download_dir.mkdir(exist_ok=True)
                out = download_dir / item["name"]
                stem, suffix = out.stem, out.suffix
                counter = 1
                while out.exists():
                    out = download_dir / f"{stem} ({counter}){suffix}"
                    counter += 1
                out.write_bytes(data)
            except (OSError, ValueError) as exc:
                for record in self.transfers:
                    if record.get("file_id") == fid:
                        record["status"] = "failed"
                        record["error"] = str(exc)
                self.incoming.pop(fid, None)
                logger.exception("Unable to save received file %s", item["name"])
                return
            for record in self.transfers:
                if record.get("file_id") == fid:
                    record["status"] = "done"
                    record["progress"] = 100
                    record["path"] = str(out)
            self.incoming.pop(fid, None)

    def udp_loop(self):
        """Nhận packet từ server, cập nhật state và ACK packet hợp lệ."""
        while not self.stop_event.is_set():
            try:
                raw, addr = self.udp.recvfrom(65535)
                packet = decode_packet(raw)
            except OSError as exc:
                if not self.stop_event.is_set():
                    logger.warning("UDP receive failed: %s", exc)
                    self.stop_event.set()
                break
            except ValueError as exc:
                logger.warning("Discarding invalid UDP packet: %s", exc)
                continue

            if addr != self.server_addr:
                continue
            if packet.ptype == ACK:
                self.mark_ack(packet)
                continue

            try:
                packet_key = (packet.msg_id.hex, packet.seq)
                if packet_key in self.received_packets:
                    self.ack(packet)
                    continue

                if packet.ptype == USER_LIST:
                    body = json_obj(packet.payload)
                    users = body.get("users")
                    all_users = body.get("all_users")
                    if not isinstance(users, list) or not all(
                        isinstance(user, str) for user in users
                    ):
                        raise ValueError("invalid user list")
                    if all_users is not None and (
                        not isinstance(all_users, list)
                        or not all(isinstance(user, str) for user in all_users)
                    ):
                        raise ValueError("invalid all_users list")
                    with self.state_lock:
                        self.users = users
                        if all_users is not None:
                            self.all_users = all_users
                elif packet.ptype == MSG:
                    if not packet.flags & FLAG_END:
                        raise ValueError("message packet is missing end flag")
                    body = json_obj(packet.payload)
                    text = body.get("text")
                    if not isinstance(text, str) or len(text) > MAX_MESSAGE_LENGTH:
                        raise ValueError("invalid message")
                    with self.state_lock:
                        self.messages.append({
                            "from": body.get("from", "?"),
                            "to": body.get("to", "*"),
                            "text": text,
                            "time": time.strftime("%H:%M:%S"),
                            "timestamp": time.time(),
                        })
                        del self.messages[:-500]
                elif packet.ptype == FILE_META:
                    self.handle_file_meta(packet, json_obj(packet.payload))
                elif packet.ptype == FILE_CHUNK:
                    self.handle_file_chunk(packet)
                else:
                    raise ValueError("unsupported packet type")

                self.received_packets[packet_key] = time.monotonic()
                while len(self.received_packets) > 4096:
                    self.received_packets.popitem(last=False)
                self.ack(packet)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError, OSError) as exc:
                logger.warning("Rejected packet from server: %s", exc)

    def heartbeat_loop(self):
        """Định kỳ báo server client còn hoạt động và hết hạn transfer dở."""
        while not self.stop_event.wait(HEARTBEAT_INTERVAL):
            self.expire_incoming_transfers()
            packet = Packet(
                HEARTBEAT,
                self.next_seq(),
                new_id(),
                payload=json_bytes({"username": self.username}),
            )
            try:
                self.udp.sendto(encode_packet(packet), self.server_addr)
            except OSError:
                logger.exception("Unable to send heartbeat")
                self.stop_event.set()
                break

    def expire_incoming_transfers(self):
        """Đánh dấu transfer nhận dở đã timeout và bỏ dữ liệu tạm."""
        cutoff = time.monotonic() - TRANSFER_TIMEOUT
        with self.state_lock:
            stale = [
                file_id
                for file_id, item in self.incoming.items()
                if item["last_activity"] < cutoff
            ]
            for file_id in stale:
                self.incoming.pop(file_id, None)
                for record in self.transfers:
                    if record.get("file_id") == file_id:
                        record["status"] = "timed out"

    def tcp_ping_loop(self):
        """Giữ kết nối TCP điều khiển và phát hiện server ngắt kết nối."""
        while not self.stop_event.wait(4):
            try:
                self.send_json_line({"type": "PING"})
                if not self.recv_json_line():
                    raise ConnectionError("server closed the control connection")
            except (OSError, ValueError, json.JSONDecodeError, ConnectionError) as exc:
                logger.warning("Control connection lost: %s", exc)
                self.stop_event.set()
                break

    def state(self):
        """Trả bản sao an toàn của state cho HTTP UI đọc."""
        with self.state_lock:
            all_users = sorted(set(getattr(self, "all_users", [])) | set(self.users))
            return copy.deepcopy({
                "username": self.username,
                "connection": "disconnected" if self.stop_event.is_set() else "connected",
                "users": self.users,
                "all_users": all_users,
                "messages": self.messages[-100:],
                "transfers": self.transfers[-50:],
            })


class UIHandler(BaseHTTPRequestHandler):
    """Phục vụ trang UI, đăng nhập và các API cục bộ cho client Python."""

    client_app = None
    default_server = SERVER_HOST
    app_lock = threading.Lock()
    static_dir = Path(__file__).with_name("static")
    index_file = static_dir / "index.html"
    login_file = static_dir / "login.html"

    def reply(self, code, data, ctype="application/json"):
        body = data if isinstance(data, bytes) else data.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def read_json(self):
        """Đọc JSON request có giới hạn kích thước và yêu cầu object."""
        size = int(self.headers.get("Content-Length", "0"))
        if size < 0:
            raise ValueError("invalid Content-Length")
        if size > MAX_HTTP_BODY:
            raise OverflowError("request body is too large")
        body = json.loads(self.rfile.read(size) or b"{}")
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

    def do_GET(self):
        """Trả trang đăng nhập, giao diện chính, tài nguyên tĩnh hoặc state client."""
        clean_path = self.path.split("?")[0]
        with UIHandler.app_lock:
            app = UIHandler.client_app

        if clean_path in ("/", "/index.html", "/static/index.html"):
            if app is None:
                self.redirect("/login")
            else:
                self.reply(200, self.index_file.read_bytes(), "text/html; charset=utf-8")
        elif clean_path in ("/login", "/login.html", "/static/login.html"):
            if app is not None:
                self.redirect("/")
            elif self.login_file.exists():
                self.reply(200, self.login_file.read_bytes(), "text/html; charset=utf-8")
            else:
                self.reply(404, b"Not Found", "text/plain")
        elif clean_path in ("/style.css", "/static/style.css"):
            css_path = self.static_dir / "style.css"
            if css_path.exists():
                self.reply(200, css_path.read_bytes(), "text/css; charset=utf-8")
            else:
                self.reply(404, b"Not Found", "text/plain")
        elif clean_path in ("/app.js", "/static/app.js"):
            js_path = self.static_dir / "app.js"
            if js_path.exists():
                self.reply(200, js_path.read_bytes(), "application/javascript; charset=utf-8")
            else:
                self.reply(404, b"Not Found", "text/plain")
        elif clean_path == "/api/auth":
            self.reply(200, json.dumps({
                "authenticated": app is not None,
                "username": app.username if app is not None else None,
                "default_server": UIHandler.default_server,
            }))
        elif clean_path == "/api/state":
            if app is None:
                self.reply(401, json.dumps({"authenticated": False, "error": "not authenticated"}))
                return
            self.reply(200, json.dumps(app.state()))
        else:
            self.reply(404, "{}")

    def do_POST(self):
        """Xử lý API đăng nhập/đăng xuất, gửi tin nhắn và yêu cầu gửi tệp."""
        try:
            body = self.read_json()
            clean_path = self.path.split("?")[0]
            if clean_path == "/api/login":
                username = body.get("username")
                password = body.get("password")
                server_host = body.get("server") or UIHandler.default_server
                if not isinstance(username, str) or not username.strip():
                    raise ValueError("Vui lòng nhập tên tài khoản.")
                if not isinstance(password, str) or not password:
                    raise ValueError("Vui lòng nhập mật khẩu.")
                if not isinstance(server_host, str) or not server_host.strip():
                    raise ValueError("Địa chỉ máy chủ không hợp lệ.")
                username = username.strip()
                server_host = server_host.strip()
                with UIHandler.app_lock:
                    old_app = UIHandler.client_app
                    if old_app is not None:
                        old_app.close()
                        UIHandler.client_app = None
                    try:
                        new_app = CollaborationClient(username, password, server_host)
                    except RuntimeError as exc:
                        if "authentication failed" in str(exc):
                            self.reply(401, json.dumps({
                                "ok": False,
                                "error": "Sai tài khoản, mật khẩu hoặc tài khoản đang được sử dụng.",
                            }))
                            return
                        raise
                    UIHandler.client_app = new_app
                self.reply(200, json.dumps({"ok": True, "username": new_app.username}))
                return
            if clean_path == "/api/logout":
                with UIHandler.app_lock:
                    old_app = UIHandler.client_app
                    UIHandler.client_app = None
                if old_app is not None:
                    old_app.close()
                self.reply(200, '{"ok":true}')
                return

            with UIHandler.app_lock:
                app = UIHandler.client_app
            if app is None:
                self.reply(401, json.dumps({
                    "ok": False,
                    "error": "Phiên đăng nhập đã hết hạn, vui lòng đăng nhập lại.",
                }))
                return

            if clean_path == "/api/send":
                app.send_message(body.get("to", "*"), body.get("text", ""))
                self.reply(200, '{"ok":true}')
            elif clean_path == "/api/file":
                encoded = body.get("data")
                name = body.get("name")
                target = body.get("to", "*")
                if not isinstance(encoded, str) or not isinstance(name, str):
                    raise ValueError("file name and data are required")
                if not isinstance(target, str) or not target:
                    raise ValueError("recipient is required")
                data = base64.b64decode(encoded, validate=True)
                if len(data) > MAX_FILE_SIZE:
                    raise OverflowError("file exceeds the maximum size")
                transfer = app.prepare_file_transfer(target, name, data)
                threading.Thread(
                    target=app._send_prepared_file,
                    args=(transfer,),
                    daemon=True,
                ).start()
                self.reply(200, json.dumps({"ok": True, "file_id": transfer["file_id"]}))
            else:
                self.reply(404, "{}")
        except OverflowError as exc:
            self.reply(413, json.dumps({"ok": False, "error": str(exc)}))
        except (
            ValueError,
            KeyError,
            TypeError,
            binascii.Error,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as exc:
            self.reply(400, json.dumps({"ok": False, "error": str(exc)}))
        except (OSError, TimeoutError, RuntimeError) as exc:
            logger.warning("Request failed because of a network error: %s", exc)
            self.reply(503, json.dumps({
                "ok": False,
                "error": "Không thể kết nối tới máy chủ. Vui lòng kiểm tra server và thử lại.",
            }))

    def log_message(self, format, *args):
        logger.info("HTTP " + format, *args)


def main():
    """Đọc tham số, khởi tạo HTTP UI cục bộ và kết nối client nếu có sẵn thông tin."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--server", default=SERVER_HOST)
    parser.add_argument("--ui-port", type=int, default=8000)
    args = parser.parse_args()

    if bool(args.username) != bool(args.password):
        parser.error("--username and --password must be provided together")

    logging.basicConfig(level=logging.INFO)
    ui = ThreadingHTTPServer(("127.0.0.1", args.ui_port), UIHandler)
    ui.daemon_threads = True
    UIHandler.default_server = args.server

    try:
        url = f"http://127.0.0.1:{args.ui_port}"
        if args.username and args.password:
            app = CollaborationClient(args.username, args.password, args.server)
            with UIHandler.app_lock:
                UIHandler.client_app = app
            print(f"Logged in as {args.username}")
            print(f"UDP port     : {app.udp_port}")
        else:
            print("Waiting for login via Web UI")
        print(f"Web UI       : {url}")
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
        ui.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        ui.server_close()
        with UIHandler.app_lock:
            active_app = UIHandler.client_app
            UIHandler.client_app = None
        if active_app is not None:
            active_app.close()


if __name__ == "__main__":
    main()
