import json
import struct
import uuid
from dataclasses import dataclass

from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken

from config import FERNET_KEY, MAX_UDP_PACKET_SIZE

MAGIC = b"NP"
VERSION = 1

# Mã loại gói để client và server diễn giải payload thống nhất.
HEARTBEAT = 1
MSG = 2
FILE_META = 3
FILE_CHUNK = 4
ACK = 5
USER_LIST = 6

# Các cờ được lưu trong trường flags của header UDP.
FLAG_ENCRYPTED = 1
FLAG_END = 2

# Header nhị phân 33 byte; dấu ! quy định thứ tự byte chuẩn của mạng.
HEADER = struct.Struct("!2sBBB I 16s H H I")
cipher = Fernet(FERNET_KEY.encode())


@dataclass
class Packet:
    """Biểu diễn một packet trước khi mã hóa hoặc sau khi giải mã."""
    ptype: int
    seq: int
    msg_id: uuid.UUID
    chunk: int = 0
    total: int = 1
    flags: int = 0
    payload: bytes = b""


def encode_packet(packet: Packet, encrypt=True) -> bytes:
    """Mã hóa payload và ghép header với nội dung để gửi qua UDP."""
    payload = packet.payload
    flags = packet.flags
    if payload and not encrypt:
        raise ValueError("UDP payloads must be encrypted")
    if encrypt and payload:
        payload = cipher.encrypt(payload)
        flags |= FLAG_ENCRYPTED
    if HEADER.size + len(payload) > MAX_UDP_PACKET_SIZE:
        raise ValueError("UDP packet exceeds the safe datagram size")
    header = HEADER.pack(
        MAGIC,
        VERSION,
        packet.ptype,
        flags,
        packet.seq,
        packet.msg_id.bytes,
        packet.chunk,
        packet.total,
        len(payload),
    )
    return header + payload


def decode_packet(data: bytes) -> Packet:
    """Kiểm tra cấu trúc datagram, giải mã payload và trả về Packet."""
    if len(data) > MAX_UDP_PACKET_SIZE:
        raise ValueError("UDP packet exceeds the safe datagram size")
    if len(data) < HEADER.size:
        raise ValueError("short packet")
    magic, version, ptype, flags, seq, mid, chunk, total, size = HEADER.unpack(
        data[:HEADER.size]
    )
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid packet")
    if len(data) != HEADER.size + size:
        raise ValueError("invalid payload length")
    if ptype not in (HEARTBEAT, MSG, FILE_META, FILE_CHUNK, ACK, USER_LIST):
        raise ValueError("unknown packet type")
    if flags & ~(FLAG_ENCRYPTED | FLAG_END):
        raise ValueError("unknown packet flags")
    if total == 0 or (ptype == FILE_CHUNK and chunk >= total):
        raise ValueError("invalid chunk coordinates")
    if ptype == MSG and (
        not flags & FLAG_END or chunk != 0 or total != 1
    ):
        raise ValueError("invalid message packet flags")
    if ptype == FILE_CHUNK and bool(flags & FLAG_END) != (chunk == total - 1):
        raise ValueError("invalid file end flag")
    if ptype not in (MSG, FILE_CHUNK) and flags & FLAG_END:
        raise ValueError("unexpected end flag")
    payload = data[HEADER.size:]
    if payload and not flags & FLAG_ENCRYPTED:
        raise ValueError("unencrypted payload")
    if flags & FLAG_ENCRYPTED and payload:
        try:
            payload = cipher.decrypt(payload)
        except InvalidToken as exc:
            raise ValueError("invalid encrypted payload") from exc
    return Packet(ptype, seq, uuid.UUID(bytes=mid), chunk, total, flags, payload)


def json_bytes(obj) -> bytes:
    """Chuyển object Python thành JSON dạng bytes cho payload."""
    return json.dumps(obj, separators=(",", ":")).encode()


def json_obj(payload: bytes):
    """Đọc payload JSON và chỉ chấp nhận object/dictionary."""
    value = json.loads(payload.decode())
    if not isinstance(value, dict):
        raise ValueError("JSON payload must be an object")
    return value


def new_id():
    """Tạo ID duy nhất cho một tin nhắn hoặc lần truyền tệp."""
    return uuid.uuid4()
