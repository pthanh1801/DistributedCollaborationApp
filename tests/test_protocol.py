import uuid

import pytest

from protocol import (
    FILE_CHUNK,
    FLAG_END,
    MSG,
    Packet,
    decode_packet,
    encode_packet,
    json_bytes,
    json_obj,
)
from protocol import HEADER
from config import CHUNK_SIZE, MAX_UDP_PACKET_SIZE


def test_packet_roundtrip():
    packet = Packet(
        MSG, 42, uuid.uuid4(), flags=FLAG_END,
        payload=json_bytes({"text": "hello"}),
    )
    restored = decode_packet(encode_packet(packet))
    assert restored.ptype == MSG
    assert restored.seq == 42
    assert restored.msg_id == packet.msg_id
    assert json_obj(restored.payload)["text"] == "hello"


def test_decode_rejects_truncated_and_extra_payload_bytes():
    packet = Packet(MSG, 1, uuid.uuid4(), flags=FLAG_END, payload=b"payload")
    encoded = encode_packet(packet)

    with pytest.raises(ValueError, match="payload length"):
        decode_packet(encoded[:-1])
    with pytest.raises(ValueError, match="payload length"):
        decode_packet(encoded + b"x")


def test_decode_rejects_tampered_encrypted_payload():
    encoded = bytearray(encode_packet(Packet(
        MSG, 1, uuid.uuid4(), flags=FLAG_END, payload=b"secret",
    )))
    encoded[-1] ^= 1

    with pytest.raises(ValueError, match="encrypted payload"):
        decode_packet(bytes(encoded))


def test_packet_header_size_matches_documented_layout():
    assert HEADER.size == 33


def test_file_chunk_fits_safe_udp_datagram_size():
    packet = Packet(FILE_CHUNK, 1, uuid.uuid4(), chunk=0, total=1, flags=FLAG_END, payload=b"x" * CHUNK_SIZE)
    encoded = encode_packet(packet)

    assert len(encoded) <= MAX_UDP_PACKET_SIZE
    assert decode_packet(encoded).payload == packet.payload


def test_encode_rejects_packet_that_would_fragment():
    packet = Packet(MSG, 1, uuid.uuid4(), flags=FLAG_END, payload=b"x" * 2_000)

    with pytest.raises(ValueError, match="safe datagram size"):
        encode_packet(packet)
