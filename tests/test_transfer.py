import hashlib
import json
import threading
import time
import uuid
from types import SimpleNamespace

import pytest

from client import CollaborationClient
from config import CHUNK_SIZE, INITIAL_WINDOW_SIZE, WINDOW_SIZE
from protocol import FILE_CHUNK, FILE_META, FLAG_END, MSG, Packet, decode_packet, json_bytes
from server import CollaborationServer


def make_client():
    client = CollaborationClient.__new__(CollaborationClient)
    client.state_lock = threading.RLock()
    client.incoming = {}
    client.transfers = []
    return client


def make_server(monkeypatch):
    server = CollaborationServer.__new__(CollaborationServer)
    server.users = {
        "alice": {"udp": ("127.0.0.1", 10001)},
        "bob": {"udp": ("127.0.0.1", 10002)},
    }
    server.users_lock = threading.Lock()
    server.transfer_lock = threading.Lock()
    server.transfers = {}
    server.seen_metadata = {}
    server.seen_messages = {}
    queued = []
    monkeypatch.setattr(
        server,
        "queue_for_recipients",
        lambda sender, target, packet: queued.append((sender, target, packet)),
    )
    return server, queued


def test_file_chunks_can_arrive_out_of_order_and_reassemble(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = make_client()
    content = b"a" * CHUNK_SIZE + b"last chunk"
    file_id = uuid.uuid4()
    total = 2
    meta = {
        "file_id": file_id.hex,
        "name": "example.bin",
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "total": total,
        "from": "alice",
    }

    client.handle_file_meta(Packet(FILE_META, 1, file_id), meta)
    client.handle_file_chunk(Packet(
        FILE_CHUNK, 3, file_id, chunk=1, total=total, flags=FLAG_END,
        payload=content[CHUNK_SIZE:],
    ))
    client.handle_file_chunk(Packet(FILE_CHUNK, 2, file_id, chunk=0, total=total, payload=content[:CHUNK_SIZE]))

    assert (tmp_path / "downloads" / "example.bin").read_bytes() == content
    assert client.transfers[0]["status"] == "done"


def test_duplicate_metadata_does_not_discard_received_chunks():
    client = make_client()
    file_id = uuid.uuid4()
    content = b"chunk"
    metadata = {
        "file_id": file_id.hex,
        "name": "example.bin",
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "total": 1,
    }

    packet = Packet(FILE_META, 1, file_id)
    client.handle_file_meta(packet, metadata)
    client.incoming[file_id.hex]["chunks"][0] = content
    client.handle_file_meta(packet, metadata)

    assert client.incoming[file_id.hex]["chunks"] == {0: content}
    assert len(client.transfers) == 1


def test_incomplete_file_transfer_times_out():
    client = make_client()
    file_id = uuid.uuid4()
    content = b"file data"
    meta = {
        "file_id": file_id.hex,
        "name": "incomplete.bin",
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "total": 1,
    }
    client.handle_file_meta(Packet(FILE_META, 1, file_id), meta)
    client.incoming[file_id.hex]["last_activity"] = time.monotonic() - 60

    client.expire_incoming_transfers()

    assert file_id.hex not in client.incoming
    assert client.transfers[0]["status"] == "timed out"


def test_server_routes_raw_file_chunks_using_metadata_target(monkeypatch):
    server, queued = make_server(monkeypatch)
    content = b"raw chunk"
    file_id = uuid.uuid4()
    metadata = {
        "to": "bob",
        "file_id": file_id.hex,
        "name": "x.bin",
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "total": 1,
    }

    server.forward("alice", Packet(FILE_META, 1, file_id, payload=json_bytes(metadata)))
    server.forward(
        "alice",
        Packet(FILE_CHUNK, 2, file_id, chunk=0, total=1, flags=FLAG_END, payload=content),
    )

    assert [target for _, target, _ in queued] == ["bob", "bob"]
    assert queued[1][2].payload == content
    assert json.loads(queued[0][2].payload)["from"] == "alice"


def test_server_deduplicates_retried_message(monkeypatch):
    server, queued = make_server(monkeypatch)
    packet = Packet(
        MSG, 5, uuid.uuid4(), flags=FLAG_END,
        payload=json_bytes({"to": "bob", "text": "hello"}),
    )

    server.forward("alice", packet)
    server.forward("alice", packet)

    assert len(queued) == 1


def test_server_rejects_file_chunk_with_wrong_size(monkeypatch):
    server, _ = make_server(monkeypatch)
    file_id = uuid.uuid4()
    metadata = {
        "to": "bob",
        "file_id": file_id.hex,
        "name": "x.bin",
        "size": CHUNK_SIZE + 1,
        "sha256": hashlib.sha256(b"x" * (CHUNK_SIZE + 1)).hexdigest(),
        "total": 2,
    }
    server.forward("alice", Packet(FILE_META, 1, file_id, payload=json_bytes(metadata)))

    with pytest.raises(ValueError, match="file chunk"):
        server.forward(
            "alice",
            Packet(FILE_CHUNK, 2, file_id, chunk=0, total=2, payload=b"short"),
        )


def test_congestion_window_grows_and_reduces_after_loss():
    client = CollaborationClient.__new__(CollaborationClient)
    client.congestion_lock = threading.Lock()
    client.congestion_window = INITIAL_WINDOW_SIZE
    client.cubic_epoch = None
    client.cubic_wmax = None

    client.update_congestion_window(loss=False)
    assert client.congestion_window == INITIAL_WINDOW_SIZE + 1

    client.update_congestion_window(loss=True)
    assert client.congestion_window < INITIAL_WINDOW_SIZE + 1

    for _ in range(WINDOW_SIZE * 2):
        client.update_congestion_window(loss=False)
    assert client.congestion_window <= WINDOW_SIZE


def test_send_window_slides_when_an_earlier_packet_is_acked(monkeypatch):
    client = CollaborationClient.__new__(CollaborationClient)
    client.congestion_lock = threading.Lock()
    client.congestion_window = 2
    client.cubic_epoch = None
    client.cubic_wmax = None
    client.pending = {}
    client.pending_lock = threading.Lock()
    client.server_addr = ("127.0.0.1", 9001)
    client.udp = SimpleNamespace()
    sent_sequences = []
    second_acked = threading.Event()
    third_sent_before_second_ack = []

    def sendto(raw, _addr):
        packet = decode_packet(raw)
        sent_sequences.append(packet.seq)
        if packet.seq == 3:
            third_sent_before_second_ack.append(not second_acked.is_set())
        if packet.seq == 1:
            def acknowledge_first():
                time.sleep(0.03)
                with client.pending_lock:
                    client.pending[(packet.msg_id.hex, packet.seq)].set()

            threading.Thread(target=acknowledge_first, daemon=True).start()
        elif packet.seq == 2:
            def acknowledge_second():
                time.sleep(0.08)
                with client.pending_lock:
                    client.pending[(packet.msg_id.hex, packet.seq)].set()
                second_acked.set()

            threading.Thread(target=acknowledge_second, daemon=True).start()
        elif packet.seq == 3:
            with client.pending_lock:
                client.pending[(packet.msg_id.hex, packet.seq)].set()

    client.udp.sendto = sendto
    packets = [
        Packet(MSG, seq, uuid.uuid4(), flags=FLAG_END, payload=b"message")
        for seq in (1, 2, 3)
    ]
    client.send_window(packets)

    assert third_sent_before_second_ack == [True]
    assert sent_sequences[:3] == [1, 2, 3]
