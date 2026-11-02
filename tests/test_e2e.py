import hashlib
import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from client import CollaborationClient
from config import (
    CLIENT_TIMEOUT,
    HEARTBEAT_INTERVAL,
    SERVER_HOST,
    TCP_PORT,
    UDP_PORT,
)
from server import CollaborationServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_SERVER_HOST = "127.0.0.1"
TEST_PASSWORDS = {
    "alice": "alice123",
    "bob": "bob123",
    "carol": "carol123",
}


def wait_until(predicate, timeout=5.0, interval=0.05):
    """Wait until predicate() becomes true or raise AssertionError."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)

    raise AssertionError("Condition was not satisfied before timeout")


def free_tcp_port():
    """Return an available TCP port.

    This helper is kept for future test isolation. The current application
    reads TCP_PORT/UDP_PORT directly from config.py, so the E2E suite uses
    the configured ports.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def start_server():
    """Start CollaborationServer in a background thread."""
    server = CollaborationServer()

    thread = threading.Thread(
        target=server.run,
        name="e2e-test-server",
        daemon=True,
    )
    thread.start()

    # Give the TCP/UDP listener threads enough time to bind and start.
    time.sleep(0.2)

    return server, thread


def stop_server(server):
    """Best-effort cleanup for a test server."""
    # CollaborationServer currently does not expose a dedicated stop()
    # method, so close the underlying sockets.
    for sock_name in ("tcp", "udp"):
        sock = getattr(server, sock_name, None)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    # Close active client sockets owned by the server.
    for info in list(getattr(server, "users", {}).values()):
        tcp = info.get("tcp")
        if tcp is not None:
            try:
                tcp.close()
            except OSError:
                pass


def start_client(username):
    """Create and authenticate a real CollaborationClient."""
    password = TEST_PASSWORDS[username]

    client = CollaborationClient(
        username,
        password,
        TEST_SERVER_HOST,
    )

    # Give the UDP receiver/heartbeat threads a moment to start.
    time.sleep(0.1)

    return client


def close_client(client):
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


@pytest.fixture
def running_server():
    """Provide a real CollaborationServer for an E2E test."""
    server, thread = start_server()

    yield server

    stop_server(server)
    thread.join(timeout=1.0)


# ---------------------------------------------------------------------------
# TCP authentication
# ---------------------------------------------------------------------------

def test_real_tcp_authentication(running_server):
    """A real TCP connection can authenticate successfully."""
    client = None

    try:
        client = start_client("alice")

        wait_until(
            lambda: "alice" in running_server.users,
            timeout=3.0,
        )

        assert client.username == "alice"
        assert running_server.users["alice"]["udp"][1] == client.udp.getsockname()[1]
    finally:
        close_client(client)


# ---------------------------------------------------------------------------
# UDP heartbeat / discovery
# ---------------------------------------------------------------------------

def test_real_udp_heartbeat_and_user_discovery(running_server):
    """Authenticated clients receive a user list through real UDP traffic."""
    alice = None
    bob = None

    try:
        alice = start_client("alice")
        bob = start_client("bob")

        wait_until(
            lambda: "alice" in running_server.users
            and "bob" in running_server.users,
            timeout=3.0,
        )

        def both_clients_see_each_other():
            alice_users = alice.state().get("users", [])
            bob_users = bob.state().get("users", [])

            return (
                "bob" in alice_users
                and "alice" in bob_users
            )

        wait_until(
            both_clients_see_each_other,
            timeout=5.0,
        )

        assert "bob" in alice.state()["users"]
        assert "alice" in bob.state()["users"]
    finally:
        close_client(alice)
        close_client(bob)


# ---------------------------------------------------------------------------
# UDP messaging
# ---------------------------------------------------------------------------

def test_real_udp_broadcast_message(running_server):
    """A real UDP broadcast message reaches another authenticated client."""
    alice = None
    bob = None

    try:
        alice = start_client("alice")
        bob = start_client("bob")

        wait_until(
            lambda: "alice" in running_server.users
            and "bob" in running_server.users,
            timeout=3.0,
        )

        alice.send_message("*", "E2E broadcast test")

        def bob_received_message():
            messages = bob.state().get("messages", [])
            return any(
                message.get("text") == "E2E broadcast test"
                for message in messages
            )

        wait_until(bob_received_message, timeout=5.0)

        assert any(
            message.get("text") == "E2E broadcast test"
            for message in bob.state()["messages"]
        )
    finally:
        close_client(alice)
        close_client(bob)


def test_real_udp_unicast_message(running_server):
    """A real UDP unicast message reaches only its intended recipient."""
    alice = None
    bob = None
    carol = None

    try:
        alice = start_client("alice")
        bob = start_client("bob")
        carol = start_client("carol")

        wait_until(
            lambda: len(running_server.users) == 3,
            timeout=3.0,
        )

        alice.send_message("bob", "E2E unicast test")

        def bob_received_message():
            return any(
                message.get("text") == "E2E unicast test"
                for message in bob.state().get("messages", [])
            )

        wait_until(bob_received_message, timeout=5.0)

        assert any(
            message.get("text") == "E2E unicast test"
            for message in bob.state()["messages"]
        )

        # The message must not be delivered to Carol.
        assert not any(
            message.get("text") == "E2E unicast test"
            for message in carol.state().get("messages", [])
        )
    finally:
        close_client(alice)
        close_client(bob)
        close_client(carol)


# ---------------------------------------------------------------------------
# Reliable file transfer
# ---------------------------------------------------------------------------

def test_real_file_transfer_with_sha256(running_server, tmp_path, monkeypatch):
    """
    Transfer a real file over the running TCP/UDP application.

    The test replaces the client's download directory with pytest's temporary
    directory, then verifies the reconstructed file and SHA-256 digest.
    """
    alice = None
    bob = None

    try:
        alice = start_client("alice")
        bob = start_client("bob")

        wait_until(
            lambda: len(running_server.users) == 2,
            timeout=3.0,
        )

        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # send_file_bytes writes to the client's "downloads" directory.
        # Change the working directory only for the duration of this test.
        monkeypatch.chdir(tmp_path)

        data = (
            b"VNsafe / distributed streaming E2E test data\n"
            * 200
        )
        expected_hash = hashlib.sha256(data).hexdigest()

        alice.send_file_bytes(
            "bob",
            "e2e_test.bin",
            data,
        )

        def bob_finished_transfer():
            state = bob.state()
            transfers = state.get("transfers", {})

            return any(
                transfer.get("status") == "done"
                for transfer in transfers
            )

        wait_until(bob_finished_transfer, timeout=10.0)

        output = download_dir / "e2e_test.bin"

        wait_until(output.exists, timeout=3.0)

        received = output.read_bytes()

        assert received == data
        assert hashlib.sha256(received).hexdigest() == expected_hash

        done_transfers = [
            transfer
            for transfer in bob.state().get("transfers", [])
            if transfer.get("status") == "done"
        ]
        assert done_transfers
    finally:
        close_client(alice)
        close_client(bob)


# ---------------------------------------------------------------------------
# Authentication / connection lifecycle
# ---------------------------------------------------------------------------

def test_real_tcp_duplicate_username_rejected(running_server):
    """The server must reject a second authenticated client using the same username."""
    first = None
    second = None

    try:
        first = start_client("alice")

        wait_until(
            lambda: "alice" in running_server.users,
            timeout=3.0,
        )

        with pytest.raises(Exception):
            second = CollaborationClient(
                "alice",
                TEST_PASSWORDS["alice"],
                TEST_SERVER_HOST,
            )
    finally:
        close_client(first)
        close_client(second)


def test_real_client_disconnect_removes_user(running_server):
    """Closing a real client removes its authenticated session from the server."""
    alice = None

    try:
        alice = start_client("alice")

        wait_until(
            lambda: "alice" in running_server.users,
            timeout=3.0,
        )

        close_client(alice)
        alice = None

        wait_until(
            lambda: "alice" not in running_server.users,
            timeout=max(3.0, CLIENT_TIMEOUT + 1.0),
        )

        assert "alice" not in running_server.users
    finally:
        close_client(alice)
