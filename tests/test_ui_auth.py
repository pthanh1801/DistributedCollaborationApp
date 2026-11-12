import io
import json
from email.message import Message

import pytest

import client as client_module
from client import UIHandler


class DummyClient:
    def __init__(self, username, password, server_host):
        self.username = username
        self.password = password
        self.server_host = server_host
        self.closed = False

    def state(self):
        return {
            "username": self.username,
            "connection": "connected",
            "users": [self.username],
            "messages": [],
            "transfers": [],
        }

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def reset_ui_handler():
    UIHandler.client_app = None
    UIHandler.default_server = "127.0.0.1"
    yield
    UIHandler.client_app = None


def dispatch(method, path, body=None):
    handler = UIHandler.__new__(UIHandler)
    handler.path = path
    handler.command = method
    handler.request_version = "HTTP/1.1"
    handler.requestline = f"{method} {path} HTTP/1.1"
    handler.close_connection = True
    handler._headers_buffer = []

    raw_body = json.dumps(body).encode() if body is not None else b""
    headers = Message()
    if body is not None:
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(raw_body))

    handler.headers = headers
    handler.rfile = io.BytesIO(raw_body)
    handler.wfile = io.BytesIO()
    handler.client_address = ("127.0.0.1", 12345)

    if method == "GET":
        handler.do_GET()
    else:
        handler.do_POST()

    raw_response = handler.wfile.getvalue()
    head, _, payload = raw_response.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    resp_headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            resp_headers[k.strip()] = v.strip()
    return status, resp_headers, payload


def test_unauthenticated_root_redirects_to_login_and_api_returns_401():
    status, headers, _ = dispatch("GET", "/")
    assert status == 302
    assert headers.get("Location") == "/login"

    status, _, raw = dispatch("GET", "/login")
    assert status == 200
    assert b"login-form" in raw

    status, _, raw = dispatch("GET", "/api/auth")
    assert status == 200
    assert json.loads(raw) == {
        "authenticated": False,
        "username": None,
        "default_server": "127.0.0.1",
    }

    status, _, raw = dispatch("GET", "/api/state")
    assert status == 401
    assert json.loads(raw)["authenticated"] is False

    status, _, _ = dispatch("POST", "/api/send", {"to": "*", "text": "hi"})
    assert status == 401


def test_login_logout_flow_and_invalid_credentials(monkeypatch):
    def fake_client_factory(username, password, server_host):
        if username == "alice" and password == "alice123":
            return DummyClient(username, password, server_host)
        raise RuntimeError("authentication failed")

    monkeypatch.setattr(client_module, "CollaborationClient", fake_client_factory)

    status, _, raw = dispatch(
        "POST", "/api/login", {"username": "alice", "password": "wrong"}
    )
    assert status == 401
    assert json.loads(raw)["ok"] is False

    status, _, raw = dispatch(
        "POST",
        "/api/login",
        {"username": "alice", "password": "alice123", "server": "127.0.0.1"},
    )
    assert status == 200
    assert json.loads(raw) == {"ok": True, "username": "alice"}

    status, _, raw = dispatch("GET", "/")
    assert status == 200
    assert b"CollabNet" in raw

    status, headers, _ = dispatch("GET", "/login")
    assert status == 302
    assert headers.get("Location") == "/"

    status, _, raw = dispatch("GET", "/api/state")
    assert status == 200
    assert json.loads(raw)["username"] == "alice"

    active = UIHandler.client_app
    status, _, raw = dispatch("POST", "/api/logout", {})
    assert status == 200
    assert json.loads(raw) == {"ok": True}
    assert active.closed is True
    assert UIHandler.client_app is None


def test_server_user_list_includes_all_system_accounts_and_client_state():
    import threading
    from types import SimpleNamespace
    from protocol import decode_packet, json_obj
    from server import CollaborationServer

    sent_packets = []
    srv = CollaborationServer.__new__(CollaborationServer)
    srv.users = {"alice": {"udp": ("127.0.0.1", 10001)}}
    srv.users_lock = threading.Lock()
    srv.seq = 1
    srv.seq_lock = threading.Lock()
    srv.credentials = {"alice": "a", "bob": "b", "carol": "c"}
    srv.udp = SimpleNamespace(sendto=lambda raw, addr: sent_packets.append(decode_packet(raw)))

    srv.send_user_list(("127.0.0.1", 10001))

    assert len(sent_packets) == 1
    payload = json_obj(sent_packets[0].payload)
    assert payload["users"] == ["alice"]
    assert payload["all_users"] == ["alice", "bob", "carol"]

    cli = client_module.CollaborationClient.__new__(client_module.CollaborationClient)
    cli.username = "alice"
    cli.state_lock = threading.RLock()
    cli.stop_event = threading.Event()
    cli.users = payload["users"]
    cli.all_users = payload["all_users"]
    cli.messages = []
    cli.transfers = []

    state = cli.state()
    assert state["users"] == ["alice"]
    assert state["all_users"] == ["alice", "bob", "carol"]
