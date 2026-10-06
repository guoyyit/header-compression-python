import pytest
from fastapi.testclient import TestClient

from hpack_conn.service import create_app


@pytest.fixture()
def client():
    with TestClient(create_app()) as client:
        yield client


def open_pair(client, a="alice", b="bob"):
    assert client.post(f"/connections/{a}").status_code == 201
    assert client.post(f"/connections/{b}").status_code == 201


def encode(client, cid, stream_id, headers):
    resp = client.post(f"/connections/{cid}/encode",
                       json={"stream_id": stream_id, "headers": headers})
    assert resp.status_code == 200, resp.text
    return resp.json()["frame"]


class TestLifecycleAPI:
    def test_root_still_works(self, client):
        assert client.get("/").json() == {"Hello": "World"}

    def test_open_state_close(self, client):
        assert client.post("/connections/c1").status_code == 201
        state = client.get("/connections/c1").json()
        assert state["phase"] == "open"
        assert state["encoder_table"]["capacity"] == 4096
        assert client.delete("/connections/c1").json()["phase"] == "closed"
        assert client.get("/connections/c1").status_code == 404

    def test_duplicate_open_conflict(self, client):
        client.post("/connections/c1")
        assert client.post("/connections/c1").status_code == 409

    def test_unknown_connection_404(self, client):
        assert client.get("/connections/nope").status_code == 404
        assert client.post("/connections/nope/encode",
                           json={"stream_id": 1, "headers": []}).status_code == 404

    def test_reset_clears_state(self, client):
        client.post("/connections/c1")
        encode(client, "c1", 1, [["x-a", "1"]])
        assert client.get("/connections/c1").json()["encoder_table"]["insertion_count"] == 1
        client.post("/connections/c1/reset")
        assert client.get("/connections/c1").json()["encoder_table"]["insertion_count"] == 0


class TestEncodeDecodeAPI:
    def test_loopback_between_two_connections(self, client):
        open_pair(client)
        frame = encode(client, "alice", 1, [[":method", "GET"], ["x-a", "1"]])
        resp = client.post("/connections/bob/decode",
                           json={"stream_id": 1, "frame": frame})
        assert resp.json() == {"status": "decoded",
                               "headers": [[":method", "GET"], ["x-a", "1"]]}

    def test_tables_stay_in_sync_via_api(self, client):
        open_pair(client)
        for sid in range(1, 6):
            frame = encode(client, "alice", sid, [[f"x-k{sid}", "v"]])
            client.post("/connections/bob/decode",
                        json={"stream_id": sid, "frame": frame})
        alice = client.get("/connections/alice").json()["encoder_table"]
        bob = client.get("/connections/bob").json()["decoder_table"]
        assert alice["insertion_count"] == bob["insertion_count"]
        assert alice["size"] == bob["size"]

    def test_blocked_stream_via_api(self, client):
        open_pair(client)
        f1 = encode(client, "alice", 1, [["x-dep", "1"]])
        f2 = encode(client, "alice", 2, [["x-dep", "1"]])
        resp = client.post("/connections/bob/decode",
                           json={"stream_id": 2, "frame": f2})
        assert resp.json()["status"] == "blocked"
        client.post("/connections/bob/decode",
                    json={"stream_id": 1, "frame": f1})
        state = client.get("/connections/bob/streams/2").json()
        assert state["phase"] == "done"
        assert state["headers"] == [["x-dep", "1"]]

    def test_capacity_update_via_api(self, client):
        open_pair(client)
        resp = client.post("/connections/alice/capacity", json={"capacity": 64})
        assert resp.status_code == 200
        frame = encode(client, "alice", 1, [["x-a", "1"]])
        client.post("/connections/bob/decode",
                    json={"stream_id": 1, "frame": frame})
        assert client.get("/connections/bob").json()["decoder_table"]["capacity"] == 64

    def test_capacity_violation_is_distinguishable(self, client):
        client.post("/connections/c1")
        resp = client.post("/connections/c1/capacity", json={"capacity": 10**9})
        assert resp.status_code == 409
        assert resp.json()["error"] == "capacity_sync_violation"

    def test_malformed_frame_is_observable(self, client):
        client.post("/connections/c1")
        resp = client.post("/connections/c1/decode",
                           json={"stream_id": 1, "frame": "ffff"})
        assert resp.status_code == 422
        assert resp.json()["error"] in {"truncated_input", "malformed_input",
                                        "invalid_index"}

    def test_bad_hex_rejected(self, client):
        client.post("/connections/c1")
        resp = client.post("/connections/c1/decode",
                           json={"stream_id": 1, "frame": "zz"})
        assert resp.status_code == 422
