"""FastAPI integration: request handling and connection lifecycle."""

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture()
def client():
    return TestClient(app)


def new_connection(client) -> str:
    response = client.get("/")
    assert response.status_code == 200
    return response.headers["x-connection-id"]


class TestLifecycle:
    def test_root_keeps_original_behaviour(self, client):
        assert client.get("/").json() == {"Hello": "World"}

    def test_connection_id_assigned_and_stable(self, client):
        cid = new_connection(client)
        again = client.get("/", headers={"x-connection-id": cid})
        assert again.headers["x-connection-id"] == cid
        # state is created deterministically at first compression touch
        assert cid not in client.get("/connections").json()["connections"]
        client.get(f"/connections/{cid}/state")
        assert cid in client.get("/connections").json()["connections"]

    def test_close_and_reopen(self, client):
        cid = new_connection(client)
        client.post(f"/connections/{cid}/encode", json={
            "stream_id": 1, "headers": [["x-t", "v"]],
        })
        assert client.delete(f"/connections/{cid}").json()["status"] == "closed"
        assert cid not in client.get("/connections").json()["connections"]
        assert client.delete(f"/connections/{cid}").status_code == 404

    def test_reset(self, client):
        cid = new_connection(client)
        client.post(f"/connections/{cid}/encode", json={
            "stream_id": 1, "headers": [["x-t", "v"]],
        })
        before = client.get(f"/connections/{cid}/state").json()
        assert before["encoder"]["insert_count"] == 1
        client.post(f"/connections/{cid}/reset")
        after = client.get(f"/connections/{cid}/state").json()
        assert after["encoder"]["insert_count"] == 0


class TestHeaderFlow:
    def test_encode_decode_loopback_between_connections(self, client):
        sender = new_connection(client)
        receiver = new_connection(client)

        def pump_batches():
            # deliver the sender's initial capacity instruction
            state = client.get(f"/connections/{sender}/state").json()
            assert state["sequencing"]["next_out_seq"] >= 0

        pump_batches()
        encoded = client.post(f"/connections/{sender}/encode", json={
            "stream_id": 1,
            "headers": [[":method", "GET"], ["x-api", "yes"]],
        }).json()
        for batch in encoded["encoder_stream"]:
            response = client.post(
                f"/connections/{receiver}/encoder-stream", json=batch
            )
            assert response.status_code == 200
        decoded = client.post(
            f"/connections/{receiver}/streams/1/headers",
            json={"block": encoded["block"]},
        ).json()
        assert decoded["status"] == "decoded"
        assert decoded["headers"] == [[":method", "GET"], ["x-api", "yes"]]

    def test_decode_before_dependency_reports_blocked(self, client):
        sender = new_connection(client)
        receiver = new_connection(client)
        encoded = client.post(f"/connections/{sender}/encode", json={
            "stream_id": 1, "headers": [["x-late", "dep"]],
        }).json()
        # decode the block BEFORE feeding the encoder stream
        response = client.post(
            f"/connections/{receiver}/streams/1/headers",
            json={"block": encoded["block"]},
        )
        assert response.json()["status"] == "blocked"
        # now feed the dependency: the stream is resumed automatically
        unblocked = []
        for batch in encoded["encoder_stream"]:
            result = client.post(
                f"/connections/{receiver}/encoder-stream", json=batch
            ).json()
            unblocked.extend(result["unblocked"])
        assert unblocked == [{"stream_id": 1, "headers": [["x-late", "dep"]]}]

    def test_malformed_block_returns_structured_error(self, client):
        cid = new_connection(client)
        response = client.post(
            f"/connections/{cid}/streams/1/headers", json={"block": "0000080561"}
        )
        assert response.status_code == 422
        body = response.json()
        assert body["error"] == "TruncatedDataError"
        assert body["connection_id"] == cid
        # error was recorded on the connection for observability
        state = client.get(f"/connections/{cid}/state").json()
        assert any("TruncatedDataError" in e for e in state["errors"])

    def test_unknown_connection_state_is_created_lazily(self, client):
        state = client.get("/connections/brand-new/state")
        assert state.status_code == 200
        assert state.json()["connection_id"] == "brand-new"
