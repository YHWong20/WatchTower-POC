from wt.common.contracts import Investigation
from wt.common.store import Store

from .conftest import fixture_json


def test_investigation_round_trip_and_ids(store):
    assert store.next_investigation_id() == "INV-0001"
    inv = Investigation.model_validate(fixture_json("contracts/investigation.json"))
    store.save_investigation(inv)
    assert store.get_investigation("INV-0001") == inv
    assert store.next_investigation_id() == "INV-0002"
    assert [e.id for e in store.evidence_for("INV-0001")] == [f"ev-{n}" for n in range(1, 8)]
    assert store.next_evidence_id("INV-0001") == "ev-8"
    assert store.next_evidence_id("INV-0002") == "ev-1"

    store.save_investigation(inv.model_copy(update={"llm_used": False}))       # upsert
    assert len(store.list_investigations()) == 1
    assert store.get_investigation("INV-0001").llm_used is False


def test_watch_items(store):
    store.save_watch_item({"id": "WI-0001", "confidence": 0.55, "status": "open", "entities": ["worker-1"]})
    store.save_watch_item({"id": "WI-0001", "confidence": 0.72, "status": "investigating"})
    assert store.get_watch_item("WI-0001")["confidence"] == 0.72
    assert [w["id"] for w in store.list_watch_items(status="investigating")] == ["WI-0001"]
    assert store.list_watch_items(status="open") == []


def test_persists_across_reopen(tmp_path):
    path = tmp_path / "wt.db"
    s = Store(path)
    s.cache_put("k", "job", "gemma3:1b", '{"a": 1}')
    s.close()
    assert Store(path).cache_get("k") == '{"a": 1}'
