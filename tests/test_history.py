import json
import threading
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

from mfthistory.auth import OpenRegistry, TokenRegistry
from mfthistory.collectors import DirectoryCollector
from mfthistory.parser import ParseError, parse_transfer_log
from mfthistory.server import serve
from mfthistory.simulate import SimConfig, generate, write_folder
from mfthistory.store import HistoryStore, Scope

STARTED = """<?xml version="1.0" encoding="UTF-8"?>
<transaction version="6.00" ID="414d5120514d31202020202020202020a1b2c3d4e5f60708" agentRole="sourceAgent"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="TransferLog.xsd">
  <action time="2026-11-16T09:00:00.000Z">started</action>
  <sourceAgent agent="HUB_AGENT_01" QMgr="QM1"/>
  <destinationAgent agent="PARTNER_ACME" QMgr="QM2"/>
  <originator><hostName>host1</hostName><userID>mftadm</userID></originator>
  <job><name>ORDERS_ACME</name></job>
  <transferSet startTime="2026-11-16T09:00:00.000Z" total="2">
    <item mode="binary"><source disposition="leave"><file size="100">/mft/out/orders_1.xml</file></source>
      <destination exist="overwrite"><file>/data/orders_1.xml</file></destination></item>
    <item mode="binary"><source disposition="leave"><file size="200">/mft/out/orders_2.xml</file></source>
      <destination exist="overwrite"><file>/data/orders_2.xml</file></destination></item>
  </transferSet>
</transaction>"""

COMPLETED = STARTED.replace(
    '<action time="2026-11-16T09:00:00.000Z">started</action>',
    '<action time="2026-11-16T09:00:05.000Z">completed</action>'
    '<status resultCode="40"><supplement>BFGRP0034I: completed with some failures</supplement></status>',
).replace('total="2"', 'total="2" bytesSent="100"').replace(
    "</destination></item>\n    <item", '</destination><status resultCode="0"/></item>\n    <item'
).replace(
    "<file>/data/orders_2.xml</file></destination></item>",
    '<file>/data/orders_2.xml</file></destination><status resultCode="1"><supplement>BFGIO0001E: File not found</supplement></status></item>',
)
TID = "414d5120514d31202020202020202020a1b2c3d4e5f60708"


@pytest.fixture
def store():
    s = HistoryStore(":memory:")
    yield s
    s.close()


def test_parse_started():
    ev = parse_transfer_log(STARTED)
    assert ev.transfer_id == TID
    assert ev.action == "started"
    assert ev.source_agent == "HUB_AGENT_01" and ev.destination_qmgr == "QM2"
    assert ev.job_name == "ORDERS_ACME"
    assert [i.source_size for i in ev.items] == [100, 200]


def test_parse_completed_item_results():
    ev = parse_transfer_log(COMPLETED)
    assert ev.action == "completed" and ev.result_code == 40 and ev.bytes_sent == 100
    assert [i.result_code for i in ev.items] == [0, 1]
    assert "not found" in ev.items[1].supplement


def test_parse_namespaced_root():
    xml = STARTED.replace("<transaction ", '<transaction xmlns="urn:example" ')
    assert parse_transfer_log(xml).transfer_id == TID


@pytest.mark.parametrize("bad", ["not xml", "<foo/>", "<transaction><action>started</action></transaction>"])
def test_parse_rejects(bad):
    with pytest.raises(ParseError):
        parse_transfer_log(bad)


def test_fold_and_status(store):
    store.ingest(parse_transfer_log(STARTED))
    row = store.get(TID)
    assert row["status"] == "in_progress" and row["file_count"] == 2 and row["total_bytes"] == 300
    store.ingest(parse_transfer_log(COMPLETED))
    row = store.get(TID)
    assert row["status"] == "partial"
    assert row["started_at"] == "2026-11-16T09:00:00.000Z"
    assert row["completed_at"] == "2026-11-16T09:00:05.000Z"
    assert row["total_bytes"] == 100
    assert [i["result_code"] for i in row["items"]] == [0, 1]
    assert [e["action"] for e in row["events"]] == ["started", "completed"]


def test_duplicates_ignored(store):
    assert store.ingest(parse_transfer_log(COMPLETED)) is True
    assert store.ingest(parse_transfer_log(COMPLETED)) is False
    assert len(store.get(TID)["events"]) == 1


def test_out_of_order(store):
    store.ingest(parse_transfer_log(COMPLETED))
    store.ingest(parse_transfer_log(STARTED))
    row = store.get(TID)
    assert row["status"] == "partial"
    assert row["last_action"] == "completed"
    assert row["started_at"] == "2026-11-16T09:00:00.000Z"
    assert [i["result_code"] for i in row["items"]] == [0, 1]  # started must not wipe final results


def test_search_filters_and_scope(store):
    store.ingest(parse_transfer_log(COMPLETED))
    assert store.search(file="orders_2")["total"] == 1
    assert store.search(file="orders_*.xml")["total"] == 1
    assert store.search(file="missing")["total"] == 0
    assert store.search(status="partial")["total"] == 1
    assert store.search(agent="PARTNER_ACME")["total"] == 1
    assert store.search(since="2026-11-16T10:00:00Z")["total"] == 0
    other = Scope("other", frozenset({"PARTNER_OTHER"}))
    assert store.search(scope=other)["total"] == 0
    assert store.get(TID, scope=other) is None
    acme = Scope("acme", frozenset({"PARTNER_ACME"}))
    assert store.search(scope=acme)["total"] == 1
    assert store.search(scope=Scope("none", frozenset()))["total"] == 0


def test_stuck(store):
    store.ingest(parse_transfer_log(STARTED))
    ref = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
    assert len(store.stuck(60, ref=ref)) == 1
    assert len(store.stuck(60, ref=ref - timedelta(hours=2, minutes=30))) == 0
    store.ingest(parse_transfer_log(COMPLETED))
    assert store.stuck(60, ref=ref) == []


def test_purge(store):
    store.ingest(parse_transfer_log(COMPLETED))
    assert store.purge(older_than_days=100000) == 0
    assert store.purge(older_than_days=0) == 1
    assert store.get(TID) is None


def test_simulation_end_to_end(tmp_path):
    end = datetime(2026, 11, 16, tzinfo=timezone.utc)
    cfg = SimConfig(agents=30, transfers=1500, days=2, seed=3, end=end)
    n = write_folder(str(tmp_path / "in"), cfg)
    assert n > 3000
    store = HistoryStore(str(tmp_path / "h.db"))
    counts = DirectoryCollector(store, str(tmp_path / "in"), done_folder=str(tmp_path / "done")).run_once()
    assert counts["errors"] == 0
    assert counts["duplicates"] > 0  # the simulator re-sends some messages on purpose
    stats = store.stats()
    assert sum(stats["by_status"].values()) == 1500
    assert stats["by_status"]["success"] > 1300
    assert stats["by_status"].get("failed", 0) > 0
    assert len(store.stuck(60, ref=end + timedelta(days=1))) == stats["by_status"].get("in_progress", 0)
    assert len(store.agents()) == 30
    # Same input twice changes nothing.
    write_folder(str(tmp_path / "again"), cfg)
    again = DirectoryCollector(store, str(tmp_path / "again")).run_once()
    assert again["stored"] == 0
    assert sum(store.stats()["by_status"].values()) == 1500


def test_simulation_is_deterministic():
    cfg = SimConfig(transfers=50, end=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert list(generate(cfg)) == list(generate(cfg))


def _get(url, token=None):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def test_http_api_with_tokens(tmp_path, store):
    store.ingest(parse_transfer_log(COMPLETED))
    reg = TokenRegistry(str(tmp_path / "tokens.json"))
    admin = reg.add("admins", "*")
    acme = reg.add("Acme", ["PARTNER_ACME"])
    other = reg.add("Other", ["PARTNER_OTHER"])
    reg.save()
    reg = TokenRegistry(str(tmp_path / "tokens.json"))  # reload from disk: only hashes stored
    assert admin not in (tmp_path / "tokens.json").read_text()

    httpd = serve(store, reg, port=0)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        assert _get(base + "/")[0] == 200
        assert _get(base + "/api/transfers")[0] == 401
        assert _get(base + "/api/transfers", "wrong")[0] == 401
        code, body = _get(base + "/api/transfers?file=orders", acme)
        assert code == 200 and json.loads(body)["total"] == 1
        assert json.loads(_get(base + "/api/transfers", other)[1])["total"] == 0
        assert _get(base + f"/api/transfers/{TID}", other)[0] == 404
        code, body = _get(base + f"/api/transfers/{TID}", admin)
        assert code == 200 and len(json.loads(body)["items"]) == 2
        code, body = _get(base + "/api/transfers.csv", admin)
        assert code == 200 and body.decode().startswith("transfer_id,")
        assert json.loads(_get(base + "/api/me", acme)[1])["agents"] == ["PARTNER_ACME"]
        assert "partial" in json.loads(_get(base + "/api/stats", admin)[1])["by_status"]
        assert _get(base + "/api/transfers?limit=abc", admin)[0] == 400
    finally:
        httpd.shutdown()


def test_open_registry_is_admin(store):
    assert OpenRegistry().scope_for(None).agents is None
