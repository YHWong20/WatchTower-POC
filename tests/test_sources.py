import base64
import json
from urllib.parse import parse_qs

import httpx
import pytest

from wt.common.sources.base import render_template
from wt.common.sources.solarwinds import QUERY_PATH, SolarWindsClient
from wt.common.sources.splunk import EXPORT_PATH, SplunkClient

from .conftest import fixture_json, fixture_text


def test_render_template_fills_and_guards():
    assert render_template("host={node} earliest=-{window}", node="ocp-sim-worker", window="30m") == \
        "host=ocp-sim-worker earliest=-30m"
    with pytest.raises(KeyError):
        render_template("host={node}", window="30m")
    with pytest.raises(ValueError):
        render_template("WHERE I.Name = '{port}'", port="x' OR 1=1 --")
    with pytest.raises(ValueError):
        render_template("host={node}", node="a | delete")


async def test_splunk_search_signal(sources, audit, store):
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["path"] = req.url.path
        seen["auth"] = req.headers["authorization"]
        seen["form"] = {k: v[0] for k, v in parse_qs(req.content.decode()).items()}
        return httpx.Response(200, text=fixture_text("splunk/export_api_svc_timeouts.jsonl"))

    sp = SplunkClient("http://splunk:8089", "tok", sources, audit, actor="openshift-agent",
                      transport=httpx.MockTransport(handler))
    res = await sp.search_signal("api_svc_timeouts", node="ocp-sim-worker", window="30m")
    await sp.close()

    assert seen["path"] == EXPORT_PATH and seen["auth"] == "Bearer tok"
    assert seen["form"]["output_mode"] == "json"
    assert seen["form"]["search"].startswith('search index=ocp sourcetype=kube:container:svc-prober host=ocp-sim-worker')
    assert res.query.endswith("earliest=-30m | timechart span=1m count")
    assert [r["count"] for r in res.rows] == ["3", "4", "7"]          # preview row dropped
    assert res.tool == "splunk" and res.audit_id.startswith("aud-")
    row = store.audit_rows("query")[0]
    assert (row["actor"], row["target"], row["ok"], row["detail"]) == ("openshift-agent", "splunk", 1, "3 rows")
    assert row["input"] == res.query and len(row["input_hash"]) == 16


async def test_splunk_fired_alerts(sources, audit):
    def handler(req):
        assert req.method == "GET" and req.url.path == "/services/alerts/fired_alerts"
        return httpx.Response(200, json=fixture_json("splunk/fired_alerts.json"))

    sp = SplunkClient("http://splunk:8089", "tok", sources, audit, transport=httpx.MockTransport(handler))
    res = await sp.fired_alerts()
    await sp.close()
    assert res.rows[0]["name"] == "NodeNotReady" and res.rows[0]["entity"] == "ocp-sim-worker"


async def test_splunk_error_is_audited(sources, audit, store):
    sp = SplunkClient("http://splunk:8089", "tok", sources, audit,
                      transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    with pytest.raises(httpx.HTTPStatusError):
        await sp.search("index=ocp")
    await sp.close()
    assert store.audit_rows()[0]["ok"] == 0


async def test_solarwinds_query_signal(sources, audit):
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["path"] = req.url.path
        seen["auth"] = base64.b64decode(req.headers["authorization"].split()[1]).decode()
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json=fixture_json("solarwinds/interface_errors_gi1-0-13.json"))

    sw = SolarWindsClient("http://solarwinds:17778", "wt", "pw", sources, audit, actor="network-agent",
                          transport=httpx.MockTransport(handler))
    res = await sw.query_signal("interface_discards", if_id=13, minutes=30)
    await sw.close()

    assert seen["path"] == QUERY_PATH and seen["auth"] == "wt:pw"
    assert "WHERE InterfaceID = 13 AND DateTime > ADDDATE('minute', -30" in seen["body"]["query"]
    assert [r["OutDiscards"] for r in res.rows] == [120, 410, 760, 1190, 1505, 1840]
    assert '"OutDiscards":1840' in res.excerpt()


async def test_solarwinds_active_alerts(sources, audit):
    sw = SolarWindsClient("http://solarwinds:17778", "wt", "pw", sources, audit,
                          transport=httpx.MockTransport(
                              lambda r: httpx.Response(200, json=fixture_json("solarwinds/alert_active_empty.json"))))
    res = await sw.active_alerts()
    await sw.close()
    assert res.rows == [] and "Orion.AlertActive" in res.query
