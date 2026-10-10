import json

import jsonschema
import pytest
from pydantic import ValidationError

from wt.common.contracts import EvidenceItem, Investigation, Reuse, Signal, dump
from wt.common.selfcheck import SCHEMA_MODELS, schema_drift

from .conftest import CONTRACTS, fixture_json


@pytest.mark.parametrize("filename", sorted(SCHEMA_MODELS))
def test_models_match_json_schemas(filename):
    schema = json.loads((CONTRACTS / filename).read_text())
    assert schema_drift(schema, SCHEMA_MODELS[filename]) == []


@pytest.mark.parametrize(
    "fixture, model, schema_file",
    [("contracts/signal.json", Signal, "signal.schema.json"),
     ("contracts/investigation.json", Investigation, "investigation.schema.json")],
)
def test_fixture_round_trips_through_model_and_schema(fixture, model, schema_file):
    raw = fixture_json(fixture)
    schema = json.loads((CONTRACTS / schema_file).read_text())
    jsonschema.validate(raw, schema)
    obj = model.model_validate(raw)
    jsonschema.validate(dump(obj), schema)          # what we publish is schema-valid too


def test_signal_defaults_are_schema_valid():
    sig = Signal(
        domain="network", agent="network-agent", rule="net.output_drops_rising",
        entity={"kind": "interface", "name": "cisco-sim-1:Gi1/0/13"}, severity="weak",
        summary="OutDiscards rising for 3 polls", value=335, baseline=0,
        source={"tool": "solarwinds", "query": "SELECT ..."},
    )
    jsonschema.validate(dump(sig), json.loads((CONTRACTS / "signal.schema.json").read_text()))


def test_contracts_reject_bad_values():
    raw = fixture_json("contracts/investigation.json")
    with pytest.raises(ValidationError):
        Investigation.model_validate({**raw, "id": "INV-1"})
    bad = json.loads(json.dumps(raw))
    bad["hypotheses"][0]["llm_adjustment"]["delta"] = 0.2           # cap is ±0.10
    with pytest.raises(ValidationError):
        Investigation.model_validate(bad)
    bad = json.loads(json.dumps(raw))
    bad["recommended_actions"][0]["kind"] = "remediation"            # read-only
    with pytest.raises(ValidationError):
        Investigation.model_validate(bad)


def test_reuse_rate_from_evidence():
    inv = Investigation.model_validate(fixture_json("contracts/investigation.json"))
    reuse = Reuse.from_evidence(inv.evidence)
    assert (reuse.reused, reuse.probed, reuse.rate) == (6, 1, 0.857)
    assert reuse.probe_reasons and "SolarWinds" in reuse.probe_reasons[0]
    assert isinstance(inv.evidence[0], EvidenceItem) and not inv.evidence[0].is_probe
