import json
from pathlib import Path

import pytest

from wt.common.audit import Audit
from wt.common.catalog import load_sources
from wt.common.config import REPO_ROOT
from wt.common.store import Store

FIXTURES = REPO_ROOT / "eval" / "fixtures"
CONTRACTS = REPO_ROOT / "contracts"
PLAYBOOKS = REPO_ROOT / "playbooks"


def fixture_text(rel: str) -> str:
    return (FIXTURES / rel).read_text()


def fixture_json(rel: str):
    return json.loads(fixture_text(rel))


@pytest.fixture
def store(tmp_path: Path) -> Store:
    s = Store(tmp_path / "wt.db")
    yield s
    s.close()


@pytest.fixture
def audit(store: Store) -> Audit:
    return Audit(store)


@pytest.fixture(scope="session")
def sources():
    return load_sources(CONTRACTS)
