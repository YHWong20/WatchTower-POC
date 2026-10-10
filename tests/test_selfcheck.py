import dataclasses

import pytest

from wt.common.config import PROFILES, load_settings
from wt.common.selfcheck import run


def test_default_profile_is_lite(monkeypatch):
    monkeypatch.delenv("WT_PROFILE", raising=False)
    monkeypatch.delenv("WT_MODEL", raising=False)
    cfg = load_settings()
    assert cfg.profile.name == "lite" and cfg.llm_model == "gemma3:1b"


def test_bad_profile_rejected(monkeypatch):
    monkeypatch.setenv("WT_PROFILE", "huge")
    with pytest.raises(ValueError):
        load_settings()


def test_llm_off_switch(monkeypatch):
    monkeypatch.setenv("WT_LLM", "off")
    assert load_settings().llm_enabled is False


@pytest.mark.parametrize("profile", sorted(PROFILES))
def test_selfcheck_passes(profile, capsys):
    cfg = dataclasses.replace(load_settings(), profile=PROFILES[profile])
    assert run(cfg) == 0
    out = capsys.readouterr().out
    assert "selfcheck OK" in out and f"profile: {profile}" in out
