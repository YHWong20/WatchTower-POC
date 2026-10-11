"""Clear WatchTower's demo state (make demo-reset): watch items, investigations, evidence, signals
and progress records. Keeps the learned baselines, sparkline samples, the audit log and the LLM
cache. Run with the agents stopped (the Parent Agent keeps open watch items in memory)."""

from __future__ import annotations

from wt.common.config import settings
from wt.common.store import Store


def main() -> None:
    store = Store(settings().db_path)
    counts = {}
    for table in ("watch_items", "investigations", "evidence", "signals"):
        counts[table] = store._exec(f"DELETE FROM {table}").rowcount
    counts["kv"] = store._exec("DELETE FROM kv WHERE key NOT IN ('inv_counter', 'watch_counter')").rowcount
    store.close()
    print("cleared " + ", ".join(f"{n} {t}" for t, n in counts.items()))


if __name__ == "__main__":
    main()
