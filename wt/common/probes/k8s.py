"""Read-only Kubernetes API probe: the fallback when Splunk has a gap for the window.

OpenShift questions are answered from Splunk first (contracts/sources.yaml). This adapter has
only named read methods, and every one of them goes through `_get`, the single place that
talks to the API server, which can only issue GET. RBAC (get/list/watch) enforces the same
thing on the cluster side.

Credentials come from a kubeconfig (`kind get kubeconfig --internal --name ocp-sim`) or the
in-cluster service account, loaded with the `kubernetes` client; requests go out with httpx.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from wt.common.audit import Audit
from wt.common.probes.base import ProbeRefused, ProbeResult

_NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")   # RFC 1123 subdomain
_SELECTOR = re.compile(r"^[A-Za-z0-9_.\-/=!,]+$")


def _name(value: str) -> str:
    if not _NAME.match(value):
        raise ProbeRefused(f"refused: {value!r} is not a valid Kubernetes object name")
    return value


class K8sProbe:
    def __init__(self, http: httpx.AsyncClient, audit: Audit, actor: str = "openshift-agent", target: str = "ocp-sim"):
        self.http = http
        self.audit = audit
        self.actor = actor
        self.target = target

    @classmethod
    def from_kubeconfig(cls, audit: Audit, kubeconfig: str | None = None, **kwargs: Any) -> K8sProbe:
        """kubeconfig path, or the in-cluster service account when None."""
        from kubernetes import client, config  # heavy import, only when a probe is actually built

        cfg = client.Configuration()
        if kubeconfig:
            config.load_kube_config(config_file=kubeconfig, client_configuration=cfg)
        else:
            config.load_incluster_config(client_configuration=cfg)
        headers = {}
        token = cfg.api_key.get("authorization") or cfg.api_key.get("BearerToken")
        if token:
            prefix = cfg.api_key_prefix.get("authorization", "")
            headers["Authorization"] = f"{prefix} {token}".strip() if prefix else token
        http = httpx.AsyncClient(
            base_url=cfg.host,
            headers=headers,
            verify=cfg.ssl_ca_cert if cfg.verify_ssl and cfg.ssl_ca_cert else cfg.verify_ssl,
            cert=(cfg.cert_file, cfg.key_file) if cfg.cert_file else None,
            timeout=15.0,
        )
        return cls(http, audit, **kwargs)

    async def close(self) -> None:
        await self.http.aclose()

    async def _get(self, path: str, params: dict[str, str] | None = None) -> ProbeResult:
        query = f"GET {path}" + ("?" + "&".join(f"{k}={v}" for k, v in params.items()) if params else "")
        async with self.audit.timed("probe", self.actor, self.target, query) as rec:
            resp = await self.http.get(path, params=params)
            resp.raise_for_status()
            data = resp.json()
            rec.detail = f"{resp.status_code} {data.get('kind', '')}"
        return ProbeResult(
            tool="probe_k8s_api", target=self.target, query=query, data=data,
            duration_ms=rec.duration_ms, audit_id=rec.audit_id,
        )

    # ------------------------------------------------------------ read methods (the whole API surface)

    async def get_node(self, node: str) -> ProbeResult:
        """Node status, conditions and status.nodeInfo.bootID."""
        return await self._get(f"/api/v1/nodes/{_name(node)}")

    async def list_nodes(self) -> ProbeResult:
        return await self._get("/api/v1/nodes")

    async def get_lease(self, node: str) -> ProbeResult:
        """The kubelet's node Lease (spec.renewTime)."""
        return await self._get(f"/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/{_name(node)}")

    async def list_pods(self, node: str | None = None, namespace: str | None = None) -> ProbeResult:
        """Pods (restartCount) on a node, optionally in one namespace."""
        path = f"/api/v1/namespaces/{_name(namespace)}/pods" if namespace else "/api/v1/pods"
        return await self._get(path, {"fieldSelector": f"spec.nodeName={_name(node)}"} if node else None)

    async def list_events(self, namespace: str | None = None, field_selector: str | None = None) -> ProbeResult:
        if field_selector and not _SELECTOR.match(field_selector):
            raise ProbeRefused(f"refused: unsafe field selector {field_selector!r}")
        path = f"/api/v1/namespaces/{_name(namespace)}/events" if namespace else "/api/v1/events"
        return await self._get(path, {"fieldSelector": field_selector} if field_selector else None)

    async def get_node_metrics(self, node: str) -> ProbeResult:
        """metrics-server usage; last resort when the node is not in SolarWinds."""
        return await self._get(f"/apis/metrics.k8s.io/v1beta1/nodes/{_name(node)}")
