"""Unit tests for the security layer: API keys, authz, delegation."""

from __future__ import annotations

import pytest

from ameoba.domain.security import AgentIdentity, AuthzRequest, DataSensitivityLabel
from ameoba.security.authn.api_key import APIKeyStore
from ameoba.security.authz.cedar_engine import SimplePolicyEngine
from ameoba.security.authz.delegation import MAX_DELEGATION_DEPTH, create_delegation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _identity(
    agent_id: str = "agent-1",
    tenant_id: str = "acme",
    scopes: list[str] | None = None,
    delegation_depth: int = 0,
) -> AgentIdentity:
    return AgentIdentity(
        agent_id=agent_id,
        tenant_id=tenant_id,
        scopes=scopes or ["read", "write"],
        delegation_depth=delegation_depth,
    )


def _request(
    principal: AgentIdentity,
    action: str = "read",
    resource_type: str = "collection",
    resource_id: str = "users",
    labels: list[DataSensitivityLabel] | None = None,
) -> AuthzRequest:
    return AuthzRequest(
        principal=principal,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        resource_labels=labels or [],
    )


# ---------------------------------------------------------------------------
# APIKeyStore
# ---------------------------------------------------------------------------


def test_api_key_store_add_and_validate():
    store = APIKeyStore()
    store.add_key("secret-key", agent_id="agent-1", tenant_id="acme")
    meta = store.validate("secret-key")
    assert meta is not None
    assert meta["agent_id"] == "agent-1"
    assert meta["tenant_id"] == "acme"


def test_api_key_store_wrong_key_returns_none():
    store = APIKeyStore()
    store.add_key("correct-key", agent_id="agent-1", tenant_id="acme")
    assert store.validate("wrong-key") is None


def test_api_key_store_empty_store_returns_none():
    store = APIKeyStore()
    assert store.validate("anything") is None


def test_api_key_store_generate_key_format():
    key = APIKeyStore.generate_key("amk")
    assert key.startswith("amk_")
    assert len(key) > 10


def test_api_key_store_load_from_list():
    store = APIKeyStore()
    store.load_from_list(["key1", "key2"])
    assert store.validate("key1") is not None
    assert store.validate("key2") is not None
    assert store.validate("key3") is None


# ---------------------------------------------------------------------------
# SimplePolicyEngine
# ---------------------------------------------------------------------------


def test_policy_engine_allows_read_with_read_scope():
    engine = SimplePolicyEngine()
    decision = engine.authorize(_request(_identity(scopes=["read"]), action="read"))
    assert decision.allowed


def test_policy_engine_allows_write_with_write_scope():
    engine = SimplePolicyEngine()
    decision = engine.authorize(_request(_identity(scopes=["write"]), action="write"))
    assert decision.allowed


def test_policy_engine_denies_write_without_write_scope():
    engine = SimplePolicyEngine()
    decision = engine.authorize(_request(_identity(scopes=["read"]), action="write"))
    assert not decision.allowed
    assert "scope" in decision.reason.lower()


def test_policy_engine_admin_bypasses_scope_check():
    engine = SimplePolicyEngine()
    decision = engine.authorize(_request(_identity(scopes=["admin"]), action="delete"))
    assert decision.allowed


def test_policy_engine_denies_phi_without_grant():
    engine = SimplePolicyEngine()
    decision = engine.authorize(
        _request(
            _identity(scopes=["read"]),
            action="read",
            labels=[DataSensitivityLabel.PHI],
        )
    )
    assert not decision.allowed
    assert "PHI" in decision.reason or "phi" in decision.reason.lower()


def test_policy_engine_allows_phi_with_explicit_grant():
    engine = SimplePolicyEngine(
        restricted_label_grants={"PHI": {"agent-1"}}
    )
    decision = engine.authorize(
        _request(
            _identity(agent_id="agent-1", scopes=["read"]),
            action="read",
            labels=[DataSensitivityLabel.PHI],
        )
    )
    assert decision.allowed


def test_policy_engine_denies_delegation_depth_exceeded():
    engine = SimplePolicyEngine()
    identity = _identity(scopes=["read", "write"], delegation_depth=4)
    decision = engine.authorize(_request(identity))
    assert not decision.allowed
    assert "delegation" in decision.reason.lower()


def test_policy_engine_injects_tenant_filter():
    engine = SimplePolicyEngine()
    identity = _identity(tenant_id="acme")
    decision = engine.authorize(_request(identity, action="read"))
    assert decision.allowed
    assert decision.row_filter is not None
    assert "acme" in decision.row_filter


def test_policy_engine_no_filter_for_default_tenant():
    engine = SimplePolicyEngine()
    identity = _identity(tenant_id="default")
    decision = engine.authorize(_request(identity))
    assert decision.allowed
    assert decision.row_filter is None


def test_policy_engine_allow_all_mode():
    engine = SimplePolicyEngine(allow_all=True)
    # Even an empty scopes list passes
    identity = AgentIdentity(agent_id="anon", tenant_id="t1", scopes=[])
    decision = engine.authorize(_request(identity))
    assert decision.allowed


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------


def test_delegation_reduces_to_intersection_of_scopes():
    delegator = _identity(agent_id="a", scopes=["read", "write", "admin"])
    delegate = _identity(agent_id="b", scopes=["read", "write"])
    result = create_delegation(delegator, delegate, delegated_scopes=["read"])
    assert set(result.scopes) == {"read"}
    assert result.delegation_depth == 1
    assert result.delegated_by == "a"


def test_delegation_cannot_escalate_beyond_delegator():
    delegator = _identity(agent_id="a", scopes=["read"])
    delegate = _identity(agent_id="b", scopes=["read", "write"])
    result = create_delegation(delegator, delegate, delegated_scopes=["read", "write"])
    # write is not in delegator's scopes — must be excluded
    assert "write" not in result.scopes
    assert "read" in result.scopes


def test_delegation_inherits_delegator_tenant():
    delegator = _identity(agent_id="a", tenant_id="acme")
    delegate = _identity(agent_id="b", tenant_id="other")
    result = create_delegation(delegator, delegate)
    assert result.tenant_id == "acme"


def test_delegation_depth_limit_raises():
    at_max = _identity(agent_id="a", delegation_depth=MAX_DELEGATION_DEPTH)
    delegate = _identity(agent_id="b")
    with pytest.raises(ValueError, match=r"[Dd]elegation"):
        create_delegation(at_max, delegate)


def test_delegation_depth_increments():
    delegator = _identity(agent_id="a", delegation_depth=0)
    delegate = _identity(agent_id="b", delegation_depth=0)
    result = create_delegation(delegator, delegate)
    assert result.delegation_depth == 1
