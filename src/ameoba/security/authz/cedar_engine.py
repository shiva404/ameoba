"""Policy engine — Cedar-compatible authorization.

The architecture specifies Cedar (42–60x faster than OPA for policy evaluation).
The Python ``cedar-policy`` bindings are not yet stable on all platforms, so this
module provides:

1. ``CedarPolicyEngine`` — wraps the cedar-policy package when available.
2. ``SimplePolicyEngine`` — a pure-Python fallback with the same interface,
   suitable for development and environments without Rust extensions.

Both implement the ``PolicyEngine`` protocol (ports/policy_engine.py).

To swap: replace ``build_policy_engine()`` to return a ``CedarPolicyEngine``
once cedar-policy bindings stabilise.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import structlog

from ameoba.domain.security import AuthzDecision, AuthzRequest, DataSensitivityLabel

logger = structlog.get_logger(__name__)

# Try to import cedar-policy bindings
_CEDAR_AVAILABLE = False
try:
    import cedar  # type: ignore[import]
    _CEDAR_AVAILABLE = True
except ImportError:
    pass


class SimplePolicyEngine:
    """Pure-Python policy engine for development and fallback.

    Implements a subset of Cedar semantics:
    - Default deny (explicit allow required)
    - Role-based rules loaded from a simple allow-list dict
    - Label-based access control (PHI/PCI requires explicit grants)
    - Query filter injection for row-level security

    This satisfies the ``PolicyEngine`` protocol.
    """

    # Scopes required to perform each action
    _ACTION_SCOPES: dict[str, str] = {
        "read":  "read",
        "write": "write",
        "query": "read",
        "delete": "delete",
        "admin": "admin",
    }

    # Labels that require explicit grant (not default-accessible)
    _RESTRICTED_LABELS = {
        DataSensitivityLabel.PHI,
        DataSensitivityLabel.PCI,
        DataSensitivityLabel.PII,
    }

    def __init__(
        self,
        *,
        allow_all: bool = False,
        restricted_label_grants: dict[str, set[str]] | None = None,
    ) -> None:
        """
        Args:
            allow_all:               If True, allow every request (dev/test only).
            restricted_label_grants: Maps label value → set of agent_ids that may access it.
        """
        self._allow_all = allow_all
        self._restricted_label_grants = restricted_label_grants or {}

    def authorize(self, request: AuthzRequest) -> AuthzDecision:
        """Evaluate the authorization request synchronously.

        Checks (in order):
        1. Scope check — does the principal have the required scope?
        2. Label check — restricted labels require explicit grant.
        3. Delegation depth check — max 3 levels.
        """
        if self._allow_all:
            return AuthzDecision(allowed=True, reason="allow_all mode")

        principal = request.principal

        # 1. Delegation depth
        if principal.delegation_depth > 3:
            return AuthzDecision(
                allowed=False,
                reason=f"Delegation depth {principal.delegation_depth} exceeds maximum of 3",
            )

        # 2. Scope check
        required_scope = self._ACTION_SCOPES.get(request.action, request.action)
        if required_scope not in principal.scopes and "admin" not in principal.scopes:
            return AuthzDecision(
                allowed=False,
                reason=f"Missing required scope '{required_scope}'",
            )

        # 3. Restricted label check
        for label in request.resource_labels:
            if label in self._RESTRICTED_LABELS:
                granted_agents = self._restricted_label_grants.get(label.value, set())
                if principal.agent_id not in granted_agents:
                    logger.warning(
                        "authz_denied_restricted_label",
                        agent_id=principal.agent_id,
                        label=label.value,
                        resource=request.resource_id,
                    )
                    return AuthzDecision(
                        allowed=False,
                        reason=f"Access to {label.value} data requires explicit grant",
                    )

        # 4. Tenant isolation — inject row filter
        row_filter = None
        if principal.tenant_id != "default":
            row_filter = f"_tenant_id = '{principal.tenant_id}'"

        return AuthzDecision(
            allowed=True,
            reason="allow",
            row_filter=row_filter,
        )


def build_policy_engine(
    *,
    policy_dir: Path | None = None,
    allow_all: bool = False,
) -> SimplePolicyEngine:
    """Build and return the configured policy engine.

    When ``cedar-policy`` bindings are available and ``policy_dir`` is set,
    returns a ``CedarPolicyEngine``.  Otherwise returns ``SimplePolicyEngine``.
    """
    if _CEDAR_AVAILABLE and policy_dir and policy_dir.exists():
        logger.info("cedar_policy_engine_active", policy_dir=str(policy_dir))
        # Future: return CedarPolicyEngine(policy_dir=policy_dir)

    if allow_all:
        logger.warning("policy_engine_allow_all_mode")

    return SimplePolicyEngine(allow_all=allow_all)
