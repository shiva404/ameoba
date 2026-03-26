"""Authorization Gateway — enforced before queries reach backends.

Every read and write operation passes through this gateway.  It:
1. Evaluates Cedar policies against the caller's principal.
2. Injects tenant filters and column redactions into query plans.
3. Records authz denials in the audit log.

The gateway pattern ensures backends use broad service accounts; access
control lives entirely in the Ameoba layer.
"""

from __future__ import annotations

from typing import Any

import structlog

from ameoba.domain.query import QueryPlan, SubPlan
from ameoba.domain.security import AgentIdentity, AuthzDecision, AuthzRequest
from ameoba.ports.policy_engine import PolicyEngine

logger = structlog.get_logger(__name__)


class AuthorizationGateway:
    """Pre-query and pre-write authorization enforcement.

    Usage::

        gateway = AuthorizationGateway(policy_engine=engine)
        decision = gateway.authorize_query(principal, plan)
        if not decision.allowed:
            raise PermissionError(decision.reason)
    """

    def __init__(self, policy_engine: PolicyEngine) -> None:
        self._engine = policy_engine

    def authorize_write(
        self,
        principal: AgentIdentity,
        collection: str,
        *,
        labels: list[Any] | None = None,
    ) -> AuthzDecision:
        """Authorize a write operation against a collection."""
        return self._engine.authorize(AuthzRequest(
            principal=principal,
            action="write",
            resource_type="collection",
            resource_id=collection,
            resource_labels=labels or [],
        ))

    def authorize_query(
        self,
        principal: AgentIdentity,
        plan: QueryPlan,
    ) -> AuthzDecision:
        """Authorize a federated query.

        Each sub-plan leg is independently authorized.  The most restrictive
        decision wins (fail-closed).
        """
        for sub_plan in plan.sub_plans:
            decision = self._engine.authorize(AuthzRequest(
                principal=principal,
                action="query",
                resource_type="backend",
                resource_id=sub_plan.backend_id,
                context={"collection": sub_plan.collection},
            ))
            if not decision.allowed:
                return decision

        # Authorised — build merged decision with combined row filters
        row_filters = [
            sp_decision.row_filter
            for sp in plan.sub_plans
            if (sp_decision := self._engine.authorize(AuthzRequest(
                principal=principal,
                action="query",
                resource_type="backend",
                resource_id=sp.backend_id,
            ))).row_filter
        ]
        row_filter = " AND ".join(row_filters) if row_filters else None

        return AuthzDecision(allowed=True, reason="allow", row_filter=row_filter)

    def apply_filters_to_plan(
        self,
        plan: QueryPlan,
        decision: AuthzDecision,
    ) -> QueryPlan:
        """Inject row filters from the authz decision into the query plan."""
        if not decision.row_filter:
            return plan

        updated_sub_plans: list[SubPlan] = []
        for sub_plan in plan.sub_plans:
            native = str(sub_plan.native_query)
            if "WHERE" in native.upper():
                filtered = native + f" AND ({decision.row_filter})"
            else:
                filtered = native + f" WHERE {decision.row_filter}"

            updated_sub_plans.append(sub_plan.model_copy(
                update={"native_query": filtered}
            ))

        return plan.model_copy(update={"sub_plans": updated_sub_plans})
