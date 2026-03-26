"""RFC 8693 Token Exchange — agent-to-agent delegation.

Allows Agent A to delegate to Agent B with a capped permission set:
    effective_permissions = intersection(A's delegated scope, B's own permissions)

Max delegation depth: 3 levels.

The ``act`` claim (RFC 8693 §4.1) embeds both identities so the audit trail
shows both the original delegator and the acting agent.
"""

from __future__ import annotations

from ameoba.domain.security import AgentIdentity


MAX_DELEGATION_DEPTH = 3


def create_delegation(
    delegator: AgentIdentity,
    delegate: AgentIdentity,
    *,
    delegated_scopes: list[str] | None = None,
) -> AgentIdentity:
    """Create a delegated identity for delegate acting on behalf of delegator.

    Args:
        delegator:        The agent granting the delegation.
        delegate:         The agent receiving the delegation.
        delegated_scopes: Scopes being delegated (subset of delegator's scopes).
                          If None, delegates all of delegator's scopes.

    Returns:
        A new ``AgentIdentity`` for the delegated context.

    Raises:
        ValueError: If delegation depth would exceed the maximum.
    """
    new_depth = delegator.delegation_depth + 1
    if new_depth > MAX_DELEGATION_DEPTH:
        raise ValueError(
            f"Delegation depth {new_depth} exceeds maximum of {MAX_DELEGATION_DEPTH}"
        )

    # Effective scopes = intersection of both
    if delegated_scopes is not None:
        effective_scopes = list(
            set(delegated_scopes) & set(delegator.scopes) & set(delegate.scopes)
        )
    else:
        effective_scopes = list(set(delegator.scopes) & set(delegate.scopes))

    return AgentIdentity(
        agent_id=delegate.agent_id,
        tenant_id=delegator.tenant_id,  # Tenant is the delegator's
        groups=delegate.groups,
        session_id=delegate.session_id,
        scopes=effective_scopes,
        delegated_by=delegator.agent_id,
        delegation_depth=new_depth,
    )
