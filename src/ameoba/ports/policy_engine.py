"""PolicyEngine protocol — authorization contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ameoba.domain.security import AuthzDecision, AuthzRequest


@runtime_checkable
class PolicyEngine(Protocol):
    """Synchronous authorization check performed in the query hot path."""

    def authorize(self, request: AuthzRequest) -> AuthzDecision:
        """Evaluate whether the principal may perform the action on the resource.

        Must be synchronous and fast (Cedar targets sub-millisecond evaluation).
        """
        ...
