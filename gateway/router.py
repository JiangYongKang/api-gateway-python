"""Prefix-based router: first matching rule wins."""
from __future__ import annotations

from typing import Any

from gateway.config import RouteRule
from gateway.errors import ErrorCode, GatewayError
from gateway.models import Request


class Router:
    """Matches requests against an ordered list of :class:`RouteRule`.

    Matching rule (deterministic, explainable):
      1. rules are evaluated strictly in configuration order;
      2. a rule matches when ``request.path`` starts with the rule prefix
         (prefix must end on a path segment boundary);
      3. when ``methods`` is set, the request method must be listed.
    The first match wins; if nothing matches, ``ROUTE_NOT_FOUND`` is raised
    -- the request is never forwarded to an unrelated target.
    """

    def __init__(self, rules: tuple[RouteRule, ...]) -> None:
        self._rules = rules

    @staticmethod
    def _prefix_matches(prefix: str, path: str) -> bool:
        if not path.startswith(prefix):
            return False
        if len(path) == len(prefix):
            return True
        # Segment boundary: "/v1" matches "/v1/x" but not "/v10/x".
        return prefix.endswith("/") or path[len(prefix)] == "/"

    def match(self, request: Request) -> RouteRule:
        method = request.method.upper()
        for rule in self._rules:
            if not self._prefix_matches(rule.prefix, request.path):
                continue
            if rule.methods is not None and method not in {m.upper() for m in rule.methods}:
                continue
            return rule
        raise GatewayError(
            ErrorCode.ROUTE_NOT_FOUND,
            f"no route for {method} {request.path}",
            details={
                "reason": "no_matching_rule",
                "method": method,
                "path": request.path,
                "tried": [
                    {
                        "rule_id": r.id,
                        "prefix": r.prefix,
                        "prefix_match": self._prefix_matches(r.prefix, request.path),
                        "method_match": r.methods is None
                        or method in {m.upper() for m in r.methods},
                    }
                    for r in self._rules
                ],
            },
        )

    def explain(self, request: Request) -> dict[str, Any]:
        try:
            rule = self.match(request)
            return {"matched": True, "rule_id": rule.id}
        except GatewayError as exc:
            return {"matched": False, "details": exc.details}
