"""路由匹配：最长前缀匹配，无匹配时给出明确错误。"""

from __future__ import annotations

from .config import GatewayConfig, RouteConfig
from .errors import GatewayError, GatewayErrorType


class Router:
    def __init__(self, config: GatewayConfig) -> None:
        # 预排序便于说明匹配过程
        self._routes = config.routes

    def match(self, method: str, path: str) -> tuple[RouteConfig | None, str]:
        """返回 (匹配到的路由, 判定依据)；未匹配时路由为 None。

        采用最长前缀匹配：多个前缀同时命中时取最长者，规则确定无歧义。
        """
        candidates = [
            r for r in self._routes if path.startswith(r.path_prefix)
        ]
        if not candidates:
            prefixes = [r.path_prefix for r in self._routes]
            return None, f"no prefix matches path={path!r} among={prefixes}"
        best = max(candidates, key=lambda r: len(r.path_prefix))
        matched = [r.path_prefix for r in candidates]
        return best, (
            f"longest-prefix match: candidates={matched} -> "
            f"route={best.id} prefix={best.path_prefix!r}"
        )

    def match_or_raise(self, method: str, path: str) -> tuple[RouteConfig, str]:
        route, reason = self.match(method, path)
        if route is None:
            raise GatewayError(
                GatewayErrorType.ROUTE_NOT_FOUND,
                f"未找到匹配的路由: {method} {path}",
                details={"reason": reason},
            )
        return route, reason
