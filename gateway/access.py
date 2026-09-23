"""来源白名单 / 黑名单判定。

规则（按顺序短路）：
1. 命中黑名单 -> 拒绝，原因 blocked
2. 白名单非空且来源不在白名单 -> 拒绝，原因 not_allowed
3. 其余放行，原因 allowed

客户端标识支持 IP，也支持带 CIDR 的简单网段（如 10.0.0.0/8）。
"""
from __future__ import annotations

import ipaddress


class AccessController:
    def __init__(self, allowlist: frozenset[str], blocklist: frozenset[str]) -> None:
        self._allow = self._compile(allowlist)
        self._block = self._compile(blocklist)

    @staticmethod
    def _compile(entries: frozenset[str]):
        nets: list[ipaddress._BaseNetwork] = []
        for e in entries:
            nets.append(ipaddress.ip_network(e, strict=False))
        return nets

    def _hit(self, nets, client: str) -> bool:
        try:
            addr = ipaddress.ip_address(client)
        except ValueError:
            return False
        return any(addr in net for net in nets)

    def decide(self, client: str) -> tuple[bool, str]:
        """返回 (是否放行, 判定原因)。"""
        if self._hit(self._block, client):
            return False, "blocked"
        if self._allow and not self._hit(self._allow, client):
            return False, "not_allowed"
        return True, "allowed"
