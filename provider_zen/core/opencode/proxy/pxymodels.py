"""Data classes for opencode proxy pool.

Split out from ``proxypool.py`` to keep files under the 400-line limit.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List


@dataclass
class ProxyInfo:
    """Metadata for a single proxy."""

    ip: str
    port: int
    protocol: str = "http"
    country: str = ""
    response_time: float = 0.0  # ms
    response_ms: float = 0.0  # alias kept for convenience
    last_verified: str = ""
    anonymity: str = ""

    @property
    def address(self) -> str:
        return "{}:{}".format(self.ip, self.port)

    def __hash__(self) -> int:
        return hash(self.address)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProxyInfo):
            return NotImplemented
        return self.address == other.address


@dataclass
class ProxyPool:
    """Collection of proxies with deduplication.

    Attributes:
        proxies: List of proxy entries.
        fetch_time: ISO-8601 timestamp string of when the pool was fetched.
        fetch_time_epoch: Unix timestamp (float) for age calculations.
        total_available: Total available proxies reported by source.
    """

    proxies: List[ProxyInfo] = field(default_factory=list)
    _seen: set = field(default_factory=set)
    fetch_time: str = ""
    fetch_time_epoch: float = 0.0
    total_available: int = 0

    def __post_init__(self) -> None:
        """Set fetch_time_epoch from fetch_time if not already set."""
        if self.fetch_time_epoch == 0.0 and self.fetch_time:
            try:
                self.fetch_time_epoch = time.mktime(
                    time.strptime(self.fetch_time, "%Y-%m-%dT%H:%M:%SZ")
                )
            except (ValueError, OverflowError):
                self.fetch_time_epoch = 0.0

    def add(self, p: ProxyInfo) -> None:
        """Add a proxy, skipping duplicates by address."""
        if p.address not in self._seen:
            self._seen.add(p.address)
            self.proxies.append(p)

    def add_many(self, items: List[ProxyInfo]) -> None:
        for p in items:
            self.add(p)

    @property
    def count(self) -> int:
        return len(self.proxies)

    def sort_by_speed(self) -> None:
        """Sort proxies by response_time ascending (fastest first)."""
        self.proxies.sort(key=lambda p: p.response_time if p.response_time > 0 else float("inf"))

    def to_address_list(self) -> List[str]:
        return [p.address for p in self.proxies]

    def to_dict(self) -> dict:
        return {
            "fetch_time": self.fetch_time,
            "fetch_time_epoch": self.fetch_time_epoch,
            "total_available": self.total_available,
            "proxies": [
                {
                    "ip": p.ip,
                    "port": p.port,
                    "protocol": p.protocol,
                    "country": p.country,
                    "response_time": p.response_time,
                    "last_verified": p.last_verified,
                    "anonymity": p.anonymity,
                }
                for p in self.proxies
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ProxyPool":
        pool = cls(
            fetch_time=data.get("fetch_time", ""),
            fetch_time_epoch=data.get("fetch_time_epoch", 0.0),
            total_available=data.get("total_available", 0),
        )
        for item in data.get("proxies", []):
            pool.add(ProxyInfo(
                ip=item["ip"],
                port=item["port"],
                protocol=item.get("protocol", "http"),
                country=item.get("country", ""),
                response_time=item.get("response_time", 0.0),
                last_verified=item.get("last_verified", ""),
                anonymity=item.get("anonymity", ""),
            ))
        return pool
