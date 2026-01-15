"""
Health monitoring service for system components.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

from sqlalchemy import select, func, text
from config.database_config import get_db_session
from config.redis_config import get_redis
from config.logger import logger
from models import PriceHistoryIntraday, PriceHistoryDaily, Instrument


@dataclass
class ComponentHealth:
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


class HealthMonitor:
    """Monitors health of all system components."""

    def __init__(self):
        self._last_check: Optional[datetime] = None
        self._cached_result: Optional[Dict[str, Any]] = None
        self._cache_ttl_seconds = 30

    async def check_database(self) -> ComponentHealth:
        """Check database connectivity and performance."""
        start = datetime.now(timezone.utc)
        try:
            async for session in get_db_session():
                result = await session.execute(text("SELECT 1"))
                _ = result.scalar()
                latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000
                return ComponentHealth(
                    name="database",
                    healthy=True,
                    latency_ms=latency,
                    message="Connected",
                )
        except Exception as e:
            latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            return ComponentHealth(
                name="database",
                healthy=False,
                latency_ms=latency,
                message=f"Error: {str(e)[:100]}",
            )

    async def check_redis(self) -> ComponentHealth:
        """Check Redis connectivity."""
        start = datetime.now(timezone.utc)
        try:
            redis = get_redis()
            if not redis:
                return ComponentHealth(
                    name="redis", healthy=False, message="Redis client not initialized"
                )

            await redis.ping()
            latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000

            # Get memory info
            info = await redis.info("memory")
            used_memory = info.get("used_memory_human", "N/A")

            return ComponentHealth(
                name="redis",
                healthy=True,
                latency_ms=latency,
                message="Connected",
                details={"used_memory": used_memory},
            )
        except Exception as e:
            latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            return ComponentHealth(
                name="redis", healthy=False, latency_ms=latency, message=str(e)[:100]
            )

    async def check_data_quality(self) -> ComponentHealth:
        """Check data quality metrics."""
        try:
            async for session in get_db_session():
                now = datetime.now(timezone.utc)
                one_hour_ago = now - timedelta(hours=1)

                # Count records needing resolution in last hour
                stmt = select(func.count(PriceHistoryIntraday.id)).where(
                    PriceHistoryIntraday.datetime >= one_hour_ago,
                    PriceHistoryIntraday.resolve_required == True,
                )
                unresolved_count = (await session.execute(stmt)).scalar() or 0

                # Count records that exceeded retry limit
                stmt_exceeded = select(func.count(PriceHistoryIntraday.id)).where(
                    PriceHistoryIntraday.datetime >= one_hour_ago,
                    PriceHistoryIntraday.resolve_required == True,
                    PriceHistoryIntraday.resolve_tries >= 3,
                )
                exceeded_count = (await session.execute(stmt_exceeded)).scalar() or 0

                # Count total records in last hour
                stmt_total = select(func.count(PriceHistoryIntraday.id)).where(
                    PriceHistoryIntraday.datetime >= one_hour_ago
                )
                total_count = (await session.execute(stmt_total)).scalar() or 0

                resolution_rate = (
                    ((total_count - unresolved_count) / total_count * 100)
                    if total_count > 0
                    else 100
                )

                healthy = resolution_rate >= 90 and exceeded_count == 0

                return ComponentHealth(
                    name="data_quality",
                    healthy=healthy,
                    message=f"Resolution rate: {resolution_rate:.1f}%",
                    details={
                        "total_records_1h": total_count,
                        "unresolved_records": unresolved_count,
                        "exceeded_retry_limit": exceeded_count,
                        "resolution_rate_percent": round(resolution_rate, 2),
                    },
                )
        except Exception as e:
            return ComponentHealth(
                name="data_quality", healthy=False, message=str(e)[:100]
            )

    async def check_providers(self) -> ComponentHealth:
        """Check provider status."""
        try:
            from services.provider.provider_manager import get_provider_manager

            pm = get_provider_manager()

            if not pm or not pm.providers:
                return ComponentHealth(
                    name="providers", healthy=False, message="No providers initialized"
                )

            provider_status = {}
            all_healthy = True

            for code, provider in pm.providers.items():
                if provider:
                    # Generic status checks
                    is_connected = getattr(provider, "is_connected", False)
                    # Some providers use _running
                    if not is_connected and hasattr(provider, "_running"):
                        is_connected = provider._running

                    subscribed = len(getattr(provider, "subscribed_symbols", []))

                    status_dict = {
                        "connected": is_connected,
                        "subscribed_symbols": subscribed,
                    }

                    # Add "last_data_received" if available (track it in provider if needed)
                    # For now just use connectivity

                    provider_status[code] = status_dict

                    if not is_connected:
                        all_healthy = False
                else:
                    provider_status[code] = {
                        "connected": False,
                        "subscribed_symbols": 0,
                    }

            return ComponentHealth(
                name="providers",
                healthy=all_healthy,
                message=f"Providers: {len(provider_status)} configured",
                details=provider_status,
            )
        except Exception as e:
            return ComponentHealth(
                name="providers", healthy=False, message=str(e)[:100]
            )

    async def get_full_health(self, use_cache: bool = True) -> Dict[str, Any]:
        """Get comprehensive health status of all components."""
        now = datetime.now(timezone.utc)

        # Return cached result if valid
        if use_cache and self._cached_result and self._last_check:
            if (now - self._last_check).total_seconds() < self._cache_ttl_seconds:
                return self._cached_result

        # Run all health checks in parallel
        results = await asyncio.gather(
            self.check_database(),
            self.check_redis(),
            self.check_data_quality(),
            self.check_providers(),
            return_exceptions=True,
        )

        components = {}
        overall_healthy = True

        for result in results:
            if isinstance(result, Exception):
                components["error"] = {"healthy": False, "message": str(result)}
                overall_healthy = False
            elif isinstance(result, ComponentHealth):
                components[result.name] = {
                    "healthy": result.healthy,
                    "latency_ms": result.latency_ms,
                    "message": result.message,
                    "details": result.details,
                }
                if not result.healthy:
                    overall_healthy = False

        health_result = {
            "status": "healthy" if overall_healthy else "unhealthy",
            "timestamp": now.isoformat(),
            "components": components,
        }

        # Cache result
        self._cached_result = health_result
        self._last_check = now

        return health_result


# Singleton instance
_health_monitor: Optional[HealthMonitor] = None


def get_health_monitor() -> HealthMonitor:
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor()
    return _health_monitor
