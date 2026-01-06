"""
Health check API endpoints.
"""
from fastapi import APIRouter, Response
from services.health_monitor import get_health_monitor

health_router = APIRouter(prefix="/health", tags=["Health"])


@health_router.get("")
async def health_check():
    """Quick health check endpoint."""
    monitor = get_health_monitor()
    result = await monitor.get_full_health(use_cache=True)
    return result


@health_router.get("/detailed")
async def detailed_health_check():
    """Detailed health check (bypasses cache)."""
    monitor = get_health_monitor()
    result = await monitor.get_full_health(use_cache=False)
    return result


@health_router.get("/ready")
async def readiness_check(response: Response):
    """Kubernetes-style readiness probe."""
    monitor = get_health_monitor()
    result = await monitor.get_full_health(use_cache=True)

    if result["status"] != "healthy":
        response.status_code = 503

    return {"ready": result["status"] == "healthy"}


@health_router.get("/live")
async def liveness_check():
    """Kubernetes-style liveness probe."""
    return {"alive": True}

