"""Menu registry heartbeat service."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from app.config import get_service_yaml

logger = logging.getLogger("dfa.registry")


class RegistryService:
    def __init__(self):
        self._cfg = get_service_yaml().registry
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def _register_url(self) -> str:
        return f"{self._cfg.menu_service_url}/api/menu/register"

    def _heartbeat_url(self) -> str:
        return f"{self._cfg.menu_service_url}/api/menu/heartbeat/{self._cfg.service_id}"

    def _payload(self) -> dict:
        m = self._cfg.menu
        return {
            "service_id": self._cfg.service_id,
            "service_name": self._cfg.service_name,
            "api_prefix": self._cfg.api_prefix,
            "host": self._cfg.host,
            "port": self._cfg.port,
            "maturity": self._cfg.maturity,
            "description": self._cfg.description,
            "menu_item": {
                "id": m.id,
                "name": m.level2.name or self._cfg.service_name,
                "path": m.path,
                "icon": m.icon,
                "order": m.order,
                "level1": {"name": m.level1.name, "name_en": m.level1.name_en},
                "level2": {"name": m.level2.name, "name_en": m.level2.name_en},
                "level3": {"name": m.level3.name, "name_en": m.level3.name_en},
            },
        }

    async def register(self) -> bool:
        if not self._cfg.enabled:
            return True
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(self._register_url(), json=self._payload())
            ok = resp.status_code in (200, 201)
            if not ok:
                logger.warning("menu register failed: %s %s", resp.status_code, resp.text[:200])
            return ok
        except Exception as exc:
            logger.warning("menu register error: %s", exc)
            return False

    async def heartbeat(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(self._heartbeat_url())
            if resp.status_code == 404:
                await self.register()
                return False
            return resp.status_code == 200
        except Exception:
            return False

    async def _loop(self) -> None:
        while self._running:
            await self.heartbeat()
            await asyncio.sleep(self._cfg.heartbeat_interval_seconds)

    def start(self) -> None:
        if not self._cfg.enabled:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="registry_heartbeat")
        logger.info("Registry heartbeat started (interval=%ds)", self._cfg.heartbeat_interval_seconds)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()


_registry_service: RegistryService | None = None


def get_registry_service() -> RegistryService:
    global _registry_service
    if _registry_service is None:
        _registry_service = RegistryService()
    return _registry_service
