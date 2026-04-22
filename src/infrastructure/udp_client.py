import asyncio
import json
import socket
from datetime import datetime, timezone
from typing import Any, Dict

from src.utils.logger import get_logger

logger = get_logger(__name__)


class UdpEventClient:
    def __init__(self, host: str = "titlis-api", port: int = 8125) -> None:
        self._host = host
        self._port = port

    def _send_sync(self, msg: bytes) -> None:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(msg, (self._host, self._port))
        except Exception:
            logger.exception("Falha ao enviar evento UDP", extra={"host": self._host, "port": self._port})

    async def send(self, event_type: str, tenant_id: int, data: Dict[str, Any]) -> None:
        payload = {
            "v": 1,
            "t": event_type,
            "ts": datetime.now(timezone.utc).isoformat(),
            "tenant_id": tenant_id,
            "data": data,
        }
        msg = json.dumps(payload).encode()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._send_sync, msg)
