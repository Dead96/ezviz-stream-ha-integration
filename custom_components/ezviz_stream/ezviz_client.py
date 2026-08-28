"""EZVIZ client for the On-Demand integration.

This device (a battery-powered "peephole"/cat-eye camera) doesn't expose
RTSP at all - confirmed by `localRtspPort`/`netRtspPort` being 0 in its own
EZVIZ connection metadata. It only streams through EZVIZ's cloud VTM relay
(a proprietary `ysproto` TCP protocol). We don't reimplement that protocol
ourselves: we depend on `pyezvizapi` (the same library the official EZVIZ
Home Assistant integration uses for login/device management), which also
ships a `stream proxy` CLI helper that speaks VTM, remuxes it with ffmpeg,
and re-serves it as plain HTTP MPEG-TS - something HA's stream component
can open directly as a `stream_source()`.

Verified manually against this exact device: a `pyezvizapi` login (v5,
MD5-hashed password) followed by pulling the VTM cloud stream returned real
live video bytes.

Everything here only runs when a client actually asks for the stream:
login and the proxy subprocess are both started lazily, the first time
`async_get_stream_url()` is called - never on a timer.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Loopback-only: the proxy's stream URL carries no auth of its own.
PROXY_LISTEN_HOST = "127.0.0.1"
_PROXY_PORT_BASE = 8558
_PROXY_PORT_RANGE = 100
_PROXY_READY_TIMEOUT = 5.0


class EzvizAuthError(Exception):
    """Raised when authentication with EZVIZ fails."""


class EzvizClient:
    """Manages an EZVIZ login session and one on-demand stream proxy per device."""

    def __init__(
        self,
        hass: HomeAssistant,
        account: str,
        password: str,
        token_path: Path,
    ) -> None:
        """Initialize the client. No network I/O or subprocess happens here."""
        self._hass = hass
        self._account = account
        self._password = password
        self._token_path = token_path

        self._client: Any | None = None  # pyezvizapi.client.EzvizClient, once logged in
        self._proxy_processes: dict[str, asyncio.subprocess.Process] = {}
        self._proxy_ports: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def async_get_stream_url(self, serial: str) -> str:
        """Return an HTTP MPEG-TS URL for `serial`, starting its proxy if needed."""
        async with self._lock:
            await self._async_ensure_logged_in()
            port = await self._async_ensure_proxy_running(serial)
        return f"http://{PROXY_LISTEN_HOST}:{port}/{serial}.ts"

    async def async_close(self) -> None:
        """Stop any running proxy subprocesses. Called on config entry unload."""
        async with self._lock:
            for serial, process in self._proxy_processes.items():
                if process.returncode is None:
                    _LOGGER.debug("Stopping EZVIZ stream proxy for %s", serial)
                    process.terminate()
            self._proxy_processes.clear()
            self._proxy_ports.clear()

    async def _async_ensure_logged_in(self) -> None:
        """Log in (or refresh the existing session) via pyezvizapi.

        Reuses a cached session token from disk when available so a restart
        doesn't require a fresh password login. Safe to call repeatedly -
        pyezvizapi's own `login()` only talks to the network when the
        cached session is missing or expired.
        """
        from pyezvizapi.client import EzvizClient as PyEzvizClient
        from pyezvizapi.exceptions import PyEzvizError

        if self._client is None:
            token = await self._hass.async_add_executor_job(self._load_token)
            self._client = PyEzvizClient(
                account=self._account, password=self._password, token=token
            )

        try:
            await self._hass.async_add_executor_job(self._client.login)
        except PyEzvizError as err:
            raise EzvizAuthError(f"EZVIZ login failed: {err}") from err

        await self._hass.async_add_executor_job(self._save_token)

    async def _async_ensure_proxy_running(self, serial: str) -> int:
        """Start the `pyezvizapi stream proxy` subprocess for `serial` if needed."""
        process = self._proxy_processes.get(serial)
        if process is not None and process.returncode is None:
            return self._proxy_ports[serial]

        port = _port_for_serial(serial)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "pyezvizapi",
            "--token-file",
            str(self._token_path),
            "stream",
            "proxy",
            "--serial",
            serial,
            "--listen-host",
            PROXY_LISTEN_HOST,
            "--listen-port",
            str(port),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self._proxy_processes[serial] = process
        self._proxy_ports[serial] = port

        await _async_wait_for_port(PROXY_LISTEN_HOST, port, _PROXY_READY_TIMEOUT)
        return port

    def _load_token(self) -> dict[str, Any] | None:
        """Load a cached pyezvizapi session token from disk, if any."""
        if not self._token_path.exists():
            return None
        try:
            return json.loads(self._token_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            _LOGGER.warning("Failed to read cached EZVIZ token at %s", self._token_path)
            return None

    def _save_token(self) -> None:
        """Persist the current pyezvizapi session token to disk.

        This is the hand-off mechanism to the separate `stream proxy`
        subprocess, which authenticates via `--token-file` instead of
        being passed the account password on its command line.
        """
        assert self._client is not None
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(
            json.dumps(self._client.export_token()), encoding="utf-8"
        )


def _port_for_serial(serial: str) -> int:
    """Deterministically map a device serial to a loopback port.

    Keeps multiple cameras from colliding on the same port without needing
    extra user-facing configuration.
    """
    digest = int(hashlib.sha1(serial.encode()).hexdigest(), 16)
    return _PROXY_PORT_BASE + (digest % _PROXY_PORT_RANGE)


async def _async_wait_for_port(host: str, port: int, timeout: float) -> None:
    """Wait until `host:port` accepts TCP connections, or give up silently.

    Best-effort only: if the proxy is slow to bind we still return the URL
    and let the stream client's own retry/backoff handle it.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            _, writer = await asyncio.open_connection(host, port)
        except OSError:
            await asyncio.sleep(0.2)
            continue
        writer.close()
        await writer.wait_closed()
        return
    _LOGGER.debug("EZVIZ stream proxy on port %s not ready after %.1fs", port, timeout)
