"""EZVIZ client for the On-Demand integration.

This device (a battery-powered "peephole"/cat-eye camera) doesn't expose
RTSP at all - confirmed by `localRtspPort`/`netRtspPort` being 0 in its own
EZVIZ connection metadata. It only streams through EZVIZ's cloud VTM relay
(a proprietary `ysproto` TCP protocol).

We don't reimplement that protocol ourselves: we vendor the relevant parts
of `pyezvizapi` (github.com/RenierM26/pyEzvizApi, Apache-2.0 - see
`vendor/pyezvizapi/`), which can speak VTM and remux it with `ffmpeg` into
plain MPEG-TS bytes. This is vendored rather than declared as a pip
requirement because the login/cloud-stream helpers used here live on the
project's `main` branch but haven't made it into the version currently
published on PyPI.

Everything here only runs when a client actually asks for the stream:
login happens lazily on the first `async_get_stream_url()` call, and the
actual EZVIZ connection is only opened once someone GETs the HTTP view
below - never on a timer, never in the background.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from aiohttp import web
from homeassistant.components.http import HomeAssistantView
from homeassistant.components.http.auth import async_sign_path
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

_SIGNED_URL_EXPIRATION = timedelta(minutes=5)

# Placeholder video shown while the real EZVIZ VTM connection (which can
# take anywhere from a few seconds to over a minute - this looks like
# inherent battery-camera wake-up latency) is still being established. A
# pre-rendered static image looped with ffmpeg - no drawtext, so nothing
# depends on a font being present in whatever ffmpeg build the Home
# Assistant host happens to have.
_PLACEHOLDER_IMAGE = Path(__file__).parent / "assets" / "placeholder.png"
_PLACEHOLDER_FPS = "2"
_FOLLOWER_PLACEHOLDER_MAX_SECONDS = 15.0

# The device closes the VTM stream after a fixed watch duration (~60s
# observed) unless renewed - a battery-saving measure the official app
# presumably works around with a "keep watching?" prompt. `delay_battery_
# device_sleep` (undocumented) is our best-effort stand-in: called
# periodically while a client is actually watching. Testing showed calling
# it too often gets rejected (looks like the server only accepts a renewal
# close to the current grant's expiry, not earlier) - 30s was the best
# result found by trial and error against a noisy, undocumented API, not a
# value with a known-correct justification. A short retry covers isolated
# failures without waiting a full extra interval. See "delay_battery_
# device_sleep" call below and CHANGELOG for the testing history.
_KEEPALIVE_INTERVAL_SECONDS = 30.0
_KEEPALIVE_MAX_RETRIES = 3
_KEEPALIVE_RETRY_DELAY_SECONDS = 5.0
_KEEPALIVE_CHANNEL = 1
_KEEPALIVE_SLEEP_TYPE = 1


class EzvizAuthError(Exception):
    """Raised when authentication with EZVIZ fails."""


class EzvizClient:
    """Manages one EZVIZ login session for a config entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        account: str,
        password: str,
        token_path: Path,
    ) -> None:
        """Initialize the client. No network I/O happens here."""
        self._hass = hass
        self._account = account
        self._password = password
        self._token_path = token_path

        self._client: Any | None = None  # vendored pyezvizapi.client.EzvizClient
        self._login_lock = asyncio.Lock()
        self._stream_locks: dict[str, asyncio.Lock] = {}

    def stream_lock_for(self, serial: str) -> asyncio.Lock:
        """Return the lock serializing stream attempts for one serial.

        The device only supports one active VTM/P2P session at a time, but
        HA's go2rtc layer can open several near-simultaneous connections to
        our stream view while probing/retrying a new source. Without this,
        those attempts fight over the device's single session slot and
        several time out with "Device offline or unreachable" even though
        the device is fine - they just needed to go one at a time.
        """
        return self._stream_locks.setdefault(serial, asyncio.Lock())

    async def async_get_stream_url(self, entry_id: str, serial: str) -> str:
        """Return a signed URL for this HA instance's own stream view.

        The view itself (`EzvizStreamView`) is what actually opens the
        EZVIZ cloud connection, and only does so once a client GETs this
        URL - so nothing happens here beyond ensuring we're logged in.
        """
        await self._async_ensure_logged_in()
        path = EzvizStreamView.url_template(entry_id, serial)
        signed_path = async_sign_path(self._hass, path, _SIGNED_URL_EXPIRATION)
        port = self._hass.http.server_port
        return f"http://127.0.0.1:{port}{signed_path}"

    async def async_close(self) -> None:
        """Nothing to tear down - kept for symmetry with async_setup_entry."""

    async def async_ensure_logged_in(self) -> Any:
        """Ensure we're logged in and return the underlying vendored client.

        Used by `EzvizStreamView` right before it opens the actual EZVIZ
        cloud connection.
        """
        await self._async_ensure_logged_in()
        assert self._client is not None
        return self._client

    async def _async_ensure_logged_in(self) -> None:
        """Log in (or refresh the existing session) via the vendored client.

        Reuses a cached session token from disk when available so a
        restart doesn't require a fresh password login. Safe to call
        repeatedly - the vendored client's own `login()` only talks to the
        network when the cached session is missing or expired.
        """
        from .vendor.pyezvizapi.client import EzvizClient as PyEzvizClient
        from .vendor.pyezvizapi.exceptions import PyEzvizError

        async with self._login_lock:
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
        """Persist the current pyezvizapi session token to disk."""
        assert self._client is not None
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(
            json.dumps(self._client.export_token()), encoding="utf-8"
        )


class _QueueOutput:
    """File-like bridge from the sync `copy_cloud_stream_to_mpegts` call
    (running in an executor thread) to the async HTTP response writing the
    bytes out to whoever asked for the stream.
    """

    def __init__(self, hass: HomeAssistant, queue: asyncio.Queue[bytes | None]) -> None:
        self._loop = hass.loop
        self._queue = queue
        self._closed = threading.Event()

    def write(self, data: bytes) -> int:
        """Called from the executor thread with each remuxed chunk."""
        if self._closed.is_set():
            raise BrokenPipeError("EZVIZ stream view: HTTP client disconnected")
        self._loop.call_soon_threadsafe(self._queue.put_nowait, data)
        return len(data)

    def flush(self) -> None:
        """No-op: each write() is already handed off immediately."""

    def close(self) -> None:
        """Signal the executor thread's next write() to stop cleanly."""
        self._closed.set()
        self._loop.call_soon_threadsafe(self._queue.put_nowait, None)


async def _stream_placeholder(
    response: web.StreamResponse,
    stop_event: asyncio.Event,
    max_duration: float | None = None,
) -> None:
    """Write a looping placeholder video to `response` until `stop_event` fires.

    Best-effort: if ffmpeg can't be started, just returns immediately and
    the caller proceeds without a placeholder.
    """
    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-re",
            "-loop",
            "1",
            "-i",
            str(_PLACEHOLDER_IMAGE),
            "-r",
            _PLACEHOLDER_FPS,
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "mpegts",
            "-muxdelay",
            "0",
            "pipe:1",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError:
        _LOGGER.debug("EZVIZ stream placeholder: could not start ffmpeg")
        return

    deadline = time.monotonic() + max_duration if max_duration is not None else None
    try:
        assert process.stdout is not None
        while not stop_event.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                chunk = await asyncio.wait_for(process.stdout.read(32768), timeout=0.5)
            except TimeoutError:
                continue
            if not chunk:
                break
            await response.write(chunk)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        if process.returncode is None:
            process.kill()
        with contextlib.suppress(ProcessLookupError):
            await process.wait()


async def _keepalive_loop(
    hass: HomeAssistant,
    pyez_client: Any,
    serial: str,
    stop_event: asyncio.Event,
) -> None:
    """Periodically call `delay_battery_device_sleep` while a client is
    watching, to stop the device closing the stream after its default
    watch duration. Best-effort: failures are logged and retried a few
    times, but never interrupt the actual video stream.
    """
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_KEEPALIVE_INTERVAL_SECONDS)
            return  # stop_event fired - client disconnected
        except TimeoutError:
            pass

        for attempt in range(1, _KEEPALIVE_MAX_RETRIES + 1):
            try:
                result = await hass.async_add_executor_job(
                    pyez_client.delay_battery_device_sleep,
                    serial,
                    _KEEPALIVE_CHANNEL,
                    _KEEPALIVE_SLEEP_TYPE,
                )
                _LOGGER.debug("EZVIZ stream %s: keep-alive ok: %s", serial, result)
                break
            except Exception:  # noqa: BLE001 - best-effort, never fatal to the stream
                _LOGGER.debug(
                    "EZVIZ stream %s: keep-alive failed (attempt %d/%d)",
                    serial,
                    attempt,
                    _KEEPALIVE_MAX_RETRIES,
                    exc_info=True,
                )
                if attempt < _KEEPALIVE_MAX_RETRIES:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=_KEEPALIVE_RETRY_DELAY_SECONDS
                        )
                        return  # stop_event fired during the retry wait
                    except TimeoutError:
                        pass


class EzvizStreamView(HomeAssistantView):
    """Serves the EZVIZ cloud VTM stream as HTTP MPEG-TS.

    Registered once for the whole integration (not per config entry); the
    entry/serial are part of the URL and looked up at request time. Access
    is via a short-lived signed URL (see `EzvizClient.async_get_stream_url`)
    rather than a normal bearer token, since the stream is opened by HA's
    own ffmpeg subprocess, which can't supply one.
    """

    url = "/api/ezviz_stream/{entry_id}/{serial}.ts"
    name = "api:ezviz_stream"

    @staticmethod
    def url_template(entry_id: str, serial: str) -> str:
        """Build the concrete path for one entry/serial pair."""
        return f"/api/ezviz_stream/{entry_id}/{serial}.ts"

    async def get(self, request: web.Request, entry_id: str, serial: str) -> web.StreamResponse:
        """Handle a GET: log in if needed, then stream MPEG-TS until the client leaves."""
        t0 = time.monotonic()
        hass: HomeAssistant = request.app["hass"]
        client: EzvizClient | None = hass.data.get(DOMAIN, {}).get(entry_id)
        if client is None:
            return web.Response(status=404, text="Unknown EZVIZ Stream config entry")

        from .vendor.pyezvizapi.cloud_stream import copy_cloud_stream_to_mpegts
        from .vendor.pyezvizapi.exceptions import PyEzvizError

        response = web.StreamResponse(
            status=200, headers={"Content-Type": "video/mp2t"}
        )
        await response.prepare(request)
        _LOGGER.debug("EZVIZ stream %s: response prepared at +%.2fs", serial, time.monotonic() - t0)

        # The device only accepts one active VTM/P2P session at a time, and
        # go2rtc can open several near-simultaneous connections to this view
        # while probing/retrying a new source. Only one of them ("owner")
        # actually attempts the EZVIZ connection; the rest ("followers") just
        # get the placeholder for a bit and then close, instead of either
        # fighting the owner for the device's one session slot or queuing
        # behind it (queuing made things worse: go2rtc's patience per
        # attempt is only a few seconds, far less than a slow/hung attempt
        # can take, so everything downstream of a queue timed out anyway).
        lock = client.stream_lock_for(serial)
        is_owner = not lock.locked()
        if is_owner:
            await lock.acquire()

        real_data_ready = asyncio.Event()
        placeholder_task = hass.async_create_task(
            _stream_placeholder(
                response,
                real_data_ready,
                max_duration=None if is_owner else _FOLLOWER_PLACEHOLDER_MAX_SECONDS,
            )
        )

        if not is_owner:
            _LOGGER.debug(
                "EZVIZ stream %s: another attempt already in flight, placeholder-only at +%.2fs",
                serial,
                time.monotonic() - t0,
            )
            await placeholder_task
            return response

        try:
            try:
                pyez_client = await client.async_ensure_logged_in()
            except EzvizAuthError:
                _LOGGER.exception("EZVIZ stream %s: login failed", serial)
                return response
            _LOGGER.debug("EZVIZ stream %s: logged in at +%.2fs", serial, time.monotonic() - t0)

            queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=64)
            output = _QueueOutput(hass, queue)

            def _copy() -> None:
                try:
                    copy_cloud_stream_to_mpegts(pyez_client, serial, output)
                except (PyEzvizError, OSError):
                    _LOGGER.exception("EZVIZ stream copy failed for %s", serial)
                finally:
                    output.close()

            copy_job = hass.async_add_executor_job(_copy)

            keepalive_stop = asyncio.Event()
            keepalive_task = hass.async_create_task(
                _keepalive_loop(hass, pyez_client, serial, keepalive_stop)
            )

            first_chunk = True
            try:
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    if first_chunk:
                        _LOGGER.debug(
                            "EZVIZ stream %s: first chunk (%d bytes) at +%.2fs",
                            serial,
                            len(chunk),
                            time.monotonic() - t0,
                        )
                        first_chunk = False
                        # Real data is ready: let the placeholder finish its
                        # current write and stop before we write anything
                        # else, so the two never interleave on `response`.
                        real_data_ready.set()
                        await placeholder_task
                    await response.write(chunk)
            except (ConnectionResetError, asyncio.CancelledError):
                _LOGGER.debug(
                    "EZVIZ stream %s: client disconnected at +%.2fs", serial, time.monotonic() - t0
                )
            finally:
                output.close()
                await copy_job
                keepalive_stop.set()
                await keepalive_task
            _LOGGER.debug("EZVIZ stream %s: copy job finished at +%.2fs", serial, time.monotonic() - t0)
        finally:
            real_data_ready.set()
            if not placeholder_task.done():
                await placeholder_task
            lock.release()

        return response
