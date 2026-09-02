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

# Real-world logs showed HA's native `stream` component holding onto a
# `stream_source()` URL and retrying that exact same signed URL for well
# over a minute before giving up - all getting HTTP 401 because our
# previous 5-minute expiration had already lapsed by the time it was
# actually used. The URL only grants access to our own loopback re-stream
# endpoint (no real secret behind it), so a long expiration is low-risk.
_SIGNED_URL_EXPIRATION = timedelta(hours=1)

# Placeholder video shown while the real EZVIZ VTM connection (which can
# take anywhere from a few seconds to over a minute - this looks like
# inherent battery-camera wake-up latency) is still being established. A
# pre-rendered static image looped with ffmpeg - no drawtext, so nothing
# depends on a font being present in whatever ffmpeg build the Home
# Assistant host happens to have.
_PLACEHOLDER_IMAGE = Path(__file__).parent / "assets" / "placeholder.png"
_PLACEHOLDER_FPS = "2"

# Cap on how many chunks a single subscriber can lag behind before we start
# dropping frames for it. Keeps one slow viewer from ever blocking the
# shared upstream or the other viewers.
_SUBSCRIBER_QUEUE_SIZE = 64

# How often an idle viewer (nothing new to write) re-checks whether its
# HTTP connection is still alive. A write() to an already-closed socket can
# still succeed once before the OS surfaces the error, so relying on it
# alone let disconnected viewers (real ones, and go2rtc's own probe
# connections) keep the shared stream - and the real EZVIZ session behind
# it - open indefinitely with nobody actually watching.
_DISCONNECT_POLL_SECONDS = 5.0

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

        # The device only supports one active VTM/P2P session at a time, but
        # several viewers may want to watch at once (two browser tabs, or
        # go2rtc opening its own extra probe connections). Rather than
        # fighting over the device's single session slot, all concurrent
        # viewers of one serial share the same real EZVIZ connection -
        # `_SharedStream` fans its bytes out to every subscriber.
        self._shared_streams: dict[str, _SharedStream] = {}
        self._shared_streams_lock = asyncio.Lock()

    async def async_attach_stream(
        self, serial: str
    ) -> tuple[_SharedStream, asyncio.Queue[bytes | None]]:
        """Attach a new viewer to the shared stream for `serial`.

        Starts the real EZVIZ connection if this is the first viewer for
        that serial; otherwise reuses the one already running. Returns the
        shared stream (needed later to detach) and a queue that will
        receive that stream's MPEG-TS chunks (a `None` marks the end).
        """
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        async with self._shared_streams_lock:
            shared = self._shared_streams.get(serial)
            if shared is None:
                shared = _SharedStream(self._hass, serial)
                self._shared_streams[serial] = shared
            shared.output.add_subscriber(queue)

        try:
            pyez_client = await self.async_ensure_logged_in()
            await shared.ensure_started(pyez_client)
        except Exception:
            await self.async_detach_stream(serial, shared, queue)
            raise
        return shared, queue

    async def async_detach_stream(
        self, serial: str, shared: _SharedStream, queue: asyncio.Queue[bytes | None]
    ) -> None:
        """Detach a viewer, tearing down the shared stream once nobody is left watching."""
        shared.output.remove_subscriber(queue)
        async with self._shared_streams_lock:
            if shared.output.subscriber_count > 0:
                return
            if self._shared_streams.get(serial) is shared:
                del self._shared_streams[serial]
        await shared.close()

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


def _broadcast_put(queue: asyncio.Queue[bytes | None], data: bytes | None) -> None:
    """Hand one chunk to one subscriber queue, dropping it if that
    subscriber is too far behind rather than blocking the broadcast."""
    try:
        queue.put_nowait(data)
    except asyncio.QueueFull:
        pass


class _BroadcastOutput:
    """File-like bridge from the sync `copy_cloud_stream_to_mpegts` call
    (running in an executor thread) to however many async viewers are
    currently watching this serial.

    Subscribers can be added/removed from the event loop thread at any
    time while `write()` is being called concurrently from the executor
    thread - the subscriber set is protected accordingly.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._loop = hass.loop
        self._subscribers: set[asyncio.Queue[bytes | None]] = set()
        self._lock = threading.Lock()
        self._closed = threading.Event()

    def add_subscriber(self, queue: asyncio.Queue[bytes | None]) -> None:
        with self._lock:
            self._subscribers.add(queue)

    def remove_subscriber(self, queue: asyncio.Queue[bytes | None]) -> None:
        with self._lock:
            self._subscribers.discard(queue)

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def write(self, data: bytes) -> int:
        """Called from the executor thread with each remuxed chunk."""
        if self._closed.is_set():
            raise BrokenPipeError("EZVIZ stream view: no viewers left")
        with self._lock:
            queues = list(self._subscribers)
        for queue in queues:
            self._loop.call_soon_threadsafe(_broadcast_put, queue, data)
        return len(data)

    def flush(self) -> None:
        """No-op: each write() is already handed off immediately."""

    def close(self) -> None:
        """Signal every current subscriber's next get() to stop cleanly."""
        self._closed.set()
        with self._lock:
            queues = list(self._subscribers)
        for queue in queues:
            self._loop.call_soon_threadsafe(_broadcast_put, queue, None)


class _SharedStream:
    """One real EZVIZ VTM connection + keep-alive loop, fanned out to every
    concurrent viewer of one camera serial.

    Started by whichever viewer arrives first while none is running for
    that serial; reused by every viewer that arrives while it's active;
    torn down once the last one detaches. This makes it architecturally
    impossible for two real connections to the same serial to exist at
    once, which is what the device's single-session limit actually needs -
    a cleaner fix than the old "first one wins, the rest get a placeholder
    and give up" approach.
    """

    def __init__(self, hass: HomeAssistant, serial: str) -> None:
        self._hass = hass
        self.serial = serial
        self.output = _BroadcastOutput(hass)
        self._start_lock = asyncio.Lock()
        self._started = False
        self._copy_job: asyncio.Future[None] | None = None
        self._keepalive_stop = asyncio.Event()
        self._keepalive_task: asyncio.Task[None] | None = None

    async def ensure_started(self, pyez_client: Any) -> None:
        """Start the real EZVIZ connection if not already running.

        Safe to call from multiple concurrently-attaching viewers: only
        the first one to get here actually starts anything.
        """
        async with self._start_lock:
            if self._started:
                return
            self._started = True

            from .vendor.pyezvizapi.cloud_stream import copy_cloud_stream_to_mpegts
            from .vendor.pyezvizapi.exceptions import PyEzvizError

            def _copy() -> None:
                try:
                    copy_cloud_stream_to_mpegts(pyez_client, self.serial, self.output)
                except (PyEzvizError, OSError):
                    _LOGGER.exception("EZVIZ stream copy failed for %s", self.serial)
                finally:
                    self.output.close()

            self._copy_job = self._hass.async_add_executor_job(_copy)
            self._keepalive_task = self._hass.async_create_task(
                _keepalive_loop(self._hass, pyez_client, self.serial, self._keepalive_stop)
            )

    async def close(self) -> None:
        """Tear down the real connection. Only called once the last subscriber has left."""
        self.output.close()
        self._keepalive_stop.set()
        if self._copy_job is not None:
            await self._copy_job
        if self._keepalive_task is not None:
            await self._keepalive_task


def _client_disconnected(request: web.Request) -> bool:
    """Check the TCP transport directly instead of relying on `write()` to
    eventually raise.

    A `write()` to a socket whose peer already closed the connection can
    still succeed (the OS accepts it into the send buffer; the error only
    surfaces on a later write, sometimes much later). aiohttp's protocol
    keeps monitoring the read side of the connection independently of our
    writes, so `transport.is_closing()` notices a dropped client quickly
    even while we're only ever writing to it.
    """
    transport = request.transport
    return transport is None or transport.is_closing()


async def _stream_placeholder(
    response: web.StreamResponse,
    request: web.Request,
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
            if _client_disconnected(request):
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

        response = web.StreamResponse(
            status=200, headers={"Content-Type": "video/mp2t"}
        )
        await response.prepare(request)
        _LOGGER.debug("EZVIZ stream %s: response prepared at +%.2fs", serial, time.monotonic() - t0)

        # Every viewer sees the placeholder until real bytes reach ITS OWN
        # queue. For the first viewer of a serial that's the time it takes
        # to log in and connect; for a viewer joining a stream that's
        # already flowing, real data typically lands within one broadcast
        # cycle, so the placeholder is only visible for a moment.
        real_data_ready = asyncio.Event()
        placeholder_task = hass.async_create_task(
            _stream_placeholder(response, request, real_data_ready, max_duration=None)
        )

        try:
            shared, queue = await client.async_attach_stream(serial)
        except EzvizAuthError:
            _LOGGER.exception("EZVIZ stream %s: login failed", serial)
            real_data_ready.set()
            await placeholder_task
            return response

        _LOGGER.debug("EZVIZ stream %s: attached to shared stream at +%.2fs", serial, time.monotonic() - t0)

        first_chunk = True
        try:
            while True:
                # A dead viewer (e.g. one of go2rtc's own probe connections
                # that never sends a clean close) must not keep the shared
                # stream - and the real EZVIZ connection behind it - alive
                # forever. Poll the queue instead of awaiting it forever, so
                # we get a regular chance to check the transport directly.
                try:
                    chunk = await asyncio.wait_for(queue.get(), timeout=_DISCONNECT_POLL_SECONDS)
                except TimeoutError:
                    if _client_disconnected(request):
                        _LOGGER.debug(
                            "EZVIZ stream %s: transport closed while idle at +%.2fs",
                            serial,
                            time.monotonic() - t0,
                        )
                        break
                    continue
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
                    # current write and stop before we write anything else,
                    # so the two never interleave on `response`.
                    real_data_ready.set()
                    await placeholder_task
                await response.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            _LOGGER.debug(
                "EZVIZ stream %s: client disconnected at +%.2fs", serial, time.monotonic() - t0
            )
        finally:
            real_data_ready.set()
            if not placeholder_task.done():
                await placeholder_task
            await client.async_detach_stream(serial, shared, queue)
            _LOGGER.debug("EZVIZ stream %s: detached at +%.2fs", serial, time.monotonic() - t0)

        return response
