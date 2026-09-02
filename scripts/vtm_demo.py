"""Standalone demo: the same VTM stream + loading-placeholder logic used by
`EzvizStreamView` in the real integration (see
`custom_components/ezviz_stream/ezviz_client.py`), served as HTTP MPEG-TS on
localhost for testing with curl or VLC - without touching Home Assistant.

This is a plain reimplementation (threads + a sync HTTP server) of that
same placeholder-then-switch-over behavior, since the production code
imports Home Assistant modules that aren't available/needed here:

1. On connect, immediately start writing a looping placeholder video (a
   pre-rendered static image, looped with ffmpeg) to the client.
2. Concurrently start the real EZVIZ VTM connection + ffmpeg remux
   (`copy_cloud_stream_to_mpegts`) in a background thread.
3. The moment the first real byte arrives, stop the placeholder and switch
   to writing real bytes for the rest of the connection.

Usage (reads credentials from the environment so they never end up in shell
history or a process listing):

    export EZVIZ_ACCOUNT="you@example.com"
    export EZVIZ_PASSWORD="..."
    export EZVIZ_SERIAL="BE0776232"
    python scripts/vtm_demo.py

Then, in another terminal:

    curl -o test.ts http://127.0.0.1:8092/live.ts
    # or just open http://127.0.0.1:8092/live.ts directly in VLC
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Empty, Queue

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "custom_components" / "ezviz_stream")
)

from vendor.pyezvizapi.client import EzvizClient as PyEzvizClient  # noqa: E402
from vendor.pyezvizapi.cloud_stream import copy_cloud_stream_to_mpegts  # noqa: E402
from vendor.pyezvizapi.exceptions import PyEzvizError  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
_LOG = logging.getLogger("vtm_demo")

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8092

# Counter-intuitive finding: shortening this from 30s to 15s made things
# WORSE (159s total vs. 182s), with most calls failing on the first attempt
# and only succeeding after 1-2 retries. That looks like the server
# rejects a renewal made too early (plenty of granted time still
# remaining) and only accepts one once actually close to expiry - so a
# longer interval that happens to land near the real expiry does better
# than firing more often. Trying longer than 30s next.
KEEPALIVE_INTERVAL_SECONDS = 45
KEEPALIVE_MAX_RETRIES = 3
KEEPALIVE_RETRY_DELAY_SECONDS = 5

# Same placeholder as the real integration (see ezviz_client.py).
PLACEHOLDER_IMAGE = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "ezviz_stream"
    / "assets"
    / "placeholder.png"
)
PLACEHOLDER_FPS = "2"


def _login() -> PyEzvizClient:
    account = os.environ.get("EZVIZ_ACCOUNT")
    password = os.environ.get("EZVIZ_PASSWORD")
    if not account or not password:
        print("Set EZVIZ_ACCOUNT and EZVIZ_PASSWORD environment variables first.", file=sys.stderr)
        raise SystemExit(2)

    client = PyEzvizClient(account=account, password=password)
    _LOG.info("Logging in as %s ...", account)
    client.login()
    _LOG.info("Login OK.")
    return client


class _QueueOutput:
    """File-like bridge from the sync `copy_cloud_stream_to_mpegts` call
    (background thread) to the HTTP handler thread writing to the client."""

    def __init__(self) -> None:
        self.queue: Queue[bytes | None] = Queue(maxsize=64)
        self.first_byte_event = threading.Event()
        self._closed = threading.Event()

    def write(self, data: bytes) -> int:
        if self._closed.is_set():
            raise BrokenPipeError("vtm_demo: client disconnected")
        self.first_byte_event.set()
        self.queue.put(data)
        return len(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._closed.set()
        self.queue.put(None)


def _run_placeholder(stop: threading.Event, wfile) -> None:
    """Write a looping placeholder video to `wfile` until `stop` is set."""
    try:
        process = subprocess.Popen(
            [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-re",
                "-loop",
                "1",
                "-i",
                str(PLACEHOLDER_IMAGE),
                "-r",
                PLACEHOLDER_FPS,
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
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        _LOG.exception("Could not start placeholder ffmpeg")
        return

    try:
        assert process.stdout is not None
        while not stop.is_set():
            chunk = process.stdout.read(32768)
            if not chunk:
                break
            try:
                wfile.write(chunk)
            except (BrokenPipeError, OSError):
                return
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()


class Handler(BaseHTTPRequestHandler):
    pyez_client: PyEzvizClient = None  # set in main()
    serial: str = None  # set in main()

    def log_message(self, fmt, *args):  # noqa: A002
        _LOG.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/live.ts", "/"):
            self.send_response(404)
            self.end_headers()
            return

        t0 = time.monotonic()
        self.send_response(200)
        self.send_header("Content-Type", "video/mp2t")
        self.end_headers()

        output = _QueueOutput()

        def _copy() -> None:
            try:
                copy_cloud_stream_to_mpegts(Handler.pyez_client, Handler.serial, output)
            except (PyEzvizError, OSError):
                _LOG.exception("EZVIZ stream copy failed")
            finally:
                output.close()

        copy_thread = threading.Thread(target=_copy, daemon=True)
        copy_thread.start()

        # EXPERIMENTAL: the previous run's live view died after ~58s of real
        # playback with "Device offline or unreachable: timed out waiting
        # for VTM stream data" - suspiciously close to a common ~60s
        # battery-camera auto-timeout. The official app likely renews the
        # view periodically (e.g. via a "keep watching?" prompt); try the
        # same delay_battery_device_sleep() call we found earlier, this
        # time DURING an active stream rather than before opening one.
        keepalive_stop = threading.Event()

        def _keepalive() -> None:
            while not keepalive_stop.wait(timeout=KEEPALIVE_INTERVAL_SECONDS):
                for attempt in range(1, KEEPALIVE_MAX_RETRIES + 1):
                    try:
                        result = Handler.pyez_client.delay_battery_device_sleep(
                            Handler.serial, 1, 1
                        )
                        _LOG.info("Keep-alive delay_battery_device_sleep -> %s", result)
                        break
                    except Exception:
                        _LOG.exception(
                            "Keep-alive delay_battery_device_sleep failed (attempt %d/%d)",
                            attempt,
                            KEEPALIVE_MAX_RETRIES,
                        )
                        if attempt < KEEPALIVE_MAX_RETRIES and keepalive_stop.wait(
                            timeout=KEEPALIVE_RETRY_DELAY_SECONDS
                        ):
                            break

        keepalive_thread = threading.Thread(target=_keepalive, daemon=True)
        keepalive_thread.start()

        placeholder_stop = threading.Event()
        placeholder_thread = threading.Thread(
            target=_run_placeholder, args=(placeholder_stop, self.wfile), daemon=True
        )
        placeholder_thread.start()
        _LOG.info("Placeholder started at +%.2fs, waiting for real data ...", time.monotonic() - t0)

        # Wait for the first real chunk (or the copy thread giving up), then
        # stop the placeholder before writing any real bytes, so the two
        # never interleave on the socket.
        while not output.first_byte_event.is_set() and copy_thread.is_alive():
            output.first_byte_event.wait(timeout=0.2)
        placeholder_stop.set()
        placeholder_thread.join(timeout=5.0)
        _LOG.info("Placeholder stopped at +%.2fs, switching to real stream", time.monotonic() - t0)

        first_real_chunk = True
        try:
            while True:
                try:
                    chunk = output.queue.get(timeout=1.0)
                except Empty:
                    if not copy_thread.is_alive():
                        break
                    continue
                if chunk is None:
                    break
                if first_real_chunk:
                    _LOG.info(
                        "First real chunk (%d bytes) at +%.2fs", len(chunk), time.monotonic() - t0
                    )
                    first_real_chunk = False
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            output.close()
            copy_thread.join(timeout=5.0)
            keepalive_stop.set()
            keepalive_thread.join(timeout=5.0)
            _LOG.info("Client disconnected / stream ended at +%.2fs", time.monotonic() - t0)


def main() -> int:
    pyez_client = _login()
    serial = os.environ.get("EZVIZ_SERIAL")
    if not serial:
        print("Set EZVIZ_SERIAL environment variable.", file=sys.stderr)
        return 2

    Handler.pyez_client = pyez_client
    Handler.serial = serial

    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    url = f"http://{LISTEN_HOST}:{LISTEN_PORT}/live.ts"
    _LOG.info("Serving on %s - open it in VLC or run:", url)
    _LOG.info("  curl -o test.ts %s", url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
