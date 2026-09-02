"""Standalone demo: connect to a device's EZVIZ cloud P2P stream and serve it
as HTTP MPEG-TS on localhost, for testing with curl or VLC.

NOT USED by the Home Assistant integration - see README.md in this folder
for why. Kept only as a reference/for a possible future device that
actually needs this path.

This deliberately does not touch Home Assistant at all - it is a plain
script exercising this `p2p/` package and the integration's vendored
`pyezvizapi` login.

Video only for this first pass (H.265 Annex-B remuxed to MPEG-TS via
ffmpeg); audio (PCMA) frames are read but not muxed in yet.

Usage (reads credentials from the environment so they never end up in shell
history or a process listing):

    export EZVIZ_ACCOUNT="you@example.com"
    export EZVIZ_PASSWORD="..."
    export EZVIZ_SERIAL="BE0776232"
    python p2p/demo.py

Then, in another terminal:

    curl -o test.ts http://127.0.0.1:8091/live.ts
    # or just open http://127.0.0.1:8091/live.ts directly in VLC
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "custom_components" / "ezviz_stream")
)

from p2p import client as p2p_client  # noqa: E402
from p2p.session import CODEC_H265, Session  # noqa: E402
from vendor.pyezvizapi.client import EzvizClient as PyEzvizClient  # noqa: E402

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
_LOG = logging.getLogger("p2p_demo")

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8091


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


def _pump_video_to_ffmpeg(session: Session, ffmpeg_stdin, stop: threading.Event) -> None:
    """Read H.265 frames from the P2P session and write them to ffmpeg's
    stdin until `stop` is set or the session ends."""
    try:
        while not stop.is_set():
            frame = session.read_frame(timeout=1.0)
            if frame is None:
                continue
            if frame.codec != CODEC_H265:
                continue
            try:
                ffmpeg_stdin.write(frame.payload)
                ffmpeg_stdin.flush()
            except (BrokenPipeError, OSError):
                return
    finally:
        try:
            ffmpeg_stdin.close()
        except OSError:
            pass


class Handler(BaseHTTPRequestHandler):
    session_factory = None  # set in main()

    def log_message(self, fmt, *args):  # noqa: A002
        _LOG.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/live.ts", "/"):
            self.send_response(404)
            self.end_headers()
            return

        _LOG.info("Client connected, starting P2P session ...")
        try:
            session = Handler.session_factory()
        except Exception:
            _LOG.exception("Failed to start P2P session")
            self.send_response(502)
            self.end_headers()
            return

        ffmpeg = subprocess.Popen(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-fflags",
                "nobuffer",
                "-f",
                "hevc",
                "-i",
                "pipe:0",
                "-c:v",
                "copy",
                "-f",
                "mpegts",
                "-muxdelay",
                "0",
                "pipe:1",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
        )

        stop = threading.Event()
        pump = threading.Thread(
            target=_pump_video_to_ffmpeg, args=(session, ffmpeg.stdin, stop), daemon=True
        )
        pump.start()

        self.send_response(200)
        self.send_header("Content-Type", "video/mp2t")
        self.end_headers()

        try:
            assert ffmpeg.stdout is not None
            while True:
                chunk = ffmpeg.stdout.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            _LOG.info("Client disconnected, tearing down P2P session")
            stop.set()
            session.close()
            if ffmpeg.poll() is None:
                ffmpeg.kill()


def main() -> int:
    pyez_client = _login()
    serial = os.environ.get("EZVIZ_SERIAL")
    if not serial:
        print("Set EZVIZ_SERIAL environment variable.", file=sys.stderr)
        return 2

    wake_sleep_type_env = os.environ.get("EZVIZ_WAKE_SLEEP_TYPE", "0")
    wake_sleep_type = None if wake_sleep_type_env.lower() == "none" else int(wake_sleep_type_env)

    def _start_session() -> Session:
        _LOG.info("Starting P2P session for %s (wake_sleep_type=%s) ...", serial, wake_sleep_type)
        sess = p2p_client.connect(pyez_client, serial, wake_sleep_type=wake_sleep_type)
        _LOG.info("P2P session up, SRT data flowing.")
        return sess

    Handler.session_factory = _start_session

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
