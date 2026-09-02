"""EZVIZ/Hik-Connect cloud P2P session: setup, hole-punch, SRT dialect, media.

Ported from `session.go` in
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(fork of AlexxIT/go2rtc, MIT licensed). See that project's `PROTOCOL.md` for
the full wire-format and transport-mix specification this implements.

Flow:

1. P2P_SETUP (0x0B02) to each P2P server registers our NAT-mapped address.
2. The 0x0B03 response carries the device's stream port; we pre-punch to it.
3. The device sends a hole-punch request (0x0C00); we reply 0x0C01 ten times.
4. PLAY_REQUEST (0x0C02) is sent directly to the device and relayed through
   the P2P server inside a TRANSFOR_DATA (0x0B04) wrapper.
5. The device opens an SRT connection (induction -> conclusion); once up it
   streams media data packets that we ACK and reassemble into NAL units.

Concurrency: ported from Go goroutines to plain threads with a blocking UDP
socket (a short read timeout keeps the receive loop responsive to shutdown).
A background thread reads datagrams and dispatches them; completed Annex-B
NAL units are pushed onto a bounded queue that `read_frame` drains. A
separate ticker thread emits SRT ACKs and keepalives.
"""

from __future__ import annotations

import base64
import logging
import queue
import random
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field

from . import v3
from .api import P2PServer
from .hikrtp import HikRTPExtractor, extract_audio_payload

_LOG = logging.getLogger(__name__)

# P2P server error codes seen in P2P_SETUP/TRANSFOR_CTRL rejections (see
# PROTOCOL.md "Error codes"). Not authoritatively tied to one specific
# attribute tag - logged wherever an integer status-shaped attribute is
# seen alongside a response, to help diagnose a rejected setup.
_KNOWN_ERROR_CODES = {
    0x101011: "device offline",
    0x101012: "device unavailable for P2P (server-side, before any punch)",
    0x0E48: "key-info mismatch",
    0x0E16: "decrypt with empty key",
    0x0E4C: "P2P-server decrypt failure",
}

# Non-V3 UDP packet types (big-endian at offset 0). The high bit marks an SRT
# control packet; data packets clear it.
PKT_SESSION_SETUP = 0x7534
PKT_CONN_CONTROL = 0x8000
PKT_KEEPALIVE = 0x8001
PKT_DATA_ACK = 0x8002

# SRT control subtypes (F=1, the 0x80xx family).
SRT_CTRL_HANDSHAKE = 0x8000
SRT_CTRL_KEEPALIVE = 0x8001
SRT_CTRL_ACK = 0x8002
SRT_CTRL_NAK = 0x8003
SRT_CTRL_SHUTDOWN = 0x8005
SRT_CTRL_ACK2 = 0x8006

# Hik-RTP inner type carried by SRT control sub-session keepalives.
INNER_CONTROL_KEEPALIVE = 0x807F

SRT_REORDER_FLUSH = 0.1  # 100ms
SRT_REORDER_MAX_AHEAD = 64  # give up on a gap once the buffer runs this far ahead

CODEC_H265 = "h265"
CODEC_PCMA = "pcma"


class SessionError(Exception):
    """Raised when the P2P session fails to come up."""


@dataclass
class Frame:
    codec: str
    payload: bytes
    timestamp: int
    frame_no: int


@dataclass
class SessionConfig:
    """Everything the session needs to reach and authenticate to a device.

    Assembled from the REST login + P2P config/secret responses (see
    `api.py`).
    """

    device_serial: str
    device_public_ip: str
    device_public_port: int
    p2p_servers: list[P2PServer]
    p2p_key: bytes  # 32-byte rotating P2P server key (outer encryption)
    p2p_link_key: bytes  # 32-byte link key (inner PLAY_REQUEST encryption)
    p2p_key_version: int
    p2p_key_salt_index: int
    p2p_key_salt_ver: int
    user_id: str
    client_id: int
    channel_no: int
    stream_type: int = 1  # 1=main, 2=sub
    bus_type: int = 1  # 1=live preview, 2=playback


def _append_tlv(buf: bytearray, tag: int, value: bytes) -> None:
    """Append one 1-byte-length TLV attribute (the local hand-built bodies in
    this module always use a single length byte, unlike the general TLV
    codec in v3.py which supports a 2-byte length for tag 0x07)."""
    buf.append(tag)
    buf.append(len(value))
    buf += value


def _timestamp32() -> int:
    return int(time.time() * 1000) & 0xFFFFFFFF


def _rand_uint32() -> int:
    return random.getrandbits(32)


def random_client_id() -> int:
    """A fresh non-zero client id for the PLAY_REQUEST expand header. The
    device does not validate it - it is a client-side correlation id."""
    while True:
        v = _rand_uint32()
        if v != 0:
            return v


def _frame_v3(opcode: int, seq: int, mask: v3.Mask, body: bytes) -> bytes:
    """Assemble a V3 header around an already-built (possibly pre-encrypted)
    body and stamp the CRC. Lower-level than v3.encode_message: used where
    the body is hand-built TLV bytes rather than a v3.Attr list."""
    full = bytearray(v3.HEADER_LEN + len(body))
    full[0] = v3.MAGIC
    full[1] = mask.encode()
    struct.pack_into(">H", full, 2, opcode)
    struct.pack_into(">I", full, 4, seq)
    struct.pack_into(">H", full, 8, 0x6234)
    full[10] = v3.HEADER_LEN
    full[11] = 0x00
    full[v3.HEADER_LEN :] = body
    full[11] = v3.crc8(bytes(full))
    return bytes(full)


class Session:
    """Holds the live UDP transport state for one P2P streaming session."""

    def __init__(self, cfg: SessionConfig) -> None:
        self.cfg = cfg
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0))
        self.sock.settimeout(0.5)

        self._lock = threading.RLock()

        self.seq_num = 0
        self.session_counter = 0
        self.source_id = _rand_uint32()
        self.data_session_id = 0

        self.device_peer: tuple[str, int] | None = None
        self.device_stream_port = 0
        self.punch_complete = False

        self.current_session_key = ""

        self.srt_syn_cookie = 0
        self.srt_peer_socket_id = 0
        self.srt_ack_number = 1
        self.last_ack_seq = 0

        # Video reorder buffer state (video sub-session sequence space).
        self.srt_deliver_seq = -1
        self.reorder_buf: dict[int, bytes] = {}
        self._flush_timer: threading.Timer | None = None

        self.extractor = HikRTPExtractor()
        self._feed_lock = threading.Lock()
        self.frame_no = 0

        self.audio_frame_no = 0
        self.audio_ts = 0
        self._epoch: float | None = None

        self.frames: queue.Queue[Frame | None] = queue.Queue(maxsize=256)
        self._punch_event = threading.Event()
        self._data_event = threading.Event()
        self._close_event = threading.Event()

        self._threads: list[threading.Thread] = []

    # -- lifecycle --

    def start(self) -> None:
        """Run the full bring-up sequence; media then arrives asynchronously
        on the frame queue."""
        t = threading.Thread(target=self._receive_loop, daemon=True, name="ezviz-p2p-recv")
        t.start()
        self._threads.append(t)

        self._contact_p2p_servers()
        self._send_play_request()

        t2 = threading.Thread(target=self._ticker_loop, daemon=True, name="ezviz-p2p-ticker")
        t2.start()
        self._threads.append(t2)

        self._wait_for_data_session(15.0)
        if self._get_data_session_id() == 0:
            raise SessionError(
                "SRT data session did not start within 15s (device offline, "
                f"blocked UDP/NAT, or channel {self.cfg.channel_no} not streaming)"
            )

        self._send_session_setup()

    def read_frame(self, timeout: float | None = None) -> Frame | None:
        """Return the next demuxed access unit, or None on stream end."""
        try:
            return self.frames.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self) -> None:
        self._send_teardown()
        self._close_event.set()
        with self._lock:
            if self._flush_timer is not None:
                self._flush_timer.cancel()
                self._flush_timer = None
        try:
            self.sock.close()
        except OSError:
            pass
        for t in self._threads:
            t.join(timeout=2.0)
        # Unblock a pending read_frame().
        try:
            self.frames.put_nowait(None)
        except queue.Full:
            pass

    # -- setup --

    def _contact_p2p_servers(self) -> None:
        setup = self._build_p2p_setup_request()
        _LOG.info(
            "P2P_SETUP -> %s (session_key=%s)",
            [(s.ip, s.port) for s in self.cfg.p2p_servers],
            self.current_session_key,
        )
        for srv in self.cfg.p2p_servers:
            self._send_to(setup, srv.ip, srv.port)

        # Wait for the device hole-punch (0x0C00 -> 0x0C01, handled in the
        # receive loop).
        if self._punch_event.wait(10.0):
            with self._lock:
                _LOG.info("Hole-punch complete, device peer=%s", self.device_peer)
            return

        _LOG.warning(
            "No hole-punch request received from the device within 10s "
            "(no P2P_SETUP response either, or it didn't carry a stream port) "
            "- falling back to punching the known device address directly"
        )
        self._hole_punch()
        self._close_event.wait(2.0)

    def _send_play_request(self) -> None:
        # Path A: directly to the device over the punched connection.
        with self._lock:
            peer = self.device_peer
            punched = self.punch_complete

        if punched and peer is not None:
            direct = self._build_inner_v3_message(self._build_play_request_body(), v3.OP_PLAY_REQUEST)
            for _ in range(3):
                self._send_to_addr(direct, peer)
            _LOG.info("PLAY_REQUEST sent directly to device peer %s", peer)
        else:
            _LOG.info("PLAY_REQUEST: no punched peer yet, skipping direct path (relay only)")

        # Path B: relayed through the P2P server inside a TRANSFOR_DATA wrapper.
        relay = self._build_p2p_server_request()
        for srv in self.cfg.p2p_servers:
            self._send_to(relay, srv.ip, srv.port)
        _LOG.info("PLAY_REQUEST relayed via P2P server(s)")

        self._close_event.wait(3.0)

    # -- V3 message builders --

    def _next_seq(self) -> int:
        with self._lock:
            self.seq_num += 1
            return self.seq_num

    def _session_key(self) -> str:
        """base64(serial) + channel + YYYYMMDDhhmmss + 5 random digits - the
        per-session correlation key the device echoes back."""
        b64 = base64.b64encode(self.cfg.device_serial.encode()).decode()
        now = time.localtime()
        ts = time.strftime("%Y%m%d%H%M%S", now)
        rand5 = str(random.randint(10000, 99999))
        return b64 + str(self.cfg.channel_no) + ts + rand5

    def _bus_type(self) -> int:
        return self.cfg.bus_type or 1

    def _build_p2p_setup_request(self) -> bytes:
        """P2P_SETUP (0x0B02): encrypted with the P2P server key, no expand
        header. Body carries the session key, user id, serial and a nested
        sub-TLV (tag 0xFF) advertising our bus type, local address and
        client id."""
        serial = self.cfg.device_serial
        key = self._session_key()
        with self._lock:
            self.current_session_key = key

        local_ip, local_port = self._local_addr()

        body = bytearray()
        _append_tlv(body, v3.ATTR_SESSION_KEY, key.encode())
        _append_tlv(body, v3.ATTR_SESSION_INFO, self.cfg.user_id.encode())
        _append_tlv(body, v3.ATTR_TRANSFOR_DATA, serial.encode())
        _append_tlv(body, v3.ATTR_BUS_TYPE_ENC, bytes([0x03]))  # protocol version 3

        transfor = bytearray()
        _append_tlv(transfor, 0x71, bytes([self._bus_type()]))
        _append_tlv(transfor, 0x72, bytes([0x03]))
        _append_tlv(transfor, 0x75, bytes([0x01]))
        _append_tlv(transfor, 0x7F, bytes([0x0A]))
        _append_tlv(transfor, 0x74, f"{local_ip}:{local_port}".encode())
        _append_tlv(transfor, 0x8C, struct.pack(">I", self.cfg.client_id))
        _append_tlv(body, v3.ATTR_END_MARKER, bytes(transfor))

        enc = v3.aes_encrypt(bytes(body), self.cfg.p2p_key)
        mask = v3.Mask(
            encrypt=True,
            is_2b_len=True,
            salt_version=self.cfg.p2p_key_salt_ver,
            salt_index=self.cfg.p2p_key_salt_index,
        )
        return _frame_v3(v3.OP_P2P_SETUP, 0, mask, enc)

    def _build_p2p_server_request(self) -> bytes:
        """Wrap an inner PLAY_REQUEST V3 message inside an AES-encrypted
        TRANSFOR_DATA (0x0B04) message addressed to the P2P server, which
        relays it to the device."""
        inner = self._build_inner_v3_message(self._build_play_request_body(), v3.OP_PLAY_REQUEST)
        outer_body = self._build_outer_body(inner)

        enc = v3.aes_encrypt(outer_body, self.cfg.p2p_key)
        mask = v3.Mask(
            encrypt=True,
            is_2b_len=True,
            salt_version=self.cfg.p2p_key_salt_ver,
            salt_index=self.cfg.p2p_key_salt_index,
        )
        return _frame_v3(v3.OP_TRANSFOR_DATA, self._next_seq(), mask, enc)

    def _build_play_request_body(self) -> bytes:
        """The (still plaintext) TLV body of PLAY_REQUEST."""
        serial = self.cfg.device_serial
        with self._lock:
            key = self.current_session_key
            counter = self.session_counter

        now = time.localtime()
        today = time.strftime("%Y-%m-%d", now)
        start = f"{today}T00:00:00"
        stop = f"{today}T{time.strftime('%H:%M:%S', now)}"

        body = bytearray()
        _append_tlv(body, v3.ATTR_BUS_TYPE, bytes([self._bus_type()]))
        _append_tlv(body, v3.ATTR_SESSION_KEY, key.encode())
        _append_tlv(body, v3.ATTR_STREAM_TYPE, bytes([self.cfg.stream_type]))
        _append_tlv(body, v3.ATTR_CHANNEL_NO, struct.pack(">H", self.cfg.channel_no))
        _append_tlv(body, v3.ATTR_STREAM_SESSION, struct.pack(">I", (counter + 1) & 0xFFFFFFFF))
        _append_tlv(body, v3.ATTR_DEVICE_SESSION_ALT, struct.pack(">I", 180))
        _append_tlv(body, v3.ATTR_START_TIME, start.encode())
        _append_tlv(body, v3.ATTR_STOP_TIME, stop.encode())
        _append_tlv(body, v3.ATTR_STREAM_META, serial.encode())
        _append_tlv(body, v3.ATTR_OPT_META1, str(uuid.uuid4()).encode())
        _append_tlv(body, v3.ATTR_OPT_META2, str(int(time.time() * 1000)).encode())
        return bytes(body)

    def _build_inner_v3_message(self, body: bytes, opcode: int) -> bytes:
        """Encrypt a body with the link key and wrap it in a V3 message
        carrying the expand header (key version, user id, client id,
        channel)."""
        enc = v3.aes_encrypt(body, self.cfg.p2p_link_key)

        expand = bytearray()
        _append_tlv(expand, v3.ATTR_TRANSFOR_DATA, struct.pack(">H", self.cfg.p2p_key_version))
        _append_tlv(expand, v3.ATTR_EXPAND_KEY_VERSION, self.cfg.user_id.encode())
        _append_tlv(expand, v3.ATTR_CLIENT_ID, struct.pack(">I", self.cfg.client_id))
        _append_tlv(expand, v3.ATTR_DEVICE_CHANNEL, struct.pack(">H", self.cfg.channel_no))

        header_len = v3.HEADER_LEN + len(expand)
        mask = v3.Mask(
            encrypt=True,
            is_2b_len=True,
            expand_header=True,
            salt_version=self.cfg.p2p_key_salt_ver,
            salt_index=self.cfg.p2p_key_salt_index,
        )

        full = bytearray(header_len + len(enc))
        full[0] = v3.MAGIC
        full[1] = mask.encode()
        struct.pack_into(">H", full, 2, opcode)
        struct.pack_into(">I", full, 4, self._next_seq())
        struct.pack_into(">H", full, 8, 0x6234)
        full[10] = header_len & 0xFF
        full[11] = 0x00
        full[v3.HEADER_LEN : header_len] = expand
        full[header_len:] = enc
        full[11] = v3.crc8(bytes(full))
        return bytes(full)

    def _build_outer_body(self, inner: bytes) -> bytes:
        """TRANSFOR_DATA outer body: a routing serial tag plus the inner V3
        message carried under tag 0x07 (2-byte length)."""
        body = bytearray()
        _append_tlv(body, v3.ATTR_TRANSFOR_DATA, self.cfg.device_serial.encode())
        body.append(v3.ATTR_LARGE_DATA)
        body += struct.pack(">H", len(inner))
        body += inner
        return bytes(body)

    # -- hole punching --

    def _hole_punch(self) -> None:
        punch = bytes([0x00])
        with self._lock:
            ports = {self.cfg.device_public_port}
            if self.device_stream_port:
                ports.add(self.device_stream_port)
        for port in ports:
            for _ in range(5):
                self._send_to(punch, self.cfg.device_public_ip, port)

    # -- session setup (0x7534) --

    def _send_session_setup(self) -> None:
        key = self._session_key()
        pkt_v3 = v3.encode_message(
            v3.Message(
                msg_type=0x0C00,
                seq_num=self._next_seq(),
                reserved=0x6234,
                mask=v3.Mask(
                    is_2b_len=True,
                    salt_version=self.cfg.p2p_key_salt_ver,
                    salt_index=self.cfg.p2p_key_salt_index,
                ),
                attrs=[
                    v3.Attr(tag=v3.ATTR_SESSION_KEY, value=key.encode()),
                    v3.Attr(tag=0x71, value=bytes([0x01])),
                    v3.Attr(tag=v3.ATTR_PORT_COUNT, value=bytes(4)),
                ],
            )
        )

        with self._lock:
            self.session_counter += 1
            session_id = self.session_counter
            seq = self.seq_num
            source = self.source_id

        pkt = bytearray(28 + len(pkt_v3))
        struct.pack_into(">H", pkt, 0, PKT_SESSION_SETUP)
        struct.pack_into(">H", pkt, 2, session_id & 0xFFFF)
        struct.pack_into(">H", pkt, 4, 0xC000)  # SYN flags
        struct.pack_into(">H", pkt, 6, seq & 0xFFFF)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, source)
        pkt[16] = 0x80
        pkt[17] = 0x7F
        pkt[28:] = pkt_v3
        pkt = bytes(pkt)

        self._send_to_device(pkt)
        threading.Timer(0.2, self._send_to_device, args=(pkt,)).start()
        threading.Timer(0.5, self._send_to_device, args=(pkt,)).start()

    # -- receive loop --

    def _receive_loop(self) -> None:
        while not self._close_event.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except TimeoutError:
                continue
            except OSError:
                self._close_frames()
                return
            self._handle_packet(data, addr)

    def _handle_packet(self, buf: bytes, src: tuple[str, int]) -> None:
        if len(buf) < 2:
            return

        # V3 messages from P2P servers (magic high nibble 0xE).
        if buf[0] >> 4 == 0xE and len(buf) >= v3.HEADER_LEN:
            self._handle_v3_response(buf, src)
            return

        t = struct.unpack(">H", buf[0:2])[0]

        if t == SRT_CTRL_KEEPALIVE:
            resp = bytearray(16)
            struct.pack_into(">H", resp, 0, SRT_CTRL_KEEPALIVE)
            struct.pack_into(">I", resp, 8, _timestamp32())
            struct.pack_into(">I", resp, 12, self._get_peer_socket_id())
            self._send_to_device(bytes(resp))
            return
        if t in (SRT_CTRL_ACK, SRT_CTRL_SHUTDOWN, SRT_CTRL_ACK2, SRT_CTRL_NAK):
            return
        if t == PKT_SESSION_SETUP:
            self._handle_session_setup(buf)
            return
        if t == PKT_CONN_CONTROL:
            self._handle_connection_control(buf)
            return

        # SRT data packet: control bit clear, has a 16-byte header, session up.
        if len(buf) >= 16 and buf[0] & 0x80 == 0 and self._get_data_session_id() != 0:
            self._handle_srt_data_packet(buf)
            return

        _LOG.debug(
            "Unhandled raw packet type=0x%04x len=%d from %s: %s",
            t,
            len(buf),
            src,
            buf[:32].hex(),
        )

    def _handle_v3_response(self, buf: bytes, src: tuple[str, int]) -> None:
        is_encrypted = bool(buf[1] & 0x80)
        try:
            msg = v3.decode_message(buf, self.cfg.p2p_key if is_encrypted else None)
        except v3.V3ProtocolError:
            _LOG.warning(
                "V3 message from %s failed to decode (encrypted=%s, %d bytes): %s",
                src,
                is_encrypted,
                len(buf),
                buf.hex(),
            )
            return

        _LOG.info(
            "V3 message from %s: type=0x%04x seq=%d attrs=%s",
            src,
            msg.msg_type,
            msg.seq_num,
            [(hex(a.tag), a.value[:64]) for a in msg.attrs],
        )
        for a in msg.attrs:
            code = v3.get_int_attr([a], a.tag)
            if code in _KNOWN_ERROR_CODES:
                _LOG.warning(
                    "V3 message from %s carries a known error code 0x%x (tag 0x%02x): %s",
                    src,
                    code,
                    a.tag,
                    _KNOWN_ERROR_CODES[code],
                )

        if msg.msg_type == v3.OP_TRANSFOR_CTRL:  # 0x0b03 - P2P_SETUP response
            self._handle_setup_response(msg)
        elif msg.msg_type == v3.OP_PUNCH_REQUEST:  # 0x0c00 - device hole-punch request
            self._handle_punch_request(src)
        # OP_PUNCH_RESPONSE (0x0c01): nothing to do.

    def _handle_setup_response(self, msg: v3.Message) -> None:
        """Extract the device stream port from the 0x0B03 response's nested
        tag 0xFF sub-TLV (0x74 = "IP:PORT") and pre-punch to it so the
        device's 0x0C00 can traverse our NAT."""
        ff = b""
        for a in msg.attrs:
            if a.tag == v3.ATTR_END_MARKER:
                ff = a.value
                break
        if not ff:
            _LOG.warning(
                "P2P_SETUP response carried no tag 0xFF sub-TLV (no device stream "
                "port) - the server may have rejected setup; check the attrs logged above"
            )
            return

        found_port = False
        off = 0
        while off + 2 <= len(ff):
            tag = ff[off]
            length = ff[off + 1]
            if off + 2 + length > len(ff):
                break
            if tag == 0x74:
                addr = ff[off + 2 : off + 2 + length].decode(errors="replace")
                if ":" in addr:
                    host, _, port_s = addr.rpartition(":")
                    try:
                        port = int(port_s)
                    except ValueError:
                        port = 0
                    if 0 < port < 65536:
                        found_port = True
                        _LOG.info("Device stream port from P2P_SETUP response: %s:%d", host, port)
                        with self._lock:
                            self.device_stream_port = port
                        punch = bytes([0x00])
                        for _ in range(5):
                            self._send_to(punch, self.cfg.device_public_ip, port)
            off += 2 + length

        if not found_port:
            _LOG.warning("P2P_SETUP response's tag 0xFF sub-TLV had no usable tag 0x74 (IP:PORT)")

    def _handle_punch_request(self, src: tuple[str, int]) -> None:
        _LOG.info("Hole-punch request (0x0C00) received from device at %s", src)
        with self._lock:
            self.device_peer = src
            key = self.current_session_key
            already = self.punch_complete
            self.punch_complete = True

        resp = v3.encode_message(
            v3.Message(
                msg_type=v3.OP_PUNCH_RESPONSE,
                seq_num=self._next_seq(),
                reserved=0x6234,
                mask=v3.Mask(
                    is_2b_len=True,
                    salt_version=self.cfg.p2p_key_salt_ver,
                    salt_index=self.cfg.p2p_key_salt_index,
                ),
                attrs=[
                    v3.Attr(tag=v3.ATTR_SESSION_KEY, value=key.encode()),
                    v3.Attr(tag=0x71, value=bytes([0x01])),
                ],
            )
        )
        for _ in range(10):
            self._send_to_addr(resp, src)

        if not already:
            self._punch_event.set()

    def _handle_session_setup(self, buf: bytes) -> None:
        if len(buf) < 28:
            return
        # Bytes 0-1 are the packet type (0x7534); the session id is the
        # 16-bit field at bytes 2-3, mirroring the layout _send_session_setup
        # writes. Echo that id so the device matches the ACK to its setup
        # exchange.
        session_id = struct.unpack(">H", buf[2:4])[0]
        self._send_data_ack(session_id)

    def _send_data_ack(self, acked_session_id: int) -> None:
        pkt = bytearray(44)
        struct.pack_into(">H", pkt, 0, PKT_DATA_ACK)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, self.source_id)
        struct.pack_into(">I", pkt, 16, acked_session_id)
        struct.pack_into(">I", pkt, 20, 0x3A0C)
        struct.pack_into(">I", pkt, 24, 0)
        struct.pack_into(">I", pkt, 28, 0x1E)
        struct.pack_into(">I", pkt, 32, 1)
        struct.pack_into(">I", pkt, 36, 0x3E8)
        struct.pack_into(">I", pkt, 40, 0x38)
        self._send_to_device(bytes(pkt))

    # -- SRT handshake --

    def _handle_connection_control(self, buf: bytes) -> None:
        if len(buf) < 64:
            _LOG.debug("Connection-control packet too short (%d bytes), ignoring", len(buf))
            return
        srt_version = struct.unpack(">I", buf[16:20])[0]
        init_seq = struct.unpack(">I", buf[24:28])[0]
        mtu = struct.unpack(">I", buf[28:32])[0]
        window = struct.unpack(">I", buf[32:36])[0]
        hs_type = struct.unpack(">I", buf[36:40])[0]
        peer_socket_id = struct.unpack(">I", buf[40:44])[0]
        _LOG.info(
            "Connection-control packet: srt_version=%d init_seq=%d hs_type=0x%x peer_socket=%d",
            srt_version,
            init_seq,
            hs_type,
            peer_socket_id,
        )

        if hs_type == 1 and srt_version == 4:
            self._handle_srt_induction(peer_socket_id, init_seq, mtu, window)
            return
        if hs_type == 0xFFFFFFFF:
            self._handle_srt_conclusion(buf, peer_socket_id)
            return
        if init_seq != 0 and self._get_data_session_id() == 0:
            self._set_data_session(init_seq)

    def _handle_srt_induction(self, peer_socket_id: int, init_seq: int, mtu: int, window: int) -> None:
        _LOG.info("SRT induction received (peer_socket=%d), replying with version 5", peer_socket_id)
        cookie = (self.source_id ^ peer_socket_id ^ _timestamp32()) & 0xFFFFFFFF

        pkt = bytearray(64)
        struct.pack_into(">H", pkt, 0, SRT_CTRL_HANDSHAKE)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, peer_socket_id)
        struct.pack_into(">I", pkt, 16, 5)  # version 5 (SRT)
        struct.pack_into(">H", pkt, 20, 0)  # encryption: none
        struct.pack_into(">H", pkt, 22, 0x4A17)  # SRT magic
        struct.pack_into(">I", pkt, 24, init_seq)
        struct.pack_into(">I", pkt, 28, mtu)
        struct.pack_into(">I", pkt, 32, window)
        struct.pack_into(">I", pkt, 36, 1)  # induction response
        struct.pack_into(">I", pkt, 40, self.source_id)
        struct.pack_into(">I", pkt, 44, cookie)

        with self._lock:
            self.srt_syn_cookie = cookie
            # The device opens two SRT sub-sessions: control then video. We
            # deliberately keep the last peer socket id so ACKs target the
            # video sub-session.
            self.srt_peer_socket_id = peer_socket_id

        self._send_to_device(bytes(pkt))

    def _handle_srt_conclusion(self, buf: bytes, peer_socket_id: int) -> None:
        init_seq = struct.unpack(">I", buf[24:28])[0]
        _LOG.info("SRT conclusion received (peer_socket=%d, init_seq=%d)", peer_socket_id, init_seq)
        if self._get_data_session_id() == 0:
            self._set_data_session(init_seq)

        with self._lock:
            cookie = self.srt_syn_cookie

        pkt = bytearray(80)
        struct.pack_into(">H", pkt, 0, SRT_CTRL_HANDSHAKE)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, peer_socket_id)
        struct.pack_into(">I", pkt, 16, 5)  # version 5 (SRT)
        struct.pack_into(">H", pkt, 20, 0)  # encryption: none
        struct.pack_into(">H", pkt, 22, 1)  # extensions present
        struct.pack_into(">I", pkt, 24, init_seq)
        struct.pack_into(">I", pkt, 28, 1500)  # MTU
        struct.pack_into(">I", pkt, 32, 32)  # window
        struct.pack_into(">I", pkt, 36, 0xFFFFFFFF)  # conclusion
        struct.pack_into(">I", pkt, 40, self.source_id)
        struct.pack_into(">I", pkt, 44, cookie)
        # SRT_CMD_HSRSP extension.
        struct.pack_into(">H", pkt, 64, 2)  # extension type HSRSP
        struct.pack_into(">H", pkt, 66, 3)  # length in 32-bit words
        struct.pack_into(">I", pkt, 68, 0x00010401)  # SRT version 1.4.1
        struct.pack_into(">I", pkt, 72, 0x000000B4)  # flags

        self._send_to_device(bytes(pkt))

    # -- SRT data path --

    def _handle_srt_data_packet(self, buf: bytes) -> None:
        seq_num = struct.unpack(">I", buf[0:4])[0] & 0x7FFFFFFF
        payload = buf[16:]

        # The device multiplexes two SRT sub-sessions onto this one socket,
        # each with its OWN sequence space: a control channel carrying
        # 0x807f keepalives and the video data channel. Our ACKs go to the
        # video socket and must carry the video channel's sequence - letting
        # a control keepalive's sequence advance last_ack_seq would ACK the
        # video session with an out-of-range sequence and stall the
        # device's flow-control window. Route by inner Hik-RTP type and
        # never mix the spaces.
        inner_type = struct.unpack(">H", payload[0:2])[0] if len(payload) >= 2 else 0
        if inner_type == INNER_CONTROL_KEEPALIVE:
            return

        with self._lock:
            self.last_ack_seq = seq_num

        self._deliver_in_order(seq_num, payload)

    def _srt_ahead(self, seq: int) -> int:
        """Wrap-aware signed distance of seq ahead of the next seq expected
        for delivery. Caller holds self._lock."""
        d = (seq - self.srt_deliver_seq) & 0x7FFFFFFF
        if d > 0x40000000:
            d -= 0x80000000
        return d

    def _deliver_in_order(self, seq: int, payload: bytes) -> None:
        """Re-sequence video payloads before NAL reassembly. UDP delivers a
        small fraction out of order and Hik-RTP carries no per-packet
        sequence, so FU reassembly relies on in-order delivery. Out-of-order
        packets are buffered briefly; a genuinely lost packet is given up on
        after a flush timeout or once the buffer runs too far ahead, so the
        stream never stalls."""
        with self._lock:
            if self.srt_deliver_seq < 0:
                self.srt_deliver_seq = seq
            ahead = self._srt_ahead(seq)

            if ahead < 0:
                return  # late duplicate of an already-delivered seq

            if ahead == 0:
                out = [payload]
                self.srt_deliver_seq = (self.srt_deliver_seq + 1) & 0x7FFFFFFF
                out.extend(self._drain_reorder_locked())
            else:
                # Ahead of expected - a packet is missing. Buffer and wait.
                self.reorder_buf[seq] = payload
                if ahead > SRT_REORDER_MAX_AHEAD:
                    out = self._advance_past_gap_locked()
                else:
                    self._schedule_flush_locked()
                    out = []

        for p in out:
            self._feed(p)

    def _drain_reorder_locked(self) -> list[bytes]:
        """Release buffered packets now contiguous with the delivery cursor.
        Caller holds self._lock."""
        out: list[bytes] = []
        while True:
            key = self.srt_deliver_seq & 0xFFFFFFFF
            p = self.reorder_buf.pop(key, None)
            if p is None:
                break
            out.append(p)
            self.srt_deliver_seq = (self.srt_deliver_seq + 1) & 0x7FFFFFFF
        if not self.reorder_buf and self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        return out

    def _advance_past_gap_locked(self) -> list[bytes]:
        """Give up on the missing sequence, jump to the lowest buffered
        sequence and drain. Caller holds self._lock."""
        lowest = -1
        lowest_ahead = 1 << 62
        for k in self.reorder_buf:
            a = self._srt_ahead(k)
            if a >= 0 and a < lowest_ahead:
                lowest_ahead = a
                lowest = k
        if lowest < 0:
            return []
        self.srt_deliver_seq = lowest
        out = self._drain_reorder_locked()
        if self.reorder_buf:
            self._schedule_flush_locked()
        return out

    def _schedule_flush_locked(self) -> None:
        """Arm the reorder flush timer. Caller holds self._lock."""
        if self._flush_timer is not None:
            return

        def _on_flush() -> None:
            with self._lock:
                self._flush_timer = None
                out = []
                if self.reorder_buf:
                    out = self._advance_past_gap_locked()
            for p in out:
                self._feed(p)

        self._flush_timer = threading.Timer(SRT_REORDER_FLUSH, _on_flush)
        self._flush_timer.start()

    def _feed(self, payload: bytes) -> None:
        """Run a delivered video payload through the Hik-RTP extractor and
        push any completed NAL units onto the frame queue. The receive loop
        and the reorder flush timer can call this from different threads,
        so the stateful extractor work is serialized under _feed_lock."""
        with self._feed_lock:
            audio = extract_audio_payload(payload)
            if audio is not None:
                ts, seq = self._next_audio(len(audio))
                self._push(Frame(codec=CODEC_PCMA, payload=bytes(audio), timestamp=ts, frame_no=seq))
                return

            # One arrival is one access unit; all NALs it yields (e.g.
            # VPS/SPS/PPS/IDR) must share a single PTS, so stamp once per
            # payload rather than per NAL.
            nals = self.extractor.process(payload)
            if not nals:
                return
            ts = self._next_timestamp()
            for nal in nals:
                self._push(Frame(codec=CODEC_H265, payload=nal, timestamp=ts, frame_no=self._next_frame_no()))

    def _push(self, f: Frame) -> None:
        if self._close_event.is_set():
            return
        try:
            self.frames.put_nowait(f)
        except queue.Full:
            # Drop on a full buffer rather than block the receive loop; the
            # consumer fell behind and stale media is not worth stalling for.
            pass

    def _next_frame_no(self) -> int:
        with self._lock:
            self.frame_no += 1
            return self.frame_no

    def _media_ticks(self, hz: float) -> int:
        """Elapsed time since the session epoch scaled to the given clock
        rate (90 kHz video, 8 kHz audio), setting the epoch on first call.
        Video frames are timestamped by arrival because the device sends at
        a variable real rate and exposes no per-frame PTS. Caller must hold
        self._lock."""
        if self._epoch is None:
            self._epoch = time.monotonic()
        return int((time.monotonic() - self._epoch) * hz) & 0xFFFFFFFF

    def _next_timestamp(self) -> int:
        with self._lock:
            return self._media_ticks(90000)

    def _next_audio(self, samples: int) -> tuple[int, int]:
        with self._lock:
            if self.audio_frame_no == 0:
                self.audio_ts = self._media_ticks(8000)
            ts = self.audio_ts
            self.audio_ts = (self.audio_ts + samples) & 0xFFFFFFFF
            self.audio_frame_no += 1
            return ts, self.audio_frame_no

    # -- ACK / keepalive ticker --

    def _ticker_loop(self) -> None:
        next_ack = time.monotonic()
        next_keepalive = time.monotonic() + 15.0
        while not self._close_event.is_set():
            now = time.monotonic()
            if now >= next_ack:
                with self._lock:
                    last = self.last_ack_seq
                if last > 0:
                    self._send_srt_ack(last)
                next_ack = now + 0.01
            if now >= next_keepalive:
                self._send_keepalive()
                next_keepalive = now + 15.0
            self._close_event.wait(0.01)

    def _send_keepalive(self) -> None:
        pkt = bytearray(20)
        struct.pack_into(">H", pkt, 0, PKT_KEEPALIVE)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, self.source_id)
        self._send_to_device(bytes(pkt))

    def _send_srt_ack(self, last_recv_seq: int) -> None:
        with self._lock:
            ack_num = self.srt_ack_number
            self.srt_ack_number += 1
            peer = self.srt_peer_socket_id

        pkt = bytearray(44)
        struct.pack_into(">H", pkt, 0, SRT_CTRL_ACK)
        struct.pack_into(">I", pkt, 4, ack_num)
        struct.pack_into(">I", pkt, 8, _timestamp32())
        struct.pack_into(">I", pkt, 12, peer)
        struct.pack_into(">I", pkt, 16, (last_recv_seq + 1) & 0x7FFFFFFF)
        struct.pack_into(">I", pkt, 20, 8000)  # RTT (us)
        struct.pack_into(">I", pkt, 24, 1000)  # RTT variance (us)
        struct.pack_into(">I", pkt, 28, 8192)  # available buffer (packets)
        struct.pack_into(">I", pkt, 32, 1000)  # receiving rate (pkt/s)
        struct.pack_into(">I", pkt, 36, 100000)  # estimated link capacity (pkt/s)
        struct.pack_into(">I", pkt, 40, 0)  # receiving rate (bytes/s)
        self._send_to_device(bytes(pkt))

    # -- shutdown --

    def _send_teardown(self) -> None:
        """Best-effort SRT shutdown so the device releases the slot."""
        with self._lock:
            peer_socket = self.srt_peer_socket_id
        if peer_socket:
            shutdown = bytearray(16)
            struct.pack_into(">H", shutdown, 0, SRT_CTRL_SHUTDOWN)
            struct.pack_into(">I", shutdown, 8, _timestamp32())
            struct.pack_into(">I", shutdown, 12, peer_socket)
            self._send_to_device(bytes(shutdown))

    def _close_frames(self) -> None:
        try:
            self.frames.put_nowait(None)
        except queue.Full:
            pass

    # -- send helpers --

    def _send_to_device(self, data: bytes) -> None:
        with self._lock:
            peer = self.device_peer
        if peer is None:
            peer = (self.cfg.device_public_ip, self.cfg.device_public_port)
        self._send_to_addr(data, peer)

    def _send_to_addr(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            self.sock.sendto(data, addr)
        except OSError:
            pass

    def _send_to(self, data: bytes, host: str, port: int) -> None:
        self._send_to_addr(data, (host, port))

    # -- accessors --

    def _get_data_session_id(self) -> int:
        with self._lock:
            return self.data_session_id

    def _set_data_session(self, sid: int) -> None:
        _LOG.info("SRT data session established (id=%d)", sid)
        with self._lock:
            self.data_session_id = sid
        self._data_event.set()

    def _get_peer_socket_id(self) -> int:
        with self._lock:
            return self.srt_peer_socket_id

    def _local_addr(self) -> tuple[str, int]:
        ip, port = self.sock.getsockname()
        if not ip or ip == "0.0.0.0":
            ip = "0.0.0.0"
        return ip, port

    # -- waiting helpers --

    def _wait_for_data_session(self, timeout: float) -> None:
        if self._get_data_session_id() != 0:
            return
        self._data_event.wait(timeout)
