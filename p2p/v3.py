"""EZVIZ/Hik-Connect "V3" binary control protocol (UDP).

Ported from the Go reference implementation and its reverse-engineered
protocol notes at
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(itself a fork of AlexxIT/go2rtc, MIT licensed), which documents the wire
format observed in Hikvision's `libezstreamclient.so`. See that project's
`PROTOCOL.md` for the full specification this module implements.

Frame layout (big-endian multi-byte fields), 12-byte header:

    off  size  field
    0    1     magic     high nibble 0xE; first byte emitted is 0xE2
    1    1     mask      flag bitfield (see Mask)
    2    2     msg_type  opcode
    4    4     seq_num   sequence number
    8    2     reserved  protocol-version constant 0x6234
    10   1     header_len 0x0C (12)
    11   1     crc8      CRC-8 over the whole packet with this byte zeroed
    12   ..    body      TLV attributes, optionally AES-128-CBC encrypted
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

from Crypto.Cipher import AES

MAGIC = 0xE2
HEADER_LEN = 12

# Opcodes (message types, big-endian at offset 2).
OP_P2P_SETUP = 0x0B02
OP_TRANSFOR_CTRL = 0x0B03
OP_TRANSFOR_DATA = 0x0B04
OP_PUNCH_REQUEST = 0x0C00  # device -> client: hole-punch request
OP_PUNCH_RESPONSE = 0x0C01  # client -> device: hole-punch response
OP_PLAY_REQUEST = 0x0C02
OP_TEARDOWN = 0x0C04

# Attribute tags (the T in the body TLVs).
ATTR_TRANSFOR_DATA = 0x00
ATTR_EXPAND_KEY_VERSION = 0x01
ATTR_CLIENT_ID = 0x02
ATTR_DEVICE_CHANNEL = 0x03
ATTR_BUS_TYPE_ENC = 0x04
ATTR_SESSION_KEY = 0x05
ATTR_SESSION_INFO = 0x06
ATTR_LARGE_DATA = 0x07
ATTR_BUS_TYPE = 0x76
ATTR_CHANNEL_NO = 0x77
ATTR_STREAM_TYPE = 0x78
ATTR_START_TIME = 0x7A
ATTR_STOP_TIME = 0x7B
ATTR_DEVICE_SESSION_ALT = 0x7D
ATTR_STREAM_SESSION = 0x7E
ATTR_PORT_COUNT = 0x82
ATTR_STREAM_META = 0x83
ATTR_DEVICE_SESSION = 0x84
ATTR_OPT_META1 = 0xB2
ATTR_OPT_META2 = 0xB3
ATTR_END_MARKER = 0xFF

# Fixed AES IV for all V3 encryption: ASCII "01234567" + 8 zero bytes.
_V3_IV = b"01234567" + b"\x00" * 8
_AES_BLOCK = 16


class V3ProtocolError(Exception):
    """Raised on malformed/rejected V3 frames."""


@dataclass
class Attr:
    tag: int
    value: bytes


@dataclass
class Mask:
    encrypt: bool = False
    salt_version: int = 0  # 1 bit
    salt_index: int = 0  # 3 bits, selects one of 8 salt-indexed keys
    expand_header: bool = False
    is_2b_len: bool = False  # tag 0x07 attributes use a 2-byte length

    def encode(self) -> int:
        b = 0
        if self.encrypt:
            b |= 1 << 7
        b |= (self.salt_version & 1) << 6
        b |= (self.salt_index & 7) << 3
        if self.expand_header:
            b |= 1 << 2
        if self.is_2b_len:
            b |= 1 << 1
        return b

    @classmethod
    def decode(cls, b: int) -> Mask:
        return cls(
            encrypt=bool(b & 0x80),
            salt_version=(b >> 6) & 1,
            salt_index=(b >> 3) & 7,
            expand_header=bool(b & 0x04),
            is_2b_len=bool(b & 0x02),
        )


def default_mask() -> Mask:
    """Mask with all flags off except is_2b_len, the common case."""
    return Mask(is_2b_len=True)


@dataclass
class Message:
    msg_type: int
    seq_num: int = 0
    reserved: int = 0
    mask: Mask = field(default_factory=Mask)
    attrs: list[Attr] = field(default_factory=list)


def crc8(data: bytes) -> int:
    """Hikvision's custom bitwise CRC-8 from libezstreamclient.so.

    Not a standard polynomial CRC; the bit operations are reproduced exactly
    from the reverse-engineered Go implementation.
    """
    crc = 0
    for d in data:
        x = (d ^ (crc & 0xFF)) & 0xFF

        crc = 0x23 if x & 1 else 0
        if x & 2:
            crc ^= 0x46
        if x & 4:
            crc ^= 0x8C

        tmp = crc >> 1
        if (crc ^ (x >> 3)) & 1:
            tmp = (crc >> 1) ^ 0x8C

        crc = tmp >> 1
        if (tmp ^ (x >> 4)) & 1:
            crc = ((tmp >> 1) | 0x80) ^ 0x0C

        tmp = crc >> 1
        if (crc ^ (x >> 5)) & 1:
            tmp = ((crc >> 1) | 0x80) ^ 0x0C

        crc = tmp >> 1
        if (tmp ^ (x >> 6)) & 1:
            crc = ((tmp >> 1) | 0x80) ^ 0x0C

        tmp = crc >> 1
        if (crc & 1) != (x >> 7):
            tmp = ((crc >> 1) | 0x80) ^ 0x0C

        crc = tmp
    return crc & 0xFF


def _encode_attrs(attrs: list[Attr], is_2b_len: bool) -> bytes:
    """Serialize TLV attributes.

    Tag 0x07 carries a 2-byte length when is_2b_len is set, every other tag
    uses a single length byte.
    """
    out = bytearray()
    for a in attrs:
        if a.tag == ATTR_LARGE_DATA and is_2b_len:
            out.append(a.tag)
            out += struct.pack(">H", len(a.value))
            out += a.value
            continue
        out.append(a.tag)
        out.append(len(a.value))
        out += a.value
    return bytes(out)


def _decode_attrs(buf: bytes, is_2b_len: bool) -> list[Attr]:
    """Parse TLV attributes out of a body buffer."""
    attrs: list[Attr] = []
    off = 0
    while off < len(buf):
        tag = buf[off]

        # Tag 0xFF is either an end marker (length 0, terminates the list)
        # or a sub-TLV container (length > 0, e.g. in P2P_SETUP). The length
        # byte distinguishes them.
        if tag == ATTR_END_MARKER and off + 1 < len(buf) and buf[off + 1] == 0:
            attrs.append(Attr(tag=tag, value=b""))
            break

        if tag == ATTR_LARGE_DATA and is_2b_len:
            if off + 3 > len(buf):
                break
            n = struct.unpack(">H", buf[off + 1 : off + 3])[0]
            end = min(off + 3 + n, len(buf))
            attrs.append(Attr(tag=tag, value=buf[off + 3 : end]))
            off = end
            continue

        if off + 2 > len(buf):
            break
        n = buf[off + 1]
        end = min(off + 2 + n, len(buf))
        attrs.append(Attr(tag=tag, value=buf[off + 2 : end]))
        off = end
    return attrs


def _pkcs7_pad(b: bytes) -> bytes:
    pad = _AES_BLOCK - len(b) % _AES_BLOCK
    return b + bytes([pad]) * pad


def _pkcs7_unpad(b: bytes) -> bytes:
    pad = b[-1]
    if pad == 0 or pad > _AES_BLOCK or pad > len(b):
        raise V3ProtocolError(f"invalid PKCS#7 padding: {pad}")
    if b[-pad:] != bytes([pad]) * pad:
        raise V3ProtocolError("invalid PKCS#7 padding bytes")
    return b[:-pad]


def aes_encrypt(body: bytes, key: bytes) -> bytes:
    """AES-128-CBC with PKCS#7 padding. Uses the first 16 bytes of `key`."""
    if len(key) < _AES_BLOCK:
        raise V3ProtocolError(f"v3 key too short: {len(key)} bytes")
    cipher = AES.new(key[:_AES_BLOCK], AES.MODE_CBC, _V3_IV)
    return cipher.encrypt(_pkcs7_pad(body))


def aes_decrypt(body: bytes, key: bytes) -> bytes:
    """Reverses aes_encrypt."""
    if len(key) < _AES_BLOCK:
        raise V3ProtocolError(f"v3 key too short: {len(key)} bytes")
    if len(body) == 0 or len(body) % _AES_BLOCK != 0:
        raise V3ProtocolError(f"v3 ciphertext not block-aligned: {len(body)} bytes")
    cipher = AES.new(key[:_AES_BLOCK], AES.MODE_CBC, _V3_IV)
    return _pkcs7_unpad(cipher.decrypt(body))


def encode_message(msg: Message, key: bytes | None = None) -> bytes:
    """Serialize a control message.

    When mask.encrypt is set and a key is supplied, the TLV body is
    AES-128-CBC encrypted.
    """
    body = _encode_attrs(msg.attrs, msg.mask.is_2b_len)

    if msg.mask.encrypt and key is not None:
        body = aes_encrypt(body, key)

    full = bytearray(HEADER_LEN + len(body))
    full[0] = MAGIC
    full[1] = msg.mask.encode()
    struct.pack_into(">H", full, 2, msg.msg_type)
    struct.pack_into(">I", full, 4, msg.seq_num)
    struct.pack_into(">H", full, 8, msg.reserved)
    full[10] = HEADER_LEN
    full[11] = 0x00  # CRC placeholder
    full[HEADER_LEN:] = body

    full[11] = crc8(bytes(full))
    return bytes(full)


def decode_message(buf: bytes, key: bytes | None = None) -> Message:
    """Parse a control message.

    Verifies the CRC and optionally decrypts the body when the encrypt flag
    is set and a key is supplied.
    """
    if len(buf) < HEADER_LEN:
        raise V3ProtocolError(f"v3 message too short: {len(buf)} bytes")
    if buf[0] >> 4 != 0xE:
        raise V3ProtocolError(f"invalid v3 magic: 0x{buf[0]:02x}")

    mask = Mask.decode(buf[1])
    msg_type = struct.unpack(">H", buf[2:4])[0]
    seq_num = struct.unpack(">I", buf[4:8])[0]
    reserved = struct.unpack(">H", buf[8:10])[0]
    header_len = buf[10]

    stored_crc = buf[11]
    check = bytearray(buf)
    check[11] = 0x00
    computed = crc8(bytes(check))
    if computed != stored_crc:
        raise V3ProtocolError(
            f"crc8 mismatch: stored=0x{stored_crc:02x} computed=0x{computed:02x}"
        )

    if header_len > len(buf):
        raise V3ProtocolError(f"v3 headerLen {header_len} exceeds packet {len(buf)}")
    body = buf[header_len:]
    if mask.encrypt and key is not None:
        body = aes_decrypt(body, key)

    return Message(
        msg_type=msg_type,
        seq_num=seq_num,
        reserved=reserved,
        mask=mask,
        attrs=_decode_attrs(body, mask.is_2b_len),
    )


def get_string_attr(attrs: list[Attr], tag: int) -> str | None:
    """Return the first attribute with the given tag as a string, or None."""
    for a in attrs:
        if a.tag == tag:
            return a.value.decode()
    return None


def get_int_attr(attrs: list[Attr], tag: int) -> int | None:
    """Return the first attribute with the given tag as a big-endian int.

    Only 1-, 2- and 4-byte values are supported; None for a missing tag or
    any other length.
    """
    for a in attrs:
        if a.tag != tag:
            continue
        if len(a.value) == 4:
            return struct.unpack(">I", a.value)[0]
        if len(a.value) == 2:
            return struct.unpack(">H", a.value)[0]
        if len(a.value) == 1:
            return a.value[0]
        return None
    return None
