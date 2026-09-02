"""Hik-RTP frame extractor: Hikvision P2P/SRT data payloads -> H.265 Annex-B.

Ported from `hikrtp.go` in
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(fork of AlexxIT/go2rtc, MIT licensed).

Framing (derived from pcap/live-capture analysis of the Hik-Connect cloud
transport):

- A data payload starts with a 12-byte Hik-RTP header; the first two bytes
  are the packet type (0x8060/0x8050/0x8051 carry video).
- After the header an optional 13-byte sub-frame header may follow
  (0x0d + 4 variable bytes + an 8-byte sync pattern).
- After the sub-header is the NAL payload: VPS/SPS/PPS (types 32-34),
  slice data (types 0-21), or length-prefixed Hikvision metadata
  (00 NN 00 LL [data]).
- NAL type 49 is an HEVC Fragmentation Unit (FU) per RFC 7798.
"""

from __future__ import annotations

import struct

HIK_RTP_HEADER_LEN = 12
SUB_HEADER_LEN = 13  # 0x0d + 4 bytes + 8-byte sync
FU_NAL_TYPE = 49

ANNEX_B_START_CODE = b"\x00\x00\x00\x01"

# Hik-RTP sub-header byte 2 marking an audio sub-frame.
AUDIO_SUB_TYPE = 0x88

_SUB_HEADER_HIGH_NIBBLES = {0x80, 0x90, 0xA0, 0xD0}


def _is_video_packet_type(t: int) -> bool:
    """A Hik-RTP type that carries video payload (as opposed to 0x807f
    control keepalives or 0x0200 IMKH metadata)."""
    return t in (0x8060, 0x8050, 0x8051)


def extract_audio_payload(payload: bytes) -> bytes | None:
    """Return raw G.711 A-law audio samples from an audio Hik-RTP packet.

    Audio shares the video packet types and the 13-byte sub-header layout,
    distinguished only by sub-header byte 2 == 0x88.
    """
    if len(payload) <= HIK_RTP_HEADER_LEN:
        return None
    if not _is_video_packet_type(struct.unpack(">H", payload[:2])[0]):
        return None
    rtp = payload[HIK_RTP_HEADER_LEN:]
    if len(rtp) <= SUB_HEADER_LEN or rtp[0] != 0x0D:
        return None
    if (rtp[1] & 0xF0) not in _SUB_HEADER_HIGH_NIBBLES:
        return None
    if rtp[2] != AUDIO_SUB_TYPE:
        return None
    return rtp[SUB_HEADER_LEN:]


class HikRTPExtractor:
    """Reassembles whole H.265 NAL units from a sequence of Hik-RTP data
    payloads. Stateful across packets to carry RFC 7798 FU fragments."""

    def __init__(self) -> None:
        self._fu_fragments: list[bytes] = []
        self._fu_nal_header: bytes | None = None
        self._fu_complete = False
        self.nal_count = 0

    def _reset_fu(self) -> None:
        self._fu_fragments = []
        self._fu_nal_header = None
        self._fu_complete = False

    def _flush(self) -> list[bytes]:
        """Return a completed-but-buffered FU NAL if one is pending, else [].

        An incomplete FU (no End received) is discarded to prevent decoder
        corruption.
        """
        try:
            if not self._fu_fragments or self._fu_nal_header is None or not self._fu_complete:
                return []
            assembled = self._fu_nal_header + b"".join(self._fu_fragments)
            nal = self._build_nal(assembled)
            return [nal] if nal is not None else []
        finally:
            self._reset_fu()

    def process(self, payload: bytes) -> list[bytes]:
        """Consume one raw P2P data payload; return any complete Annex-B NAL
        units it produced (each start-code prefixed). May return []."""
        if len(payload) < 2:
            return []

        if not _is_video_packet_type(struct.unpack(">H", payload[:2])[0]):
            return []

        if len(payload) <= HIK_RTP_HEADER_LEN:
            return []
        rtp_payload = payload[HIK_RTP_HEADER_LEN:]

        data_start = 0
        if rtp_payload[0] == 0x0D and len(rtp_payload) > SUB_HEADER_LEN:
            if (rtp_payload[1] & 0xF0) in _SUB_HEADER_HIGH_NIBBLES:
                if rtp_payload[2] == 0x88:
                    return []  # audio packet, skip
                data_start = SUB_HEADER_LEN

        nal_data = rtp_payload[data_start:]
        if len(nal_data) < 2:
            return []

        return self._process_nal_unit(nal_data)

    def _process_nal_unit(self, data: bytes) -> list[bytes]:
        if len(data) < 2:
            return []

        first_byte = data[0]
        nal_type = (first_byte >> 1) & 0x3F

        out: list[bytes] = []

        if nal_type != FU_NAL_TYPE and self._fu_nal_header is not None:
            out.extend(self._flush())

        # Length-prefixed format: 00 NN 00 LL [LL bytes of NAL data].
        if first_byte == 0x00 and len(data) > 4:
            offset = 0
            while offset + 4 <= len(data):
                if data[offset] != 0x00:
                    break
                data_len = struct.unpack(">H", data[offset + 2 : offset + 4])[0]
                if data_len <= 0 or offset + 4 + data_len > len(data):
                    break
                nal = self._build_nal(data[offset + 4 : offset + 4 + data_len])
                if nal is not None:
                    out.append(nal)
                offset += 4 + data_len
            return out

        # Standard HEVC NAL types: slices (0-21), VPS(32)/SPS(33)/PPS(34),
        # SEI (35, 39-40).
        if nal_type <= 21 or (32 <= nal_type <= 35) or nal_type in (39, 40):
            nal = self._build_nal(data)
            if nal is not None:
                out.append(nal)
            return out

        # NAL type 49: HEVC Fragmentation Unit (FU) per RFC 7798.
        if nal_type == FU_NAL_TYPE and len(data) >= 3:
            fu_header = data[2]
            is_start = (fu_header >> 7) & 1
            is_end = (fu_header >> 6) & 1
            fu_type = fu_header & 0x3F

            if is_start:
                out.extend(self._flush())
                orig_first_byte = (data[0] & 0x81) | ((fu_type << 1) & 0x7E)
                self._fu_nal_header = bytes([orig_first_byte, data[1]])
                self._fu_fragments = [data[3:]]
            else:
                self._fu_fragments.append(data[3:])

            if is_end:
                self._fu_complete = True
                out.extend(self._flush())
            return out

        # Other NAL types in the 48-63 range: pass through.
        if nal_type >= 48:
            nal = self._build_nal(data)
            if nal is not None:
                out.append(nal)
            return out

        return out

    def _build_nal(self, data: bytes) -> bytes | None:
        """Validate a NAL unit and prefix it with the Annex-B start code."""
        if len(data) < 2:
            return None

        nal_type = (data[0] >> 1) & 0x3F
        if nal_type == 0 and data[1] == 0:
            return None

        self.nal_count += 1
        return ANNEX_B_START_CODE + data
