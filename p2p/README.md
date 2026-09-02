# EZVIZ cloud P2P protocol port (NOT used by the integration)

This folder is a from-scratch Python port of EZVIZ/Hik-Connect's cloud P2P
streaming protocol (UDP, P2P_SETUP + hole-punch + a proprietary SRT dialect
+ Hik-RTP media framing), ported from the Go reference implementation and
its reverse-engineering notes at
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(fork of AlexxIT/go2rtc, MIT licensed; see that project's `PROTOCOL.md` for
the wire-format specification).

**It is not wired into `custom_components/ezviz_stream/` and the
integration does not use it.**

## Why it exists, and why it's unused

We initially suspected the official EZVIZ app used this P2P channel (not
the VTM cloud relay the integration actually uses) to stream this specific
battery peephole, based on a `P2P` field present in the device's cloud
metadata. The full protocol was ported (`v3.py`, `hikrtp.py`, `api.py`,
`session.py`, `client.py`) and verified layer-by-layer:

- `v3.py`'s framing/CRC-8/AES-128-CBC was checked byte-for-byte against the
  Go reference's own test vectors (39/39 passing).
- The ported code correctly builds and sends a `P2P_SETUP` packet and
  correctly parses the cloud P2P server's response.

Live testing against the real device, however, showed the P2P server
consistently rejecting setup with error `0x101012` ("device unavailable for
P2P, server-side, before any punch") - even at a moment when the device was
confirmed online and streaming fine in the official app.

A `tcpdump` capture of the official app actually opening this exact
device's live view (done via a rooted Android emulator, since this is UDP
traffic a plain HTTP(S) proxy can't see) settled it: **the app used the VTM
relay (the same `externalIp`/`port` from the device's `VTM` cloud metadata,
confirmed by IP+port match), not the P2P channel, for this device.** The
`P2P` field in the device metadata appears to be an alternate/unused path
for this particular model, not what the app actually relies on for live
view.

## Disposition

Kept as a reference in case a future device genuinely needs the P2P path
(the protocol port itself works correctly up through P2P_SETUP; only the
device-availability rejection blocked going further) - but for this
integration's actual target device, the VTM path
(`custom_components/ezviz_stream/vendor/pyezvizapi/cloud_stream.py`, used
by `ezviz_client.EzvizStreamView`) is the one that matters and is what's
actually shipped.

## Contents

- `v3.py` - V3 binary control protocol (framing, CRC-8, AES-128-CBC).
- `hikrtp.py` - Hik-RTP media frame extractor (H.265 Annex-B, RFC 7798 FU reassembly).
- `api.py` - REST bootstrap (device P2P config, account P2P secret).
- `session.py` - UDP session: P2P_SETUP, hole-punch, SRT handshake, data path.
- `client.py` - top-level orchestration (`connect()`).
- `demo.py` - standalone script serving the P2P stream as HTTP MPEG-TS for
  local testing with curl/VLC, independent of Home Assistant.
