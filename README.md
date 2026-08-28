# EZVIZ Stream (On-Demand) — Home Assistant custom integration

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![Validate](https://github.com/Dead96/ezviz-stream-ha-integration/actions/workflows/validate.yml/badge.svg)](https://github.com/Dead96/ezviz-stream-ha-integration/actions/workflows/validate.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Custom integration for a battery-powered EZVIZ digital peephole (tested
against a CS-DP2C), designed to avoid draining the battery: **the device is
never polled in the background** — the stream only starts when it's
actually requested (e.g. opening the camera card on a dashboard).

The official Home Assistant EZVIZ integration doesn't work with this model
because it relies on RTSP, and this device doesn't expose RTSP at all
(`localRtspPort`/`netRtspPort` are both `0` in its own EZVIZ metadata).
Streaming only happens through EZVIZ's proprietary cloud VTM relay
(`ysproto` protocol), not RTSP.

## Structure

```
custom_components/ezviz_stream/
├── __init__.py        # sets up the entry, registers the stream view, forwards to the camera platform
├── camera.py           # camera entity: the "on-demand" logic lives here
├── ezviz_client.py      # login + the HTTP view that streams MPEG-TS on demand
├── config_flow.py       # UI configuration form (Settings > Devices & services)
├── const.py
├── manifest.json
├── strings.json
├── vendor/pyezvizapi/    # vendored login/cloud-stream helpers (see THIRD_PARTY_LICENSES.md)
└── translations/
    ├── en.json
    └── it.json
```

## How the "on-demand" behavior works

In [`camera.py`](custom_components/ezviz_stream/camera.py):

- `should_poll = False` and there's no timer/coordinator anywhere: the
  entity never polls on its own.
- `async_camera_image()` always returns `None`, so Lovelace cards never
  trigger periodic still-image ("thumbnail") requests, which would wake
  the device even when nobody is actually watching the live stream.
- `stream_source()` is the only point Home Assistant calls when it
  actually needs the stream (opening the camera card, `camera.play_stream`,
  `camera.record`, etc.). It delegates to
  [`EzvizClient.async_get_stream_url()`](custom_components/ezviz_stream/ezviz_client.py).

## How the streaming works

We don't reimplement EZVIZ's proprietary cloud protocol ourselves: we
depend on login/cloud-stream helpers from
[RenierM26/pyEzvizApi](https://github.com/RenierM26/pyEzvizApi) (the same
project the official Home Assistant EZVIZ integration uses), which can
speak the VTM protocol and remux it with `ffmpeg` into plain MPEG-TS.

This code is **vendored** into
[`vendor/pyezvizapi/`](custom_components/ezviz_stream/vendor/pyezvizapi/)
rather than declared as a normal `pyezvizapi` pip dependency: the
`login`/`export_token`/`cloud_stream` helpers this integration relies on
live on that project's `main` branch but aren't part of the version
currently published on PyPI. See
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) for the Apache-2.0
attribution.

**Verified manually end-to-end** against this device: login (MD5-hashed
password) followed by pulling the cloud VTM stream returned real MPEG-TS
video bytes.

Rather than spawning a separate subprocess/port, the stream is served
through an HTTP view registered directly on Home Assistant's own web
server ([`EzvizStreamView`](custom_components/ezviz_stream/ezviz_client.py)):

1. `stream_source()` calls `EzvizClient.async_get_stream_url()`, which
   ensures we're logged in (cached session token at
   `.storage/ezviz_stream_<entry_id>.json`, so restarts don't need a fresh
   password login) and returns a short-lived **signed URL**
   (`/api/ezviz_stream/<entry_id>/<serial>.ts`) — the same
   `async_sign_path` mechanism Home Assistant's own camera component uses
   for URLs that ffmpeg opens directly, since ffmpeg can't supply a bearer
   token.
2. Home Assistant's stream pipeline opens that URL. Only *then* does
   `EzvizStreamView.get()` open the actual EZVIZ cloud VTM connection,
   spawn `ffmpeg` to remux it, and stream the MPEG-TS bytes back — nothing
   talks to EZVIZ before a client actually connects.
3. When the HTTP client disconnects, the `ffmpeg`/VTM copy loop is torn
   down.

### External dependencies

- `pycryptodome`, `requests`, `paho-mqtt`, `xmltodict` (declared in
  `manifest.json`, installed automatically by Home Assistant) — required
  by the vendored `pyezvizapi` code.
- **`ffmpeg`** must be available on the Home Assistant host's `PATH`
  (already present on Home Assistant OS/Supervised; needs checking on Core
  installs).

### Known limitations / TODO

- No retry/backoff if login fails on the first attempt — a second stream
  open request from HA just redoes everything from scratch.
- The device isn't currently encrypted (`isEncrypt: 0`): if encryption
  gets enabled later, this would need `client.get_cam_key()` plus the
  verification code (already collected in the config flow as
  `verification_code`, not used yet).
- The local proprietary CPD7 protocol (ports 9010/9020, used by other
  EZVIZ models like HP7/CP7) was not attempted: the cloud VTM path turned
  out to be sufficient and simpler.
- Each viewer opens its own independent upstream EZVIZ connection (no
  multiplexing) — fine for the expected single-viewer peephole use case.

## Installation

### Via HACS (custom repository)

This integration isn't in the default HACS store, so add it as a custom
repository. (The `brands` check in CI is intentionally non-blocking: it
only matters for submitting to the default store, which requires an icon
merged into [home-assistant/brands](https://github.com/home-assistant/brands)
- not needed to use this repo as a custom repository.)

1. In Home Assistant, go to **HACS > Integrations**, open the **⋮** menu
   in the top right, and choose **Custom repositories**.
2. Add `https://github.com/Dead96/ezviz-stream-ha-integration` as the
   repository URL, with category **Integration**.
3. Find "EZVIZ Stream (On-Demand)" in HACS and install it.
4. Make sure `ffmpeg` is available on the host.
5. Restart Home Assistant.
6. Go to **Settings > Devices & services > Add integration**, search for
   "EZVIZ Stream (On-Demand)" and fill in the form (name, device serial,
   EZVIZ account and password).

### Manual

1. Copy the `custom_components/ezviz_stream` folder into your Home
   Assistant instance's `custom_components` folder.
2. Follow steps 4-6 above.

## License

[MIT](LICENSE)
