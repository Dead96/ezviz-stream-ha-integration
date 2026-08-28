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
├── __init__.py        # sets up the entry, creates the EZVIZ client, forwards to the camera platform
├── camera.py           # camera entity: the "on-demand" logic lives here
├── ezviz_client.py      # login + stream proxy lifecycle, via pyezvizapi
├── config_flow.py       # UI configuration form (Settings > Devices & services)
├── const.py
├── manifest.json
├── strings.json
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

## How the streaming works (`ezviz_client.py`)

We don't reimplement EZVIZ's proprietary cloud protocol ourselves: we rely
on [`pyezvizapi`](https://pypi.org/project/pyezvizapi/) (the same library
the official Home Assistant EZVIZ integration uses for login/device
management), which also ships an experimental `stream proxy` command that
speaks the VTM protocol, remuxes it with `ffmpeg`, and re-serves it as
plain HTTP MPEG-TS — something Home Assistant's `stream` component can
open directly.

**Verified manually end-to-end** against this device: login (v5 API,
MD5-hashed password) followed by pulling the cloud VTM stream through the
proxy returned real MPEG-TS video bytes (`Content-Type: video/MP2T`).

On the first call to `async_get_stream_url(serial)`:

1. Login (or session refresh) via `pyezvizapi`, run in an executor since
   the library is synchronous (`requests`-based). The session token is
   cached to disk (`.storage/ezviz_stream_<entry_id>.json`).
2. If not already running, a subprocess is started:
   `python -m pyezvizapi --token-file ... stream proxy --serial ... --listen-port ...`,
   bound only to `127.0.0.1` on a port deterministically derived from the
   serial (range 8558-8657). The subprocess stays listening but **never
   talks to EZVIZ until a client actually connects** to the HTTP URL.
3. `http://127.0.0.1:<port>/<serial>.ts` is returned, which HA hands to its
   own stream/ffmpeg pipeline for playback.

The subprocess is terminated in `async_unload_entry` when the integration
is removed/reloaded.

### External dependencies

- `pyezvizapi` (declared in `manifest.json`, installed automatically by
  Home Assistant).
- **`ffmpeg`** must be available on the Home Assistant host's `PATH`
  (already present on Home Assistant OS/Supervised; needs checking on Core
  installs).

### Known limitations / TODO

- **Startup latency**: in manual testing, it took about 10 seconds from
  the first HTTP request to the first byte of video (spawning the
  `ffmpeg` subprocess + initial buffering). This is inherent to the
  proxy's remuxing approach, not something our integration adds on top —
  worth keeping in mind if a dashboard card's own stream timeout is short.
- No retry/backoff if the login or the proxy startup fails on the first
  attempt — a second stream open request from HA just redoes everything
  from scratch.
- The device isn't currently encrypted (`isEncrypt: 0`): if encryption
  gets enabled later, the proxy would need `--decrypt-video` plus the
  verification code (already collected in the config flow as
  `verification_code`, not used yet).
- The local proprietary CPD7 protocol (ports 9010/9020, used by other
  EZVIZ models like HP7/CP7) was not attempted: the cloud VTM path turned
  out to be sufficient and simpler.

## Installation

### Via HACS (custom repository)

This integration isn't in the default HACS store, so add it as a custom
repository:

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
