# Changelog

All notable changes to this project are documented in this file.

## [0.2.2] - 2026-08-28

### Fixed

- Added `http` to `manifest.json`'s `dependencies`: we use
  `homeassistant.components.http` (for the stream view) without
  declaring it, which hassfest correctly flagged.

### Changed

- The vendored ffmpeg remux process (`cloud_stream.py`) now uses
  low-latency flags (`-fflags nobuffer -flags low_delay -probesize 32k
  -analyzeduration 0 -muxdelay 0`) to cut down time-to-first-byte, after
  real-world testing showed Home Assistant's stream pipeline giving up
  ("Immediate exit requested") before the default ffmpeg probing settings
  produced any output.
- Added debug-level timing logs around login, response setup, and first
  chunk received, to help diagnose remaining startup-latency issues.
  Enable with:
  ```yaml
  logger:
    logs:
      custom_components.ezviz_stream: debug
  ```

## [0.2.1] - 2026-08-28

### Fixed

- The vendored `copy_cloud_stream_to_mpegts()` calls `output.flush()` on
  the file-like object it writes to, the same way it would on a real
  file/socket. `EzvizStreamView`'s bridge object
  (`_QueueOutput`) only implemented `write()`/`close()`, so every stream
  request crashed with `AttributeError: '_QueueOutput' object has no
  attribute 'flush'` before a single byte reached the client. Added a
  no-op `flush()`.

## [0.2.0] - 2026-08-28

### Fixed

- The `pyezvizapi` package published on PyPI does not include the
  login/cloud-stream helpers this integration needs (`export_token`,
  `cloud_stream`, `cas`) — they only exist on the project's `main` branch
  on GitHub. Depending on it as a plain pip requirement therefore failed
  on real installs with `AttributeError: 'EzvizClient' object has no
  attribute 'export_token'`.

### Changed

- Vendored the required `pyezvizapi` modules directly into
  `custom_components/ezviz_stream/vendor/pyezvizapi/` (Apache-2.0, see
  `THIRD_PARTY_LICENSES.md`) instead of depending on the PyPI package.
- Replaced the `pyezvizapi stream proxy` subprocess (bound to its own
  local port) with an HTTP view registered directly on Home Assistant's
  own web server (`EzvizStreamView`). The stream URL returned by
  `stream_source()` is now a short-lived **signed URL**
  (`/api/ezviz_stream/<entry_id>/<serial>.ts`), using the same
  `async_sign_path` mechanism Home Assistant's own `camera` component
  uses for URLs opened directly by `ffmpeg`.
- `manifest.json` requirements changed from `pyezvizapi` to the vendored
  code's actual dependencies: `pycryptodome`, `requests`, `paho-mqtt`,
  `xmltodict`.

### Notes

- Still requires `ffmpeg` on the Home Assistant host's `PATH` (used
  internally by the vendored code to remux the cloud VTM stream to
  MPEG-TS).
- No behavior change to the on-demand design: the EZVIZ cloud connection
  is still only opened once a client actually requests the stream, never
  in the background.

## [0.1.0] - 2026-08-28

### Added

- Initial release: on-demand `camera` entity for EZVIZ battery peepholes
  that don't support RTSP (tested against a CS-DP2C).
- Config flow to add a device (name, serial, EZVIZ account credentials,
  optional verification code).
- `should_poll = False` and `async_camera_image()` returning `None`, so
  the device is never contacted in the background — only
  `stream_source()` triggers any network activity, and only when a
  client actually opens the live view.
