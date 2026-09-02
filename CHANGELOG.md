# Changelog

All notable changes to this project are documented in this file.

## [0.4.1] - 2026-09-02

### Fixed

- 0.4.0's fan-out let the real EZVIZ connection (and its keep-alive calls)
  run indefinitely even after nobody was actually watching. Root cause:
  each viewer's cleanup relied on `response.write()` eventually raising to
  detect a disconnected client, but a `write()` to an already-closed
  socket can still succeed once before the OS surfaces the error - so a
  viewer whose connection died without a clean close (observed with some
  of go2rtc's own probe connections) kept counting as an active
  subscriber forever, which kept the shared stream (and the battery drain
  behind it) alive with no real viewer left.
- Every viewer (including the placeholder-only phase) now also polls the
  HTTP transport directly (`transport.is_closing()`) every few seconds,
  independent of whether a write has been attempted. This catches dead
  connections aiohttp already knows about internally but that our
  write-based detection was missing.

## [0.4.0] - 2026-09-02

### Changed

- Replaced the "one owner gets the real feed, everyone else gets a
  placeholder for 15s and gives up" design with real fan-out: all
  concurrent viewers of one camera (e.g. two browser tabs, or go2rtc's own
  extra probe connections) now share the single real EZVIZ VTM connection
  and all receive the actual video, instead of only the first one to
  connect. Found via real-world testing: opening the same camera in two
  browser tabs left one showing live video and the other stuck on the
  placeholder indefinitely.
- The real EZVIZ connection (and its keep-alive loop) now starts when the
  first viewer for a serial attaches and is torn down once the last one
  disconnects, whoever they are - it's no longer tied to a single "owning"
  request. This also keeps the device's one-session-at-a-time limit
  architecturally impossible to violate, rather than relying on a lock to
  avoid contention.
- A viewer joining a stream that's already flowing will briefly see the
  placeholder until the next real chunk reaches it (typically well under a
  second), then switches over like any other viewer.

## [0.3.3] - 2026-09-02

### Fixed

- Real-world logs showed HA's native `stream` component retrying the exact
  same `stream_source()` signed URL for well over a minute, all failing
  with HTTP 401 - the previous 5-minute signature expiration had already
  lapsed by the time it actually tried to use it, which looks like the
  main reason a "click to watch" could take ~90 seconds. Bumped the
  expiration to 1 hour; the URL only grants access to our own loopback
  re-stream endpoint, so a long-lived signature is low-risk.

## [0.3.2] - 2026-09-02

### Added

- A periodic keep-alive call (`delay_battery_device_sleep`, every 30s
  while a client is actively watching) to counteract the device's default
  ~60s auto-close on the live view. Best-effort only: failures are logged
  and retried a few times but never interrupt the video stream. Testing
  with `scripts/vtm_demo.py` (see 0.3.1) took playback from a hard ~58s
  ceiling to 100-180s per session, though results were noisy across runs
  and this isn't a guaranteed-indefinite fix - see the code comments in
  `ezviz_client.py` next to `_KEEPALIVE_INTERVAL_SECONDS` for the tuning
  history and reasoning.

## [0.3.1] - 2026-09-02

### Changed

- The loading placeholder is now a pre-rendered static image
  (`assets/placeholder.png` - a lens/spinner icon with "Connecting..." /
  "Waking up the camera" text) looped with ffmpeg, instead of a plain solid
  color. Still no `drawtext`/font dependency at runtime: the image is
  rendered once at dev time and shipped as a plain asset, so nothing about
  the Home Assistant host's ffmpeg build matters.
- Verified with a new standalone test tool
  (`scripts/vtm_demo.py`, mirroring `scripts/p2p_demo/demo.py`'s pattern):
  serves the VTM stream with the same placeholder-then-switch-over logic as
  the real integration, as local HTTP MPEG-TS for testing with curl/VLC
  without touching Home Assistant. Confirmed the placeholder-to-real-stream
  handoff is clean (no glitch/interleaving) end-to-end.

### Known issue under investigation

- Real-world testing showed the VTM stream itself dying after ~58s of
  playback with "Device offline or unreachable: timed out waiting for VTM
  stream data" - suspiciously close to a common ~60s battery-camera
  auto-timeout window. Trying a periodic `delay_battery_device_sleep()`
  call while streaming (the same endpoint investigated earlier, but as a
  keep-alive during an active view rather than a pre-connect wake-up) to
  see if it extends the session, mirroring what a "keep watching?" prompt
  in the official app likely does.

## [0.3.0] - 2026-08-28

### Added

- A placeholder video (solid color, generated on the fly with `ffmpeg`'s
  `lavfi` source - no extra image/font assets needed) is now streamed
  immediately when the stream view is opened, before the real EZVIZ VTM
  connection has produced any data. This addresses two problems at once:
  - go2rtc gave up almost immediately ("Immediate exit requested") when
    the connection produced no bytes for the first several seconds -
    real testing showed VTM connect times ranging from ~8s to well over
    a minute (looks like inherent battery-camera wake-up latency).
  - The "fail fast if busy" behavior from 0.2.4 returned a truly empty
    response for the losing concurrent attempts, which appears to have
    made go2rtc treat that source as broken (its internal RTSP restream
    later 404'd on it) rather than just retry.
- The connection that wins the per-serial attempt ("owner") switches
  seamlessly from placeholder to the real feed the moment real data
  arrives. Connections that lose ("followers", since the device only
  supports one VTM/P2P session at a time) show the placeholder for up to
  15 seconds and then close, instead of contending with the owner or
  queuing behind it.

## [0.2.4] - 2026-08-28

### Fixed

- The 0.2.3 lock made things worse, not better: real-world logs showed
  attempts queuing behind each other for over two minutes, because
  individual VTM connection attempts can themselves take anywhere from
  ~8s to 70+s (this looks like inherent battery-camera wake-up latency,
  not something in our control), and go2rtc only waits a few seconds per
  attempt before giving up and retrying on its own. Waiting in line
  behind a slow attempt meant every queued request was already doomed by
  the time its turn came.
- Changed to a "fail fast if busy" check instead of waiting for the
  lock: a new request only proceeds if no other attempt for that serial
  is currently in flight, otherwise it returns immediately (empty
  response) so go2rtc's own retry loop gets an unblocked shot next time,
  instead of joining a growing queue.

## [0.2.3] - 2026-08-28

### Fixed

- Real-world testing (with Home Assistant's `go2rtc` streaming backend)
  showed several near-simultaneous connections opened to the same
  camera's stream view while go2rtc probes/retries a new source. Each one
  independently tried to open its own EZVIZ VTM/P2P session, and the
  device only supports one at a time, so most of them failed with
  `DeviceException: Device offline or unreachable: timed out waiting for
  VTM stream data` even though the device was fine.
- Added a per-serial lock (`EzvizClient.stream_lock_for`) so concurrent
  stream attempts for the same camera are serialized instead of
  contending for the device's single session slot. Confirmed working:
  the stream was visibly playable end-to-end once this contention was
  the only remaining issue.

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
