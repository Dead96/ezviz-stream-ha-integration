"""Camera platform for the EZVIZ Stream (On-Demand) integration.

Design goal: this camera must NEVER talk to the EZVIZ device on its own
schedule. It is a battery-powered peephole, so any background polling would
drain it quickly. Concretely:

- `should_poll` is False and no coordinator/timer is set up anywhere.
- `async_camera_image` returns None, so dashboard cards never trigger
  periodic still-image ("thumbnail") requests.
- The only entry point that reaches the device is `stream_source()`, which
  Home Assistant's frontend/stream component calls exclusively when a user
  actually opens the live view (or something calls `camera.play_stream` /
  `camera.record` / an HLS URL is requested). That method delegates to the
  shared `EzvizClient` (see `ezviz_client.py`), which is where the actual
  EZVIZ API calls live.
"""

from __future__ import annotations

import logging

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_SERIAL, CONF_VERIFICATION_CODE, DOMAIN
from .ezviz_client import EzvizClient

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the EZVIZ Stream camera from a config entry."""
    client: EzvizClient = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([EzvizStreamCamera(entry, client)])


class EzvizStreamCamera(Camera):
    """An EZVIZ camera whose stream is fetched only on explicit request."""

    _attr_has_entity_name = True
    _attr_name = None
    _attr_should_poll = False
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(self, entry: ConfigEntry, client: EzvizClient) -> None:
        """Initialize the camera from config entry data."""
        super().__init__()
        self._entry = entry
        self._client = client
        self._serial: str = entry.data[CONF_SERIAL]
        self._verification_code: str | None = entry.data.get(CONF_VERIFICATION_CODE)

        self._attr_unique_id = self._serial
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, self._serial)},
            name=entry.data[CONF_NAME],
            manufacturer="EZVIZ",
            model="Battery Peephole",
        )

    async def stream_source(self) -> str | None:
        """Return the stream URL, fetched fresh for this request only.

        Home Assistant calls this on demand (e.g. when a Lovelace camera
        card is opened, or `camera.play_stream`/`camera.record` is called).
        It is never called on a timer by this integration.
        """
        try:
            return await self._async_get_stream_url()
        except Exception:  # noqa: BLE001 - log and degrade gracefully
            _LOGGER.exception(
                "Failed to obtain EZVIZ stream URL for %s", self._serial
            )
            return None

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Disable still-image snapshots.

        Returning None prevents dashboard cards and the frontend from
        polling a preview thumbnail in the background, which would wake the
        device even when nobody is actually watching the live stream.
        """
        return None

    async def _async_get_stream_url(self) -> str | None:
        """Fetch a fresh, ready-to-use stream URL from the EZVIZ API.

        Delegates to the shared `EzvizClient`, which handles login/session
        caching and the stream URL lookup. This is the only method that
        performs network I/O toward the device/cloud, and it's only ever
        called from `stream_source()`, so it stays fully on-demand.
        """
        return await self._client.async_get_stream_url(self._entry.entry_id, self._serial)
