"""The EZVIZ Stream (On-Demand) integration.

This integration exists because the official EZVIZ integration does not work
for this device (a battery-powered "peephole" camera). It intentionally never
polls the device in the background: the camera entity only talks to EZVIZ
when a client explicitly asks to view the stream, to preserve battery life.
"""

from __future__ import annotations

from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant

from .const import DOMAIN, PLATFORMS
from .ezviz_client import EzvizClient


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EZVIZ Stream from a config entry.

    Building the client here does not perform any network I/O - it only
    logs in lazily, the first time a camera actually needs a stream.
    """
    token_path = Path(hass.config.path(".storage", f"ezviz_stream_{entry.entry_id}.json"))
    client = EzvizClient(
        hass=hass,
        account=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        token_path=token_path,
    )
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = client

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        client: EzvizClient = hass.data[DOMAIN].pop(entry.entry_id)
        await client.async_close()
    return unload_ok
