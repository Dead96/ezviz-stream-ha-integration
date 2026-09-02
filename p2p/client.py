"""Top-level orchestration for an EZVIZ cloud P2P streaming session.

Ported from (the `connect()` portion of) `client.go` in
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(fork of AlexxIT/go2rtc, MIT licensed).

Unlike the Go version, login itself is not reimplemented here: callers pass
an already-authenticated `vendor.pyezvizapi.client.EzvizClient` (see
`..ezviz_client.EzvizClient`, which already handles login/session caching),
and this module only adds the P2P-specific bootstrap (device P2P config +
account P2P secret) and starts the session.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from . import api
from .session import Session, SessionConfig, random_client_id

_LOG = logging.getLogger(__name__)


def _wake_device(pyez_client: Any, serial: str, channel: int, sleep_type: int) -> None:
    """Best-effort call to `delay_battery_device_sleep` before attempting
    P2P_SETUP.

    EXPERIMENTAL: real-world testing showed the P2P server rejects setup
    with error 0x101012 ("device unavailable for P2P, server-side, before
    any punch") for this battery device - i.e. a server-side bookkeeping
    issue, not a client protocol bug. This call (undocumented; `sleep_type`
    semantics unknown) is our best lead so far for nudging the device to
    register itself as available before we contact the P2P servers. Never
    fatal: if it fails or does nothing, P2P_SETUP is attempted anyway.
    """
    try:
        result = pyez_client.delay_battery_device_sleep(serial, channel, sleep_type)
        _LOG.info("delay_battery_device_sleep(sleep_type=%d) -> %s", sleep_type, result)
    except Exception:
        _LOG.exception("delay_battery_device_sleep(sleep_type=%d) failed, continuing anyway", sleep_type)


def connect(
    pyez_client: Any,
    serial: str,
    *,
    channel: int = 1,
    stream_type: int = 1,
    bus_type: int = 1,
    wake_sleep_type: int | None = 0,
    wake_settle_seconds: float = 2.0,
) -> Session:
    """Resolve P2P config/secret for `serial` and bring up a streaming
    session. Blocks until the SRT data session is up or raises
    `session.SessionError` (mirrors `Client.connect` + `session.start` in
    the Go reference).

    `wake_sleep_type`: if not None, calls the experimental
    `delay_battery_device_sleep` wake-up endpoint first (see `_wake_device`);
    pass None to skip it entirely.
    """
    if wake_sleep_type is not None:
        _wake_device(pyez_client, serial, channel, wake_sleep_type)
        if wake_settle_seconds > 0:
            time.sleep(wake_settle_seconds)

    p2p_cfg = api.get_p2p_config(pyez_client, serial)
    secret = api.get_p2p_secret(pyez_client)

    # The link key (inner PLAY_REQUEST encryption) is the first 32 ASCII
    # chars of the KMS secret.
    link_key = p2p_cfg.secret_key.encode()
    if len(link_key) < 32:
        raise api.P2PBootstrapError(f"KMS secret too short: {len(link_key)} chars")
    link_key = link_key[:32]

    # P2P servers come from the per-device config; fall back to the
    # account-level list returned alongside the secret.
    servers = p2p_cfg.servers or secret.servers
    if not servers:
        raise api.P2PBootstrapError("no P2P servers available")

    # Device NAT-mapped stream endpoint: prefer the WAN IP, fall back to NET IP.
    device_ip = p2p_cfg.wan_ip or p2p_cfg.net_ip

    session_id = pyez_client._token["session_id"]  # noqa: SLF001
    cfg = SessionConfig(
        device_serial=serial,
        device_public_ip=device_ip,
        device_public_port=p2p_cfg.net_stream_port,
        p2p_servers=servers,
        p2p_key=secret.key,
        p2p_link_key=link_key,
        p2p_key_version=p2p_cfg.key_version,
        p2p_key_salt_index=secret.salt_index,
        p2p_key_salt_ver=secret.salt_ver,
        user_id=api.extract_user_id(session_id),
        client_id=random_client_id(),
        channel_no=channel,
        stream_type=stream_type,
        bus_type=bus_type,
    )

    sess = Session(cfg)
    try:
        sess.start()
    except Exception:
        sess.close()
        raise
    return sess
