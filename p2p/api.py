"""REST bootstrap for an EZVIZ cloud P2P session.

Ported from `api.go` in
https://github.com/pedropaulovc/go2rtc/tree/feat/ezviz-p2p-transport/pkg/ezviz
(fork of AlexxIT/go2rtc, MIT licensed).

Rather than reimplementing login, this reuses an already-authenticated
`vendor.pyezvizapi.client.EzvizClient` (see `..ezviz_client.EzvizClient`,
which handles login/session caching) for the two calls it already knows how
to make (device pagelist), and only adds the one call it doesn't: the
account-level rotating P2P server key from `/api/p2p/configurations`.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any


class P2PBootstrapError(Exception):
    """Raised when P2P config/secret can't be resolved from the cloud."""


@dataclass
class P2PServer:
    ip: str
    port: int


@dataclass
class P2PConfig:
    """Per-device P2P configuration resolved from the device pagelist."""

    servers: list[P2PServer]
    secret_key: str  # KMS secret; first 32 ASCII chars seed the inner link key
    key_version: int
    wan_ip: str
    net_ip: str
    net_stream_port: int


@dataclass
class P2PSecret:
    """Account-level rotating P2P server key plus its salt."""

    key: bytes  # 32 bytes
    salt_index: int
    salt_ver: int
    servers: list[P2PServer]


#: The Go reference implementation (api.go) sends this exact clientType for
#: every P2P-bootstrap REST call, distinct from the generic mobile-app
#: clientType (3) pyezvizapi's own session otherwise uses. Real-world
#: testing showed P2P_SETUP rejected as "device unavailable" (0x101012)
#: when reusing the session's default clientType even though the device was
#: confirmed online and streaming fine in the official app at the same
#: moment - this header may be how the cloud scopes device P2P-availability
#: bookkeeping to the calling client type.
_P2P_CLIENT_TYPE = "55"


def get_p2p_config(pyez_client: Any, serial: str) -> P2PConfig:
    """Resolve per-device P2P servers, NAT-mapped stream endpoint and KMS secret."""
    base_url = f"https://{pyez_client._token['api_url']}"  # noqa: SLF001
    limit = 50
    offset = 0
    while True:
        resp = pyez_client._session.get(  # noqa: SLF001
            f"{base_url}/v3/userdevices/v1/resources/pagelist",
            params={
                "groupId": -1,
                "limit": limit,
                "offset": offset,
                "filter": "P2P,KMS,CONNECTION",
            },
            headers={"clientType": _P2P_CLIENT_TYPE},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("meta", {}).get("code") != 200:
            raise P2PBootstrapError(f"p2p config failed: {data.get('meta')}")

        p2p = (data.get("P2P") or {}).get(serial)
        if p2p:
            kms = (data.get("KMS") or {}).get(serial)
            if not kms:
                raise P2PBootstrapError(f"no KMS entry for device {serial}")
            conn = (data.get("CONNECTION") or {}).get(serial)
            if not conn:
                raise P2PBootstrapError(f"no CONNECTION entry for device {serial}")
            try:
                key_version = int(kms["version"])
            except (KeyError, ValueError) as err:
                raise P2PBootstrapError(f"invalid KMS version: {kms.get('version')!r}") from err

            return P2PConfig(
                servers=[P2PServer(ip=s["ip"], port=s["port"]) for s in p2p],
                secret_key=kms["secretKey"],
                key_version=key_version,
                wan_ip=conn.get("wanIp", ""),
                net_ip=conn.get("netIp", ""),
                net_stream_port=conn.get("netStreamPort", 0),
            )

        # Last page reached without finding the serial.
        if len(data.get("deviceInfos") or []) < limit:
            raise P2PBootstrapError(f"no P2P servers for device {serial}")
        offset += limit


def get_p2p_secret(pyez_client: Any) -> P2PSecret:
    """Fetch the rotating account-level P2P server key and salt.

    The key arrives as a decimal byte-array string ("[12,34,...]") of 32
    signed bytes.
    """
    resp = pyez_client._session.post(  # noqa: SLF001
        f"https://{pyez_client._token['api_url']}/api/p2p/configurations",  # noqa: SLF001
        headers={"clientType": _P2P_CLIENT_TYPE},
        timeout=15,
    )
    resp.raise_for_status()
    out = resp.json()

    secret = out.get("secret") or {}
    if out.get("resultCode") != "0" or not secret.get("data"):
        raise P2PBootstrapError(
            f"p2p secret failed: resultCode={out.get('resultCode')} {out.get('resultDes')}"
        )

    key = _parse_byte_array(secret["data"])
    if len(key) != 32:
        raise P2PBootstrapError(f"expected 32-byte P2P key, got {len(key)}")

    servers = [P2PServer(ip=s["ip"], port=s["port"]) for s in out.get("serverInfos", [])]
    return P2PSecret(
        key=key,
        salt_index=int(secret.get("saltIndex", 0)),
        salt_ver=int(secret.get("version", 0)),
        servers=servers,
    )


def _parse_byte_array(s: str) -> bytes:
    """Turn a "[b0, b1, ..., bN]" decimal byte-array string (signed values
    masked to a byte) into a bytes object."""
    s = s.strip().removeprefix("[").removesuffix("]")
    if not s:
        return b""
    return bytes(int(p.strip()) & 0xFF for p in s.split(","))


def extract_user_id(session_id: str) -> str:
    """Return the `aud` claim from a Hik-Connect session JWT.

    That claim is the account user id used in PLAY_REQUEST expand headers.
    """
    parts = session_id.split(".")
    if len(parts) != 3:
        return ""
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        payload = base64.urlsafe_b64decode(padded)
        claims = json.loads(payload)
    except (ValueError, json.JSONDecodeError):
        return ""
    return claims.get("aud", "")
