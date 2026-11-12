"""Wire protocol, 64-bit integer handling and pure range/key helpers.

Design notes
------------
* pos / s / C are non-negative 64-bit integers and booleans are forbidden in
  those slots.  JSON itself cannot express a uint64 distinct from a signed
  integer, so every boundary is validated as ``int`` (not ``bool``) against the
  closed range ``[0, 2**64 - 1]``.
* Keys are non-empty ASCII strings; ASCII byte ordering equals Python string
  ordering for ``str`` values consisting of ASCII code points, and equals
  SQLite's default BINARY collation on TEXT columns.  Range membership uses
  half-open intervals ``[lo, hi)`` where ``hi is None`` means +infinity.
* Positions are stored in SQLite as zero-padded TEXT (20 digits) so that every
  uint64 value, including values larger than the signed 64-bit maximum, has a
  stable lexicographic order identical to numeric order.
"""

from __future__ import annotations

import http.client
import json
import urllib.error
import urllib.request
from typing import Any, Optional, Sequence, Tuple

UINT64_MAX = (1 << 64) - 1
POS_WIDTH = 20  # len(str(2**64 - 1)) == 20


# ---------------------------------------------------------------------------
# uint64 helpers
# ---------------------------------------------------------------------------
def is_u64(value: Any) -> bool:
    """True only for plain ints in the uint64 range.

    ``bool`` is a subclass of ``int`` in Python; the spec forbids booleans in
    numeric slots, so it is rejected explicitly.
    """
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= UINT64_MAX


def require_u64(value: Any, name: str) -> int:
    if not is_u64(value):
        raise ProtocolError(f"{name} must be a non-negative 64-bit integer, got {value!r}")
    return value


def enc_pos(pos: int) -> str:
    require_u64(pos, "pos")
    return str(pos).zfill(POS_WIDTH)


def dec_pos(text: str) -> int:
    return int(text)


# ---------------------------------------------------------------------------
# key / range helpers
# ---------------------------------------------------------------------------
def validate_key(key: Any) -> str:
    if not isinstance(key, str) or not key:
        raise ProtocolError(f"key must be a non-empty ASCII string, got {key!r}")
    try:
        key.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ProtocolError(f"key must be ASCII, got {key!r}") from exc
    return key


def key_in_range(key: str, lo: Optional[str], hi: Optional[str]) -> bool:
    """Half-open membership: ``lo <= key < hi``; ``None`` endpoint = infinity."""
    if lo is not None and key < lo:
        return False
    if hi is not None and not (key < hi):
        return False
    return True


def find_chunk(key: str, ranges: Sequence[Tuple[Optional[str], Optional[str]]]) -> int:
    """Return the index of the fixed range owning ``key``.

    The ranges supplied by the source are ascending, non-overlapping and cover
    every key, so exactly one range matches.
    """
    for idx, (lo, hi) in enumerate(ranges):
        if key_in_range(key, lo, hi):
            return idx
    raise ProtocolError(f"key {key!r} is not covered by any chunk range {ranges!r}")


# ---------------------------------------------------------------------------
# value / after validation (shared by source and loader-side sanity checks)
# ---------------------------------------------------------------------------
def validate_after(after: Any) -> Any:
    """Validate an ``after`` object.

    * ``None`` means the key was deleted.
    * otherwise it must be an object carrying exactly a ``value`` field whose
      payload is a string or ``None`` (real null: ``{"value": None}``).
    """
    if after is None:
        return None
    if not isinstance(after, dict) or set(after.keys()) != {"value"}:
        raise ProtocolError(f"after must be null or {{'value': ...}}, got {after!r}")
    value = after["value"]
    if value is not None and not isinstance(value, str):
        raise ProtocolError(f"after.value must be a string or null, got {value!r}")
    return after


def value_of(after: Any) -> Any:
    return None if after is None else after["value"]


# ---------------------------------------------------------------------------
# HTTP / JSON
# ---------------------------------------------------------------------------
class ProtocolError(Exception):
    """Raised on malformed protocol data or local invariant violations."""


class RemoteError(Exception):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} from {url}: {body[:200]}")
        self.status = status
        self.body = body
        self.url = url


def http_json(
    method: str,
    url: str,
    payload: Any = None,
    *,
    timeout: float = 10.0,
    retries: int = 5,
) -> Any:
    """JSON-over-HTTP client with retry on transport errors and 5xx.

    Scan responses are allowed to be delayed: timeouts are retried.  4xx
    responses are permanent protocol failures and are not retried.
    """
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            if 500 <= exc.code < 600 and attempt < retries:
                last_exc = RemoteError(exc.code, body, url)
            else:
                raise RemoteError(exc.code, body, url)
        except (urllib.error.URLError, http.client.HTTPException,
                TimeoutError, ConnectionError, json.JSONDecodeError) as exc:
            last_exc = exc
        # simple bounded backoff; delays stay small for tests
        if attempt < retries:
            import time

            time.sleep(min(0.05 * (2 ** attempt), 0.8))
    raise RemoteError(0, f"retries exhausted: {last_exc!r}", url)
