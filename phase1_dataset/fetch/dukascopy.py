#!/usr/bin/env python3
"""
dukascopy.py -- raw-data acquisition for Phase 1 (P3.2, P3.6, P3.7).

Scope (deliberately narrow): fetch Dukascopy raw files exactly as served,
store them immutably with a hash manifest, and decode them with
self-checking decoders. It contains NO bar building, NO calendar, NO
labels and NO research logic. Those belong to later Phase 1 steps and
stay unbuilt until their protocol packages are frozen.

Standard library only, so it runs unchanged on GitHub Actions runners and
on a Windows PC.

Fail-loud rules (P3.6, PROJECT_SPEC.md "fail visibly, not silently"):
  * Any transport failure after the allowed retries raises FetchError.
    Nothing is skipped, filled or retried silently.
  * HTTP 404 is recorded in the manifest as status "missing" with its URL.
  * HTTP 200 with an empty body is recorded as "empty" (closed hours usually
    answer this way). It is stored as a zero-byte file so it can be audited and
    re-checked (--recheck-empty). It is never silently equated with "no trading".
  * Transient failures (503/429/5xx/timeouts) are retried; a file that never
    succeeds is recorded as "unavailable" and the run exits non-zero.
  * A payload whose decompressed length is not a whole number of records,
    or whose records violate basic invariants (time order, ask >= bid,
    low <= open/close <= high, prices > 0), raises DecodeError.
  * An existing raw file is never overwritten. A re-fetch whose bytes
    differ from the stored file raises RevisionDetected (P3.6 revisions).

Record formats (big-endian), as documented by the community decoders and
TO BE CONFIRMED on real data by research_protocol/probes/p3_source_probe.py
(the 'crosscheck' mode compares candles against ticks):
  ticks   (20 bytes): uint32 ms_offset, uint32 ask, uint32 bid, float32 ask_vol, float32 bid_vol
  candles (24 bytes): uint32 s_offset, uint32 open, uint32 close, uint32 low, uint32 high, float32 volume
Prices are integer points; EURUSD point = 1e-5.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import lzma
import os
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

UTC = dt.timezone.utc
BASE = "https://datafeed.dukascopy.com/datafeed"
POINT = {"EURUSD": 1e-5}
TICK_REC = struct.Struct(">IIIff")
CANDLE_REC = struct.Struct(">IIIIIf")
USER_AGENT = "eurusd-restart-fetch/1.0"


class FetchError(RuntimeError):
    """Transport failure after all allowed retries."""


class DecodeError(ValueError):
    """Payload is malformed or violates a record invariant."""


class RevisionDetected(RuntimeError):
    """A re-fetched file differs from the immutable stored copy."""


@dataclass(frozen=True)
class Tick:
    ts: dt.datetime
    bid: float
    ask: float
    bid_vol: float
    ask_vol: float


@dataclass(frozen=True)
class Candle:
    ts: dt.datetime          # minute OPEN time, as served by the provider
    open: float
    high: float
    low: float
    close: float
    volume: float


# ---------------------------------------------------------------- URLs
def _require_utc_hour(t: dt.datetime) -> None:
    if t.tzinfo is None or t.utcoffset() != dt.timedelta(0):
        raise ValueError(f"timestamp must be timezone-aware UTC, got {t!r}")
    if (t.minute, t.second, t.microsecond) != (0, 0, 0):
        raise ValueError(f"timestamp must be on the hour, got {t.isoformat()}")


def tick_url(hour_start: dt.datetime, symbol: str = "EURUSD") -> str:
    _require_utc_hour(hour_start)
    h = hour_start
    # Dukascopy months are 0-indexed in the path.
    return f"{BASE}/{symbol}/{h.year:04d}/{h.month - 1:02d}/{h.day:02d}/{h.hour:02d}h_ticks.bi5"


def candle_url(day: dt.date, side: str, symbol: str = "EURUSD") -> str:
    if side not in ("BID", "ASK"):
        raise ValueError("side must be 'BID' or 'ASK'")
    return f"{BASE}/{symbol}/{day.year:04d}/{day.month - 1:02d}/{day.day:02d}/{side}_candles_min_1.bi5"


# ------------------------------------------------------------ decoding
def _decompress(raw: bytes) -> bytes:
    if not raw:
        return b""
    try:
        return lzma.decompress(raw)
    except lzma.LZMAError as e:
        raise DecodeError(f"payload is not valid LZMA: {e}") from e


def decode_ticks(raw: bytes, hour_start: dt.datetime, symbol: str = "EURUSD") -> list[Tick]:
    _require_utc_hour(hour_start)
    data = _decompress(raw)
    if len(data) % TICK_REC.size:
        raise DecodeError(f"tick payload length {len(data)} is not a multiple of {TICK_REC.size}")
    pt = POINT[symbol]
    out: list[Tick] = []
    prev_ms = -1
    for ms, ask, bid, ask_vol, bid_vol in TICK_REC.iter_unpack(data):
        if ms < prev_ms:
            raise DecodeError(f"tick times go backwards ({ms} ms after {prev_ms} ms)")
        if ms >= 3_600_000:
            raise DecodeError(f"tick offset {ms} ms lies outside its hour")
        if bid <= 0 or ask <= 0:
            raise DecodeError("non-positive price")
        if ask < bid:
            raise DecodeError(f"crossed quote at +{ms} ms: ask {ask} < bid {bid}")
        prev_ms = ms
        out.append(Tick(hour_start + dt.timedelta(milliseconds=ms), bid * pt, ask * pt, bid_vol, ask_vol))
    return out


def decode_candles(raw: bytes, day: dt.date, symbol: str = "EURUSD") -> list[Candle]:
    data = _decompress(raw)
    if len(data) % CANDLE_REC.size:
        raise DecodeError(f"candle payload length {len(data)} is not a multiple of {CANDLE_REC.size}")
    pt = POINT[symbol]
    day0 = dt.datetime(day.year, day.month, day.day, tzinfo=UTC)
    out: list[Candle] = []
    prev = -1
    for s, o, c, lo, hi, vol in CANDLE_REC.iter_unpack(data):
        if s <= prev:
            raise DecodeError(f"candle times not strictly increasing ({s}s after {prev}s)")
        if s >= 86_400 or s % 60:
            raise DecodeError(f"candle offset {s}s is not a minute inside the day")
        if min(o, c, lo, hi) <= 0:
            raise DecodeError("non-positive price")
        # This check is what exposes a wrong field-order assumption.
        if not (lo <= o <= hi and lo <= c <= hi):
            raise DecodeError(f"OHLC invariant violated at +{s}s: o={o} h={hi} l={lo} c={c}")
        prev = s
        out.append(Candle(day0 + dt.timedelta(seconds=s), o * pt, hi * pt, lo * pt, c * pt, vol))
    return out


# ------------------------------------------------------- market week (P4.1)
def _nth_sunday(year: int, month: int, n: int) -> dt.date:
    d = dt.date(year, month, 1)
    d += dt.timedelta(days=(6 - d.weekday()) % 7)        # first Sunday
    return d + dt.timedelta(weeks=n - 1)


def _last_sunday(year: int, month: int) -> dt.date:
    nxt = dt.date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() + 1) % 7)


def us_dst_bounds_utc(year: int) -> tuple[dt.datetime, dt.datetime]:
    """US Eastern daylight-time start/end as UTC instants (2:00 local).
    2007+: 2nd Sunday of March to 1st Sunday of November. 1987-2006: 1st
    Sunday of April to last Sunday of October. Implemented by hand so the
    module needs no tzdata (Windows Python ships without it); the tests check
    it against zoneinfo where zoneinfo is available."""
    if year >= 2007:
        start, end = _nth_sunday(year, 3, 2), _nth_sunday(year, 11, 1)
    elif year >= 1987:
        start, end = _nth_sunday(year, 4, 1), _last_sunday(year, 10)
    else:
        raise ValueError("US DST rules before 1987 are not implemented")
    return (dt.datetime(start.year, start.month, start.day, 7, tzinfo=UTC),   # 02:00 EST
            dt.datetime(end.year, end.month, end.day, 6, tzinfo=UTC))         # 02:00 EDT


def new_york_offset_hours(t: dt.datetime) -> int:
    start, end = us_dst_bounds_utc(t.year)
    return -4 if start <= t < end else -5


def is_market_open(t: dt.datetime) -> bool:
    """Spot-FX week: Sunday 17:00 to Friday 17:00 America/New_York (P4.1).
    Holidays are NOT modelled here. They are handled by the P4.3 calendar, and
    whatever the provider serves on those days is recorded as-is."""
    if t.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    t = t.astimezone(UTC)
    local = (t + dt.timedelta(hours=new_york_offset_hours(t))).replace(tzinfo=None)
    wd, hr = local.weekday(), local.hour          # Monday=0 ... Sunday=6
    if wd == 5:
        return False                              # Saturday
    if wd == 4:
        return hr < 17                            # Friday until 17:00
    if wd == 6:
        return hr >= 17                           # Sunday from 17:00
    return True


# --------------------------------------------------------------- fetch
Opener = Callable[[str, float], tuple[int, bytes]]


def _default_opener(url: str, timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""


TRANSIENT = "unavailable"


def fetch(url: str, opener: Opener = _default_opener, retries: int = 6,
          backoff_s: float = 3.0, timeout: float = 30.0,
          sleep: Callable[[float], None] = time.sleep,
          give_up_as_unavailable: bool = False) -> tuple[int, bytes]:
    """Return (status, bytes). 200 and 404 are final answers. Transient failures
    (HTTP 429/5xx, timeouts, connection errors) are retried with exponential
    backoff: 3, 6, 12, 24, 48, 96 s by default.

    Probe evidence, 2026-10-25 (status-map, 27 single requests from GitHub
    Actions): 503s and timeouts hit OPEN and closed hours alike, and the same
    hour could return 503 and then 200 with data a few seconds later. So 503
    means "server busy", never "market closed". Closed hours normally answer
    200 with an empty body. (This corrects a 2026-10-24 reading, based on a
    single sample, that 503 marked closed hours.)

    If every attempt fails transiently:
      give_up_as_unavailable=False -> raise FetchError
      give_up_as_unavailable=True  -> return (0, b"") so the caller records the
                                      file as 'unavailable' and retries it later.
    Any other HTTP status (e.g. 403) always raises FetchError."""
    last = None
    for attempt in range(retries + 1):
        try:
            status, body = opener(url, timeout)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            status, body, last = None, b"", repr(e)
        if status in (200, 404):
            return status, body
        if status is not None and not (status == 429 or 500 <= status <= 599):
            raise FetchError(f"{url}: non-retryable HTTP {status}")
        last = last if status is None else f"HTTP {status}"
        if attempt < retries:
            sleep(backoff_s * (2 ** attempt))
    if give_up_as_unavailable:
        return 0, b""
    raise FetchError(f"{url}: failed after {retries + 1} attempts ({last})")


# ------------------------------------------------------------ snapshot
def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


class Snapshot:
    """Immutable raw store: <root>/<path-after-datafeed> plus manifest.jsonl."""

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self.manifest_path = os.path.join(self.root, "manifest.jsonl")

    def local_path(self, url: str) -> str:
        if not url.startswith(BASE + "/"):
            raise ValueError(f"not a Dukascopy datafeed URL: {url}")
        rel = url[len(BASE) + 1:]
        return os.path.join(self.root, *rel.split("/"))

    def record(self, url: str, status: int, body: bytes, fetched_at: dt.datetime) -> dict:
        path = self.local_path(url)
        if status == 200:
            st_name = "ok" if body else "empty"
        else:
            st_name = {404: "missing", 0: TRANSIENT}[status]
        entry = {
            "url": url,
            "status": st_name,
            "http_status": status,
            "bytes": len(body),
            "sha256": _sha256(body) if status == 200 else None,
            "fetched_at_utc": fetched_at.astimezone(UTC).isoformat(),
        }
        if status == 200:
            if os.path.exists(path):
                with open(path, "rb") as f:
                    existing = f.read()
                if existing == b"" and body:
                    # A stored EMPTY answer later served with data: the empty answer was a
                    # fetch failure, not provider data. Replace it, and say so.
                    entry["replaced_empty"] = True
                    with open(path + ".part", "wb") as f:
                        f.write(body)
                    os.replace(path + ".part", path)
                elif existing != body:
                    raise RevisionDetected(
                        f"{url}: stored sha256 {_sha256(existing)} differs from "
                        f"re-fetched sha256 {entry['sha256']}. The stored file is kept; "
                        "record this revision and create a new dataset version (P3.6).")
            else:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = path + ".part"
                with open(tmp, "wb") as f:
                    f.write(body)
                os.replace(tmp, path)
        with open(self.manifest_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, sort_keys=True) + "\n")
        return entry

    def manifest_digest(self) -> str:
        """Hash of the latest entry per URL, independent of fetch order. Feeds
        P9.2's dataset_id. Timestamps are excluded so that re-runs over the
        same content give the same digest."""
        latest: dict[str, dict] = {}
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, encoding="utf-8") as f:
                for line in f:
                    e = json.loads(line)
                    latest[e["url"]] = e
        canon = [(u, latest[u]["status"], latest[u]["sha256"]) for u in sorted(latest)]
        return _sha256(json.dumps(canon).encode())


def hours_between(start: dt.datetime, end: dt.datetime) -> Iterable[dt.datetime]:
    _require_utc_hour(start)
    _require_utc_hour(end)
    t = start
    while t < end:
        yield t
        t += dt.timedelta(hours=1)


def download(snapshot: Snapshot, urls: Iterable[str], opener: Opener = _default_opener,
             pause_s: float = 0.5, sleep: Callable[[float], None] = time.sleep,
             now: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC),
             log=print, recheck_empty: bool = False, retries: int = 6, timeout: float = 30.0,
             deadline: Optional[float] = None, clock: Callable[[], float] = time.monotonic,
             progress_every: int = 50, workers: int = 1) -> dict:
    """Fetch every URL not already stored. With recheck_empty=True, files stored
    empty are fetched again, because an empty 200 can be a transient failure.
    The answer is recorded, and an empty file that now has data is replaced.

    deadline (a clock() value): stop cleanly before starting a file once it has
    passed. Everything already fetched is on disk with its manifest row, so the
    next run resumes where this one stopped (finding 2026-10-25: a 5-hour CI
    limit killed a run mid-pass and nothing was kept). counts['not_attempted']
    says how many files were left for the next run."""
    counts = {"ok": 0, "empty": 0, "missing": 0, TRANSIENT: 0, "skipped_existing": 0,
              "empty_replaced": 0, "not_attempted": 0}
    t0 = clock()
    stopped = False
    # Request timing (finding 2026-10-25: with a 10 s timeout only 17% of files succeeded,
    # versus ~97% at 30 s, so the server is often slow rather than failing). Every attempt
    # is timed; the progress line reports how long successful answers took.
    ok_secs: list[float] = []
    attempts = [0]
    lock = threading.Lock()
    def timed(u, to):
        with lock:
            attempts[0] += 1
        a = clock()
        st, b = opener(u, to)          # exceptions propagate to fetch(), which retries them
        if st in (200, 404):
            with lock:
                ok_secs.append(clock() - a)
        return st, b
    def one(url):
        nonlocal stopped
        with lock:
            if stopped or (deadline is not None and clock() >= deadline):
                if not stopped:
                    log("  time budget reached: stopping cleanly; the next run resumes from here")
                stopped = True
                counts["not_attempted"] += 1
                return
        status, body = fetch(url, opener=timed, sleep=sleep, give_up_as_unavailable=True,
                             retries=retries, timeout=timeout)
        with lock:                       # one writer at a time: files, manifest, counts, log
            e = snapshot.record(url, status, body, now())
            counts[e["status"]] += 1
            counts["empty_replaced"] += bool(e.get("replaced_empty"))
            if e["status"] == TRANSIENT:
                log(f"  UNAVAILABLE after all retries (will be retried next run): {url}")
            done = counts["ok"] + counts["empty"] + counts["missing"] + counts[TRANSIENT]
            if done % progress_every == 0:
                log(f"  progress: {counts}  {_timing(clock() - t0, done, attempts[0], ok_secs)}  workers={workers}")
        sleep(pause_s)
    todo = []
    for url in urls:
        path = snapshot.local_path(url)
        if os.path.exists(path) and not (recheck_empty and os.path.getsize(path) == 0):
            counts["skipped_existing"] += 1
        else:
            todo.append(url)
    if workers <= 1:
        for url in todo:
            one(url)
    else:
        # Concurrent requests (finding 2026-10-25: answers take a median 19 s, so one request
        # at a time mostly waits). Any exception in a worker (e.g. RevisionDetected) is
        # re-raised here, so nothing fails silently.
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            for f in [ex.submit(one, u) for u in todo]:
                f.result()
    counts["timing"] = _timing(clock() - t0, max(1, counts["ok"] + counts["empty"] + counts["missing"] + counts[TRANSIENT]),
                               attempts[0], ok_secs)
    return counts


def _timing(elapsed: float, files: int, attempts: int, ok_secs: list) -> str:
    q = sorted(ok_secs)
    pct = (lambda f: f"{q[min(len(q) - 1, int(f * len(q)))]:.1f}") if q else (lambda f: "-")
    return (f"avg {elapsed / max(1, files):.1f} s/file, {attempts / max(1, files):.2f} attempts/file, "
            f"answer time median {pct(0.5)} s / p90 {pct(0.9)} s / max {pct(1.0)} s")


def verify_sample(snapshot: Snapshot, n: int, opener: Opener = _default_opener,
                  seed: int = 0, sleep: Callable[[float], None] = time.sleep,
                  now: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC)) -> dict:
    """Re-fetch a random sample of stored 'ok' files and compare bytes (P3.6
    revision detection). Raises RevisionDetected on the first mismatch."""
    import random
    ok_urls = []
    if os.path.exists(snapshot.manifest_path):
        with open(snapshot.manifest_path, encoding="utf-8") as f:
            ok_urls = sorted({json.loads(l)["url"] for l in f if json.loads(l)["status"] == "ok"})
    sample = random.Random(seed).sample(ok_urls, min(n, len(ok_urls)))
    for url in sample:
        status, body = fetch(url, opener=opener, sleep=sleep)
        if status != 200 or not body:
            raise RevisionDetected(f"{url}: previously ok with data, now HTTP {status} with {len(body)} bytes")
        snapshot.record(url, status, body, now())   # raises on any byte difference
    return {"verified": len(sample), "of": len(ok_urls)}


# ----------------------------------------------------------------- CLI
def _parse_utc(s: str) -> dt.datetime:
    t = dt.datetime.fromisoformat(s)
    return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fetch Dukascopy raw files into an immutable snapshot.")
    p.add_argument("--root", required=True, help="snapshot directory")
    p.add_argument("--kind", choices=["ticks", "candles"], required=True)
    p.add_argument("--start", required=True, help="UTC start, e.g. 2012-01-01T00:00")
    p.add_argument("--end", required=True, help="UTC end (exclusive)")
    p.add_argument("--weekdays-only", action="store_true",
                   help="skip Saturdays (no trading). Sunday is kept for the weekly open.")
    p.add_argument("--market-hours-only", action="store_true",
                   help="ticks: request only hours inside the Sun 17:00-Fri 17:00 New York week (recommended)")
    p.add_argument("--allow-unavailable", action="store_true",
                   help="exit 0 even if some files stayed unavailable after all retries (still recorded in the manifest)")
    p.add_argument("--recheck-empty", action="store_true",
                   help="re-fetch files previously stored empty (an empty answer can be a transient failure)")
    p.add_argument("--retries", type=int, default=6, help="retries per file after the first attempt")
    p.add_argument("--timeout", type=float, default=30.0, help="seconds per request before it counts as a timeout")
    p.add_argument("--workers", type=int, default=1,
                   help="files fetched at the same time (default 1). Each worker waits --pause between its requests")
    p.add_argument("--budget-min", type=float, default=None,
                   help="stop cleanly after this many minutes; the next run resumes (exit 3 if files were left)")
    p.add_argument("--verify-sample", type=int, default=0,
                   help="after downloading, re-fetch N stored files and compare bytes")
    a = p.parse_args(argv)
    start, end = _parse_utc(a.start), _parse_utc(a.end)
    # Never request the future, or the still-forming current hour/day: those can never succeed
    # and would be recorded as 'unavailable'. Candles need the whole day to be over.
    now = dt.datetime.now(UTC)
    limit = (now.replace(hour=0, minute=0, second=0, microsecond=0) if a.kind == "candles"
             else now.replace(minute=0, second=0, microsecond=0))
    if end > limit:
        print(f"note: --end {end.isoformat()} is in the future; clamped to {limit.isoformat()}")
        end = limit
    snap = Snapshot(a.root)
    skipped_closed = 0
    if a.kind == "ticks":
        all_hours = list(hours_between(start, end))
        if a.market_hours_only:
            open_hours = [h for h in all_hours if is_market_open(h)]
            skipped_closed = len(all_hours) - len(open_hours)
        else:
            open_hours = [h for h in all_hours if not (a.weekdays_only and h.weekday() == 5)]
        urls = (tick_url(h) for h in open_hours)
    else:
        days = []
        d = start.date()
        while d < end.date():
            if not (a.weekdays_only and d.weekday() == 5):
                days.append(d)
            d += dt.timedelta(days=1)
        urls = (candle_url(d, side) for d in days for side in ("BID", "ASK"))
    deadline = None if a.budget_min is None else time.monotonic() + 60 * a.budget_min
    counts = download(snap, urls, recheck_empty=a.recheck_empty, retries=a.retries,
                      timeout=a.timeout, deadline=deadline, workers=a.workers)
    counts["skipped_closed"] = skipped_closed
    print(f"done: {counts}  manifest_digest={snap.manifest_digest()}")
    if a.verify_sample:
        print(f"verify: {verify_sample(snap, a.verify_sample)}")
    if counts["not_attempted"]:
        print(f"INCOMPLETE: time budget used; {counts['not_attempted']} file(s) not attempted yet. "
              "Re-run to resume.")
        return 3
    if counts[TRANSIENT] and not a.allow_unavailable:
        print(f"FAIL: {counts[TRANSIENT]} file(s) still unavailable after all retries. They are listed in the "
              "manifest. Re-run the same command to retry only those files.")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
