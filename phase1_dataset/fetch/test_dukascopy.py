#!/usr/bin/env python3
"""
test_dukascopy.py -- self-tests for phase1_dataset/fetch/dukascopy.py.

All tests use synthetic payloads and a fake network. No real data is
needed, so they run anywhere, including CI. Run from the repo root:
    python3 phase1_dataset/fetch/test_dukascopy.py
Output follows the Phase 0 convention: one [PASS]/[FAIL] line per check,
then "TOTAL: N passed, M failed".
"""
import datetime as dt
import json
import lzma
import os
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dukascopy as dk  # noqa: E402

UTC = dt.timezone.utc
results = []


def check(name, cond):
    results.append(bool(cond))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")


def raises(exc, fn):
    try:
        fn()
    except exc:
        return True
    except Exception as e:  # wrong exception type is a failure
        print(f"      (raised {type(e).__name__}: {e})")
        return False
    return False


def enc_ticks(recs):
    return lzma.compress(b"".join(struct.pack(">IIIff", *r) for r in recs), format=lzma.FORMAT_ALONE)


def enc_candles(recs):
    # (s, open, close, low, high, vol) -- the documented order
    return lzma.compress(b"".join(struct.pack(">IIIIIf", *r) for r in recs), format=lzma.FORMAT_ALONE)


H = dt.datetime(2024, 1, 2, 10, tzinfo=UTC)
D = dt.date(2024, 1, 2)

print("URLs")
check("tick URL uses 0-indexed month", dk.tick_url(H).endswith("/EURUSD/2024/00/02/10h_ticks.bi5"))
check("December is month 11", dk.tick_url(dt.datetime(2023, 12, 31, 23, tzinfo=UTC)).endswith("/2023/11/31/23h_ticks.bi5"))
check("candle URL BID", dk.candle_url(D, "BID").endswith("/2024/00/02/BID_candles_min_1.bi5"))
check("naive datetime rejected", raises(ValueError, lambda: dk.tick_url(dt.datetime(2024, 1, 2, 10))))
check("non-UTC offset rejected", raises(ValueError, lambda: dk.tick_url(dt.datetime(2024, 1, 2, 13, tzinfo=dt.timezone(dt.timedelta(hours=3))))))
check("off-the-hour rejected", raises(ValueError, lambda: dk.tick_url(H.replace(minute=5))))
check("bad candle side rejected", raises(ValueError, lambda: dk.candle_url(D, "MID")))

print("Tick decoding")
t = dk.decode_ticks(enc_ticks([(0, 109520, 109510, 1.5, 2.0), (1500, 109521, 109512, 0.7, 0.9)]), H)
check("two ticks decoded", len(t) == 2)
check("prices scaled to 1e-5", abs(t[0].bid - 1.09510) < 1e-12 and abs(t[0].ask - 1.09520) < 1e-12)
check("timestamp offset applied", t[1].ts == H + dt.timedelta(milliseconds=1500))
check("empty payload -> no ticks", dk.decode_ticks(b"", H) == [])
check("crossed quote rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(enc_ticks([(0, 109500, 109510, 1, 1)]), H)))
check("backwards time rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(enc_ticks([(10, 2, 1, 1, 1), (5, 2, 1, 1, 1)]), H)))
check("offset beyond hour rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(enc_ticks([(3_600_000, 2, 1, 1, 1)]), H)))
check("zero price rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(enc_ticks([(0, 0, 0, 1, 1)]), H)))
check("truncated payload rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(lzma.compress(b"x" * 7, format=lzma.FORMAT_ALONE), H)))
check("non-LZMA payload rejected", raises(dk.DecodeError, lambda: dk.decode_ticks(b"not lzma at all", H)))

print("Candle decoding")
c = dk.decode_candles(enc_candles([(0, 109500, 109520, 109490, 109530, 12.5), (60, 109520, 109510, 109505, 109525, 3.0)]), D)
check("two candles decoded", len(c) == 2)
check("open/high/low/close mapped from (o,c,l,h)", (round(c[0].open, 5), round(c[0].high, 5), round(c[0].low, 5), round(c[0].close, 5)) == (1.095, 1.0953, 1.0949, 1.0952))
check("minute timestamps", c[1].ts == dt.datetime(2024, 1, 2, 0, 1, tzinfo=UTC))
# A file whose real order were (o,h,l,c) would usually break the invariant:
wrong = enc_candles([(0, 109500, 109530, 109490, 109520, 1.0)])  # decoded as close=109530 > high=109520
check("wrong field order is caught by the OHLC invariant", raises(dk.DecodeError, lambda: dk.decode_candles(wrong, D)))
check("non-minute offset rejected", raises(dk.DecodeError, lambda: dk.decode_candles(enc_candles([(30, 2, 2, 1, 3, 1)]), D)))
check("duplicate minute rejected", raises(dk.DecodeError, lambda: dk.decode_candles(enc_candles([(60, 2, 2, 1, 3, 1), (60, 2, 2, 1, 3, 1)]), D)))

print("Fetch (fake network)")
sleeps = []
seq = iter([(503, b""), (429, b""), (200, b"payload")])
st, body = dk.fetch("u", opener=lambda u, t: next(seq), sleep=sleeps.append)
check("retries 503/429 then succeeds", (st, body) == (200, b"payload"))
check("exponential backoff used (3 s, 6 s)", sleeps == [3.0, 6.0])
check("404 is a final answer, not an error", dk.fetch("u", opener=lambda u, t: (404, b""), sleep=lambda s: None) == (404, b""))
check("persistent 500 raises FetchError", raises(dk.FetchError, lambda: dk.fetch("u", opener=lambda u, t: (500, b""), retries=2, sleep=lambda s: None)))
def boom(u, t):
    raise OSError("network down")
check("network error raises FetchError after retries", raises(dk.FetchError, lambda: dk.fetch("u", opener=boom, retries=1, sleep=lambda s: None)))

print("Snapshot")
with tempfile.TemporaryDirectory() as tmp:
    snap = dk.Snapshot(tmp)
    u1, u2, u3 = dk.tick_url(H), dk.tick_url(H + dt.timedelta(hours=1)), dk.tick_url(H + dt.timedelta(hours=2))
    responses = {u1: (200, b"AAA"), u2: (404, b""), u3: (200, b"")}
    now = lambda: dt.datetime(2026, 10, 23, 12, tzinfo=UTC)
    counts = dk.download(snap, [u1, u2, u3], opener=lambda u, t: responses[u], sleep=lambda s: None, now=now, log=lambda *a: None)
    check("counts ok/empty/missing", counts == {"ok": 1, "empty": 1, "missing": 1, "unavailable": 0, "skipped_existing": 0, "empty_replaced": 0, "not_attempted": 0, "timing": counts.get("timing")})
    check("an empty 200 is stored as a zero-byte file and marked 'empty'", os.path.getsize(snap.local_path(u3)) == 0)
    check("raw bytes stored at mirrored path", open(snap.local_path(u1), "rb").read() == b"AAA")
    check("404 leaves no file", not os.path.exists(snap.local_path(u2)))
    entries = [json.loads(l) for l in open(snap.manifest_path)]
    check("manifest has one row per URL", len(entries) == 3)
    check("manifest records sha256 of the bytes", entries[0]["sha256"] == dk._sha256(b"AAA"))
    check("manifest records the 404 explicitly", entries[1]["status"] == "missing" and entries[1]["http_status"] == 404)
    d1 = snap.manifest_digest()
    again = dk.download(snap, [u1, u3], opener=lambda u, t: (_ for _ in ()).throw(AssertionError("must not refetch")), sleep=lambda s: None, log=lambda *a: None)
    check("existing files are not re-fetched", again["skipped_existing"] == 2)
    check("revision (changed bytes) raises and keeps the original", raises(dk.RevisionDetected, lambda: snap.record(u1, 200, b"BBB", now())) and open(snap.local_path(u1), "rb").read() == b"AAA")
    check("identical re-fetch accepted", snap.record(u1, 200, b"AAA", now())["status"] == "ok")
    check("manifest digest ignores fetch timestamps", snap.manifest_digest() == d1)
    v = dk.verify_sample(snap, 5, opener=lambda u, t: responses[u], sleep=lambda s: None, now=now)
    check("verify_sample checks every stored ok (non-empty) file", v == {"verified": 1, "of": 1})
    responses[u1] = (200, b"CHANGED")
    check("verify_sample detects a provider revision", raises(dk.RevisionDetected, lambda: dk.verify_sample(snap, 5, opener=lambda u, t: responses[u], sleep=lambda s: None, now=now)))
    check("non-Dukascopy URL refused", raises(ValueError, lambda: snap.local_path("https://example.com/x")))

seq_t = iter([TimeoutError("The read operation timed out"), (200, b"late")])
def slow(u, t):
    r = next(seq_t)
    if isinstance(r, Exception):
        raise r
    return r
check("a read timeout is retried, not fatal (probe finding 2026-10-24)", dk.fetch("u", opener=slow, sleep=lambda s: None) == (200, b"late"))

print("Transient failures and empty answers (probe finding 2026-10-25)")
check("persistent 503 raises by default", raises(dk.FetchError, lambda: dk.fetch("u", opener=lambda u, t: (503, b""), retries=1, sleep=lambda s: None)))
check("persistent 503 -> 'unavailable' (0) when asked", dk.fetch("u", opener=lambda u, t: (503, b""), retries=1, sleep=lambda s: None, give_up_as_unavailable=True) == (0, b""))
mixed = iter([(500, b""), (503, b"")])
check("a mix of 500 and 503 is transient too", dk.fetch("u", opener=lambda u, t: next(mixed), retries=1, sleep=lambda s: None, give_up_as_unavailable=True) == (0, b""))
def tout(u, t):
    raise TimeoutError("timed out")
check("persistent timeouts -> 'unavailable' when asked", dk.fetch("u", opener=tout, retries=1, sleep=lambda s: None, give_up_as_unavailable=True) == (0, b""))
check("HTTP 403 is never retried and always raises", raises(dk.FetchError, lambda: dk.fetch("u", opener=lambda u, t: (403, b""), sleep=lambda s: (_ for _ in ()).throw(AssertionError("no retry")), give_up_as_unavailable=True)))
flip = iter([(503, b""), (200, b"DATA")])
check("503 then 200 with data (seen in the probe) succeeds", dk.fetch("u", opener=lambda u, t: next(flip), sleep=lambda s: None) == (200, b"DATA"))
with tempfile.TemporaryDirectory() as tmp:
    snap = dk.Snapshot(tmp)
    logs = []
    u = dk.tick_url(H)
    c3 = dk.download(snap, [u], opener=lambda u_, t: (503, b""), sleep=lambda s: None, log=logs.append)
    check("download records a never-successful file as 'unavailable'", c3["unavailable"] == 1 and json.loads(open(snap.manifest_path).readline())["status"] == "unavailable")
    check("'unavailable' leaves no file, so the next run retries it", not os.path.exists(snap.local_path(u)))
    check("'unavailable' is logged loudly", any("UNAVAILABLE" in l for l in logs))
    c4 = dk.download(snap, [u], opener=lambda u_, t: (200, b"OK"), sleep=lambda s: None, log=lambda *a: None)
    check("an unavailable file is retried on the next run", c4["ok"] == 1)
    ue = dk.tick_url(H + dt.timedelta(hours=5))
    dk.download(snap, [ue], opener=lambda u_, t: (200, b""), sleep=lambda s: None, log=lambda *a: None)
    c5 = dk.download(snap, [ue], opener=lambda u_, t: (_ for _ in ()).throw(AssertionError("must not refetch")), sleep=lambda s: None, log=lambda *a: None)
    check("without --recheck-empty an empty file is not re-fetched", c5["skipped_existing"] == 1)
    c6 = dk.download(snap, [ue], opener=lambda u_, t: (200, b"REAL"), sleep=lambda s: None, log=lambda *a: None, recheck_empty=True)
    check("--recheck-empty replaces an empty file that now has data, and says so", c6["empty_replaced"] == 1 and open(snap.local_path(ue), "rb").read() == b"REAL")
    last = [json.loads(l) for l in open(snap.manifest_path)][-1]
    check("the replacement is flagged in the manifest", last.get("replaced_empty") is True and last["status"] == "ok")
    check("non-empty data is still never overwritten", raises(dk.RevisionDetected, lambda: snap.record(ue, 200, b"OTHER", dt.datetime(2026, 10, 25, tzinfo=UTC))))

print("Time budget and resume (CI finding 2026-10-25: a 5-hour limit killed pass 1)")
with tempfile.TemporaryDirectory() as tmp:
    snap = dk.Snapshot(tmp)
    urls = [dk.tick_url(H + dt.timedelta(hours=i)) for i in range(6)]
    t = [0.0]
    def tick_opener(u, timeout):
        t[0] += 10.0                      # each request "takes" 10 s of clock
        return 200, b"D" + u.encode()
    logs = []
    c = dk.download(snap, urls, opener=tick_opener, sleep=lambda s: None, log=logs.append,
                    deadline=25.0, clock=lambda: t[0])
    check("stops cleanly once the budget has passed", c["ok"] == 3 and c["not_attempted"] == 3)
    check("says it stopped and that the next run resumes", any("time budget reached" in l for l in logs))
    check("files fetched before the stop are kept", all(os.path.exists(snap.local_path(u)) for u in urls[:3]))
    c2 = dk.download(snap, urls, opener=tick_opener, sleep=lambda s: None, log=lambda *a: None)
    check("the next run resumes: only the remaining files are fetched", c2["skipped_existing"] == 3 and c2["ok"] == 3 and c2["not_attempted"] == 0)
    seen = {}
    def rec(u, timeout):
        seen["timeout"] = timeout
        return 503, b""
    tries = []
    c3 = dk.download(dk.Snapshot(os.path.join(tmp, "b")), [urls[0]], opener=lambda u, to: (tries.append(1), rec(u, to))[1],
                     sleep=lambda s: None, log=lambda *a: None, retries=2, timeout=8.0)
    check("--retries and --timeout reach every request", len(tries) == 3 and seen["timeout"] == 8.0 and c3["unavailable"] == 1)
    rc = dk.main(["--root", os.path.join(tmp, "c"), "--kind", "candles", "--start", "2024-01-01T00:00",
                  "--end", "2024-01-03T00:00", "--budget-min", "0"])
    check("CLI exits 3 (incomplete) when the budget leaves files unattempted", rc == 3)

print("Request timing (finding 2026-10-25)")
with tempfile.TemporaryDirectory() as tmp:
    clk = [0.0]
    seq = iter([(503, b""), (200, b"A"), (200, b"B")])
    def op(u, to):
        clk[0] += 4.0
        return next(seq)
    c = dk.download(dk.Snapshot(tmp), [dk.tick_url(H), dk.tick_url(H + dt.timedelta(hours=1))], opener=op,
                    sleep=lambda s: None, log=lambda *a: None, clock=lambda: clk[0])
    check("timing counts every attempt (3 attempts for 2 files)", "1.50 attempts/file" in c["timing"])
    check("answer time is measured on successful answers only", "median 4.0 s" in c["timing"] and "max 4.0 s" in c["timing"])

print("Concurrent workers (finding 2026-10-25: median answer 19 s)")
import random as _r, time as _t
with tempfile.TemporaryDirectory() as tmp:
    urls = [dk.tick_url(H + dt.timedelta(hours=i)) for i in range(40)]
    def body(u): return b"X" + u.encode()
    flaky = {}
    def op(u, to):
        _t.sleep(0.002)
        n = flaky.get(u, 0); flaky[u] = n + 1
        return (503, b"") if n == 0 and hash(u) % 3 == 0 else (200, body(u))
    s1 = dk.Snapshot(os.path.join(tmp, "seq")); s4 = dk.Snapshot(os.path.join(tmp, "par"))
    c1 = dk.download(s1, urls, opener=lambda u, t: (200, body(u)), sleep=lambda s: None, log=lambda *a: None)
    c4 = dk.download(s4, urls, opener=op, sleep=lambda s: None, log=lambda *a: None, workers=6)
    check("workers=6 fetches every file", c4["ok"] == 40 and c4["unavailable"] == 0)
    check("one manifest row per file, none lost or interleaved", sum(1 for _ in open(s4.manifest_path)) == 40
          and all(json.loads(l) for l in open(s4.manifest_path)))
    check("same content digest as one-at-a-time", s1.manifest_digest() == s4.manifest_digest())
    def explode(u, t):
        if u == urls[7]:
            raise ValueError("unexpected")
        return 200, body(u)
    check("an unexpected error in a worker stops the run (not swallowed)",
          raises(ValueError, lambda: dk.download(dk.Snapshot(os.path.join(tmp, "err")), urls, opener=explode,
                                                 sleep=lambda s: None, log=lambda *a: None, workers=4)))
    check("HTTP 403 in a worker stops the run", raises(dk.FetchError, lambda: dk.download(
          dk.Snapshot(os.path.join(tmp, "403")), urls, opener=lambda u, t: (403, b""), sleep=lambda s: None,
          log=lambda *a: None, workers=4)))
    clk = [0.0]
    def slowop(u, t):
        clk[0] += 10.0
        return 200, body(u)
    cb = dk.download(dk.Snapshot(os.path.join(tmp, "bud")), urls, opener=slowop, sleep=lambda s: None,
                     log=lambda *a: None, workers=4, deadline=95.0, clock=lambda: clk[0])
    check("time budget also stops concurrent workers", cb["not_attempted"] > 0 and cb["ok"] + cb["not_attempted"] == 40)

print("Market week (P4.1), hand-written US DST rule")
UTCd = dt.timezone.utc
check("2024 DST start = 10 Mar 07:00 UTC", dk.us_dst_bounds_utc(2024)[0] == dt.datetime(2024, 3, 10, 7, tzinfo=UTCd))
check("2024 DST end = 3 Nov 06:00 UTC", dk.us_dst_bounds_utc(2024)[1] == dt.datetime(2024, 11, 3, 6, tzinfo=UTCd))
check("2005 (old rule) start = 3 Apr", dk.us_dst_bounds_utc(2005)[0].date() == dt.date(2005, 4, 3))
check("2005 (old rule) end = 30 Oct", dk.us_dst_bounds_utc(2005)[1].date() == dt.date(2005, 10, 30))
check("Friday 20:59 UTC in July is open", dk.is_market_open(dt.datetime(2024, 7, 12, 20, 59, tzinfo=UTCd)))
check("Friday 21:00 UTC in July is closed (17:00 EDT)", not dk.is_market_open(dt.datetime(2024, 7, 12, 21, tzinfo=UTCd)))
check("Friday 21:00 UTC in January is still open (16:00 EST)", dk.is_market_open(dt.datetime(2024, 1, 12, 21, tzinfo=UTCd)))
check("Friday 22:00 UTC in January is closed", not dk.is_market_open(dt.datetime(2024, 1, 12, 22, tzinfo=UTCd)))
check("Saturday is closed", not dk.is_market_open(dt.datetime(2026, 10, 3, 12, tzinfo=UTCd)))
check("Sunday 20:00 UTC in July is closed", not dk.is_market_open(dt.datetime(2024, 7, 14, 20, tzinfo=UTCd)))
check("Sunday 21:00 UTC in July is open", dk.is_market_open(dt.datetime(2024, 7, 14, 21, tzinfo=UTCd)))
check("Sunday 22:00 UTC in January is open", dk.is_market_open(dt.datetime(2024, 1, 14, 22, tzinfo=UTCd)))
check("naive timestamp rejected", raises(ValueError, lambda: dk.is_market_open(dt.datetime(2024, 1, 14, 22))))
try:
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    t = dt.datetime(2003, 1, 1, tzinfo=UTCd)
    mismatches = 0
    n = 0
    while t < dt.datetime(2031, 1, 1, tzinfo=UTCd):
        if dk.new_york_offset_hours(t) != int(t.astimezone(ny).utcoffset().total_seconds() // 3600):
            mismatches += 1
        n += 1
        t += dt.timedelta(hours=1)
    check(f"New York offset matches zoneinfo for every hour 2003-2030 ({n} hours)", mismatches == 0)
except Exception as e:  # zoneinfo/tzdata unavailable on this machine
    print(f"  [SKIP] zoneinfo cross-check unavailable here ({type(e).__name__}); runs in CI")

n_pass = sum(results)
print(f"\nTOTAL: {n_pass} passed, {len(results) - n_pass} failed")
sys.exit(0 if n_pass == len(results) else 1)
