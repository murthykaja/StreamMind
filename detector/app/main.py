import json
import os
import time
import uuid

from confluent_kafka import Consumer, Producer

from .baseline import Baseline
from .rules import evaluate, severity
from .store import Store, fmt_ts

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WINDOW_MS = int(float(os.getenv("WINDOW_SECONDS", "5")) * 1000)
LATENESS_MS = int(float(os.getenv("ALLOWED_LATENESS_SECONDS", "5")) * 1000)
WARMUP_WINDOWS = int(os.getenv("WARMUP_WINDOWS", "12"))
CONSECUTIVE = int(os.getenv("CONSECUTIVE_WINDOWS", "2"))
RECOVERY = int(os.getenv("RECOVERY_WINDOWS", "3"))

store = Store(os.getenv("REDIS_URL", "redis://redis:6379/0"),
              os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"))
producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5})


class Agg:
    __slots__ = ("count", "errors", "lat_sum", "lats", "last_ts")

    def __init__(self):
        self.count = 0
        self.errors = 0
        self.lat_sum = 0.0
        self.lats = []
        self.last_ts = 0

    def add(self, ev):
        self.count += 1
        if ev["status"] >= 500:
            self.errors += 1
        self.lat_sum += ev["latency_ms"]
        self.lats.append(ev["latency_ms"])
        if ev["ts"] > self.last_ts:
            self.last_ts = ev["ts"]


class Stream:
    def __init__(self):
        self.open = {}
        self.services = set()
        self.max_ts = 0
        self.next_close = None
        self.baselines = {}
        self.streak = {}
        self.calm = {}
        self.active = {}
        self.windows_closed = 0

    def ingest(self, ev):
        ts = ev["ts"]
        wstart = ts - ts % WINDOW_MS
        if self.next_close is None:
            self.next_close = wstart
        if wstart < self.next_close:
            store.bump("late_dropped")
            return
        if ts < self.max_ts:
            store.bump("out_of_order_accepted")
        self.services.add(ev["service"])
        self.open.setdefault(wstart, {}).setdefault(ev["service"], Agg()).add(ev)
        if ts > self.max_ts:
            self.max_ts = ts

    def advance(self):
        if self.next_close is None:
            return
        watermark = self.max_ts - LATENESS_MS
        rows = []
        while self.next_close + WINDOW_MS <= watermark:
            wstart = self.next_close
            bucket = self.open.pop(wstart, {})
            now_ms = int(time.time() * 1000)
            fresh = []
            for service in sorted(self.services):
                agg = bucket.get(service) or Agg()
                rows.append(self.close_window(service, wstart, agg, now_ms))
                if agg.last_ts:
                    fresh.append(now_ms - agg.last_ts)
            store.push_freshness(fresh)
            self.next_close += WINDOW_MS
            self.windows_closed += 1
            store.bump("windows_closed")
        store.insert_windows(rows)

    def close_window(self, service, wstart, agg, now_ms):
        n = agg.count
        wend = wstart + WINDOW_MS
        lats = sorted(agg.lats)
        avg = agg.lat_sum / n if n else 0.0
        p95 = lats[min(len(lats) - 1, int(0.95 * len(lats)))] if lats else 0.0
        values = {
            "request_rate": n / (WINDOW_MS / 1000),
            "error_rate": (agg.errors / n) if n else 0.0,
        }
        if n:
            values["latency"] = avg
        for metric, value in values.items():
            self.score(service, metric, value, wstart, wend, agg.last_ts, now_ms)
        return {"service": service, "window_start": fmt_ts(wstart), "window_end": fmt_ts(wend),
                "requests": n, "errors": agg.errors, "avg_latency_ms": round(avg, 2),
                "p95_latency_ms": round(p95, 2)}

    def score(self, service, metric, value, wstart, wend, last_ts, now_ms):
        key = (service, metric)
        base = self.baselines.setdefault(key, Baseline())
        if base.n < WARMUP_WINDOWS:
            base.update(value)
            return
        direction, z = evaluate(metric, value, base)
        if direction:
            self.calm[key] = 0
            self.streak[key] = self.streak.get(key, 0) + 1
            if self.streak[key] >= CONSECUTIVE and key not in self.active:
                self.fire(service, metric, direction, value, base, z, wstart, wend, last_ts, now_ms)
                self.active[key] = now_ms
            return
        self.streak[key] = 0
        if key in self.active:
            self.calm[key] = self.calm.get(key, 0) + 1
            if self.calm[key] >= RECOVERY:
                del self.active[key]
                self.calm[key] = 0
            return
        base.update(value)

    def fire(self, service, metric, direction, value, base, z, wstart, wend, last_ts, now_ms):
        alert = {
            "alert_id": uuid.uuid4().hex[:12], "service": service, "metric": metric,
            "direction": direction, "severity": severity(metric, direction, value, base.mean),
            "observed": round(value, 4), "expected": round(base.mean, 4), "zscore": round(z, 2),
            "window_start": wstart, "window_end": wend, "last_event_ts": last_ts,
            "detected_at": now_ms, "freshness_ms": now_ms - (last_ts or wend),
        }
        producer.produce("alerts", json.dumps(alert).encode(), key=service)
        producer.poll(0)
        store.push_alert(alert)
        store.bump("alerts_emitted")
        print("[detector] ALERT %s %s %s observed=%.3f expected=%.3f z=%.1f" % (
            service, metric, direction, value, base.mean, z), flush=True)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP, "group.id": "detector", "auto.offset.reset": "latest",
        "enable.auto.commit": True, "fetch.wait.max.ms": 100,
    })
    consumer.subscribe(["events"])
    stream = Stream()
    print("[detector] consuming events window=%dms lateness=%dms" % (WINDOW_MS, LATENESS_MS), flush=True)
    last_flush = time.time()
    while True:
        msgs = consumer.consume(num_messages=500, timeout=0.3)
        events = []
        for m in msgs:
            if m.error():
                continue
            try:
                events.append(json.loads(m.value()))
            except ValueError:
                store.bump("malformed")
        if events:
            fresh = store.first_seen([e["event_id"] for e in events])
            for ev, is_new in zip(events, fresh):
                store.bump("events_total")
                if not is_new:
                    store.bump("duplicates_dropped")
                    continue
                stream.ingest(ev)
        stream.advance()
        if time.time() - last_flush > 1:
            store.flush_stats()
            last_flush = time.time()


if __name__ == "__main__":
    main()
