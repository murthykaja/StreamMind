import json
from datetime import datetime, timezone

import httpx
import redis


def fmt_ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class Store:
    def __init__(self, redis_url, clickhouse_url):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.ch = httpx.Client(base_url=clickhouse_url, timeout=10)
        self.stats = {}

    def bump(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def flush_stats(self):
        if not self.stats:
            return
        pipe = self.r.pipeline()
        for k, v in self.stats.items():
            pipe.hincrby("stats", k, v)
        pipe.execute()
        self.stats = {}

    def first_seen(self, event_ids):
        pipe = self.r.pipeline()
        for eid in event_ids:
            pipe.set("dedup:" + eid, 1, nx=True, ex=180)
        return [bool(x) for x in pipe.execute()]

    def push_alert(self, alert):
        pipe = self.r.pipeline()
        pipe.lpush("alerts", json.dumps(alert))
        pipe.ltrim("alerts", 0, 499)
        pipe.execute()

    def push_freshness(self, values):
        if not values:
            return
        pipe = self.r.pipeline()
        pipe.lpush("freshness:windows", *values)
        pipe.ltrim("freshness:windows", 0, 2999)
        pipe.execute()

    def insert_windows(self, rows):
        if not rows:
            return
        body = "\n".join(json.dumps(r) for r in rows)
        try:
            resp = self.ch.post("/", params={"query": "INSERT INTO streammind.window_metrics FORMAT JSONEachRow"},
                                content=body)
            resp.raise_for_status()
        except Exception as exc:
            print("[detector] clickhouse insert failed:", exc, flush=True)
