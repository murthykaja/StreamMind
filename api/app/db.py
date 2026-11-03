import json
import os

import httpx
import redis

r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
ch = httpx.Client(base_url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"), timeout=15)


def sql(query):
    resp = ch.post("/", params={"default_format": "JSON"}, content=query)
    resp.raise_for_status()
    return resp.json().get("data", [])


def incident(iid):
    raw = r.hgetall("incident:" + iid)
    if not raw:
        return None
    data = {k: json.loads(v) for k, v in raw.items()}
    data["alerts"] = [json.loads(a) for a in r.lrange("incident:%s:alerts" % iid, 0, -1)]
    return data
