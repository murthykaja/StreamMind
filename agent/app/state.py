import json
import os
import time

import redis

r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)


def save(incident_id, **fields):
    r.hset("incident:" + incident_id, mapping={k: json.dumps(v) for k, v in fields.items()})


def load(incident_id):
    raw = r.hgetall("incident:" + incident_id)
    return {k: json.loads(v) for k, v in raw.items()}


def create(incident_id, alert):
    save(incident_id, id=incident_id, service=alert["service"], created_at=int(time.time() * 1000),
         status="analyzing", first_alert=alert, engine="pending")
    r.rpush("incident:%s:alerts" % incident_id, json.dumps(alert))
    r.lpush("incidents", incident_id)
    r.ltrim("incidents", 0, 199)
    r.set("open_incident:" + alert["service"], incident_id, ex=90)


def open_incident(service):
    iid = r.get("open_incident:" + service)
    if iid:
        r.expire("open_incident:" + service, 90)
    return iid


def attach(incident_id, alert):
    r.rpush("incident:%s:alerts" % incident_id, json.dumps(alert))
