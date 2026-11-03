import json
import os
import threading
import time

from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .db import incident, r, sql
from .metrics import compute

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SERVICES = ["checkout", "payments", "inventory", "search", "auth"]

app = FastAPI(title="StreamMind API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
producer = Producer({"bootstrap.servers": BOOTSTRAP})


def truth_loop():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "api-truth",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe(["ground-truth"])
    while True:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        pipe = r.pipeline()
        pipe.lpush("groundtruth", m.value().decode())
        pipe.ltrim("groundtruth", 0, 499)
        pipe.execute()


@app.on_event("startup")
def start():
    threading.Thread(target=truth_loop, daemon=True).start()


@app.get("/api/health")
def health():
    return {"status": "ok", "time": int(time.time() * 1000)}


@app.get("/api/series")
def series(minutes: int = 6):
    minutes = max(1, min(minutes, 60))
    rows = sql("""
        SELECT service, toUnixTimestamp64Milli(window_start) AS t, requests, errors,
               avg_latency_ms, p95_latency_ms
        FROM streammind.window_metrics
        WHERE window_start > now64(3) - INTERVAL %d MINUTE
        ORDER BY t""" % minutes)
    out = {s: [] for s in SERVICES}
    for row in rows:
        out.setdefault(row["service"], []).append({
            "t": int(row["t"]), "requests": row["requests"], "errors": row["errors"],
            "avg_latency_ms": row["avg_latency_ms"], "p95_latency_ms": row["p95_latency_ms"]})
    return out


@app.get("/api/alerts")
def alerts(limit: int = 50):
    return [json.loads(a) for a in r.lrange("alerts", 0, max(0, min(limit, 500)) - 1)]


@app.get("/api/incidents")
def incidents(limit: int = 30):
    ids = r.lrange("incidents", 0, max(0, min(limit, 200)) - 1)
    return [i for i in (incident(x) for x in ids) if i]


@app.get("/api/incidents/{iid}")
def incident_detail(iid: str):
    inc = incident(iid)
    if not inc:
        raise HTTPException(404, "incident not found")
    return inc


def decide(iid, decision, by):
    inc = incident(iid)
    if not inc:
        raise HTTPException(404, "incident not found")
    if inc.get("status") != "pending_approval":
        raise HTTPException(409, "incident is %s, not awaiting approval" % inc.get("status"))
    r.hset("incident:" + iid, "status", json.dumps("deciding"))
    producer.produce("approvals", json.dumps({"incident_id": iid, "decision": decision, "by": by}).encode())
    producer.flush(3)
    return {"incident_id": iid, "decision": decision}


@app.post("/api/incidents/{iid}/approve")
def approve(iid: str, by: str = "operator"):
    return decide(iid, "approve", by)


@app.post("/api/incidents/{iid}/reject")
def reject(iid: str, by: str = "operator"):
    return decide(iid, "reject", by)


@app.get("/api/ground-truth")
def ground_truth(limit: int = 50):
    return [json.loads(x) for x in r.lrange("groundtruth", 0, max(0, min(limit, 500)) - 1)]


@app.get("/api/metrics")
def metrics():
    return compute()
