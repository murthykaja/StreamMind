import json
import math
import os
import random
import time
import uuid
from collections import deque

from confluent_kafka import Producer

from .faults import ANOMALY_KINDS, DISTRACTOR_KINDS, build_fault
from .topology import (AUTHORS, BASE_ERROR, BASE_LATENCY, BASE_RATE, DEPENDENCIES,
                       ENDPOINTS, SERVICES)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WARMUP = float(os.getenv("WARMUP_SECONDS", "90"))
GAP_MIN = float(os.getenv("FAULT_INTERVAL_MIN", "40"))
GAP_MAX = float(os.getenv("FAULT_INTERVAL_MAX", "70"))
SEED = os.getenv("SEED") or None
TICK = 0.1

producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "compression.type": "lz4"})
rng = random.Random(int(SEED) if SEED else None)

versions = {s: "v1.%d.0" % rng.randint(10, 30) for s in SERVICES}
delay_queue = []


def send(topic, payload, key=None):
    producer.produce(topic, json.dumps(payload).encode(), key=key)
    producer.poll(0)


def poisson(lam):
    if lam > 30:
        return max(0, int(rng.gauss(lam, math.sqrt(lam))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def bump(version):
    major, minor, patch = version[1:].split(".")
    return "v%s.%d.0" % (major, int(minor) + 1)


def emit_deployment(service, summary, now):
    prev = versions[service]
    versions[service] = bump(prev)
    send("context.deployments", {
        "ts": int(now * 1000), "service": service, "version": versions[service],
        "previous_version": prev, "author": rng.choice(AUTHORS), "change_summary": summary,
    })
    return prev


def log(service, level, message, now, version=None):
    send("context.logs", {"ts": int(now * 1000), "service": service, "level": level,
                          "message": message, "version": version or versions[service]})


def span(service, peer, duration, error, now):
    send("context.spans", {"ts": int(now * 1000), "trace_id": uuid.uuid4().hex[:16],
                           "service": service, "peer": peer,
                           "duration_ms": round(duration, 1), "error": int(error)})


BENIGN_SUMMARIES = ["bump logging library", "update dashboards config", "tune connection timeouts",
                    "add feature flag for banner", "refresh TLS bundle", "minor copy changes"]
NOISE_WARN = ["cache miss ratio above 0.30", "GC pause 85ms", "retrying idempotent request (attempt 2)",
              "slow client detected, closing idle connection"]


def emit_event(service, now, eff, fault_active_dup):
    latency = rng.lognormvariate(math.log(BASE_LATENCY[service] * eff["lat"]), 0.25)
    status = 500 if rng.random() < BASE_ERROR + eff["err"] else 200
    event = {
        "event_id": uuid.uuid4().hex[:20], "service": service, "ts": int(now * 1000),
        "endpoint": rng.choice(ENDPOINTS[service]), "status": status,
        "latency_ms": round(latency, 1), "client": "client-%03d" % rng.randint(1, 200),
    }
    if eff["rate"] > 2:
        if rng.random() < 0.7:
            event["client"] = "campaign-flash-sale"
    body = json.dumps(event).encode()
    if eff["late"]:
        if rng.random() < 0.5:
            delay_queue.append((now + rng.uniform(2.0, 4.0), body))
            return
        if rng.random() < 0.1:
            delay_queue.append((now + 20.0, body))
            return
    producer.produce("events", body, key=service)
    if fault_active_dup:
        producer.produce("events", body, key=service)
    elif rng.random() < 0.004:
        producer.produce("events", body, key=service)
    elif rng.random() < 0.008:
        delay_queue.append((now + rng.uniform(2.0, 4.0), body))
    return


def announce(fault, now):
    anomaly = fault.category != "none"
    send("ground-truth", {
        "id": uuid.uuid4().hex[:12], "kind": "anomaly" if anomaly else "distractor",
        "type": fault.kind, "category": fault.category, "service": fault.service,
        "affected": fault.affected, "suspect": fault.suspect, "action": fault.action,
        "start_ms": int(fault.start * 1000), "end_ms": int(fault.end * 1000),
    })
    print("[generator] %s on %s (%.0fs)" % (fault.kind, fault.service, fault.end - fault.start), flush=True)


def fault_logs(fault, service, now, tick_logs):
    if not fault.active(now):
        return
    if fault.kind == "bad_deployment" and service == fault.service and rng.random() < 0.5 * tick_logs:
        v = versions[service]
        msg = rng.choice([
            "NullPointerException in OrderMapper.map (%s)" % v,
            "unhandled exception in request handler (%s): KeyError 'currency'" % v,
        ])
        log(service, "ERROR", msg, now)
    elif fault.kind == "dependency_degradation":
        dep = fault.params["dependency"]
        if service == dep and rng.random() < 0.5 * tick_logs:
            log(service, "WARN", rng.choice(["connection pool exhausted (max=50)",
                                             "slow query: took 2400ms on primary replica"]), now)
        if service == fault.service and rng.random() < 0.4 * tick_logs:
            log(service, "WARN", "timeout calling %s after 2000ms" % dep, now)
    elif fault.kind == "traffic_surge" and service == fault.service and rng.random() < 0.4 * tick_logs:
        log(service, rng.choice(["INFO", "WARN"]),
            rng.choice(["request rate 4x above baseline, top client campaign-flash-sale (71%)",
                        "autoscaler at max replicas, queue depth growing"]), now)
    elif fault.kind == "upstream_outage":
        if service == fault.service and rng.random() < 0.6 * tick_logs:
            log(service, "ERROR", "auth upstream unreachable: connection refused (auth:8443)", now)
        if service == "auth" and rng.random() < 0.4 * tick_logs:
            log(service, "ERROR", "TLS handshake failure: certificate expired", now)


def main():
    start = time.time()
    next_fault_at = start + WARMUP
    pending = []
    history = deque(maxlen=6)
    next_noise = {s: 0.0 for s in SERVICES}
    anomalies_since_distractor = 0
    print("[generator] started, warmup %.0fs" % WARMUP, flush=True)
    while True:
        tick_start = time.time()
        now = tick_start
        pending = [f for f in pending if f.end > now]

        if now >= next_fault_at:
            roll = rng.random()
            if roll < 0.7 or anomalies_since_distractor < 1:
                candidates = [k for k in ANOMALY_KINDS if k not in list(history)[-2:]] or ANOMALY_KINDS
                kind = rng.choice(candidates)
                anomalies_since_distractor += 1
            else:
                kind = rng.choice(DISTRACTOR_KINDS)
                anomalies_since_distractor = 0
            history.append(kind)
            fault = build_fault(rng, kind, now, versions)
            pending.append(fault)
            announce(fault, now)
            if kind == "bad_deployment":
                emit_deployment(fault.service, "refactor order mapper and currency handling", now)
            next_fault_at = fault.end + rng.uniform(GAP_MIN, GAP_MAX)
            if rng.random() < 0.8:
                other = rng.choice([s for s in SERVICES if s not in fault.affected])
                emit_deployment(other, rng.choice(BENIGN_SUMMARIES), now - rng.uniform(5, 25))

        while delay_queue and delay_queue[0][0] <= now:
            _, body = delay_queue.pop(0)
            producer.produce("events", body)
        delay_queue.sort(key=lambda x: x[0])

        for service in SERVICES:
            eff = {"rate": 1.0, "lat": 1.0, "err": 0.0, "dup": False, "late": False}
            for f in pending:
                e = f.effect(service, now)
                eff["rate"] *= e["rate"]
                eff["lat"] *= e["lat"]
                eff["err"] += e["err"]
                eff["dup"] = eff["dup"] or e["dup"]
                eff["late"] = eff["late"] or e["late"]
            n = poisson(BASE_RATE[service] * eff["rate"] * TICK)
            for _ in range(n):
                emit_event(service, now, eff, eff["dup"])
            for f in pending:
                fault_logs(f, service, now, 1.0)
            if now >= next_noise[service]:
                next_noise[service] = now + rng.uniform(0.7, 1.6)
                if rng.random() < 0.12:
                    log(service, "WARN", rng.choice(NOISE_WARN), now)
                else:
                    log(service, "INFO", "%s %d %dms" % (rng.choice(ENDPOINTS[service]), 200,
                                                         BASE_LATENCY[service]), now)
            for dep in DEPENDENCIES[service]:
                if rng.random() < 0.35:
                    dur = rng.lognormvariate(math.log(BASE_LATENCY[dep] * 0.7), 0.2)
                    err = rng.random() < 0.01
                    for f in pending:
                        if f.active(now):
                            if f.kind == "dependency_degradation" and f.params["dependency"] == dep:
                                dur *= 12
                            if f.kind == "upstream_outage" and service == f.service and dep == "auth":
                                err = True
                                dur = 5.0
                    span(service, dep, dur, err, now)
        producer.poll(0)
        sleep = TICK - (time.time() - tick_start)
        if sleep > 0:
            time.sleep(sleep)


if __name__ == "__main__":
    main()
