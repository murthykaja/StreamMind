import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer
from langgraph.types import Command

from . import llm, state
from .graph import build

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
graph = build()
pool = ThreadPoolExecutor(max_workers=4)


def run_alert(incident_id, alert):
    cfg = {"configurable": {"thread_id": incident_id}}
    try:
        graph.invoke({"alert": alert, "incident_id": incident_id}, cfg)
    except Exception as exc:
        print("[agent] incident %s failed: %r" % (incident_id, exc), flush=True)
        state.save(incident_id, status="failed", error=str(exc))


def resume(incident_id, decision):
    cfg = {"configurable": {"thread_id": incident_id}}
    for _ in range(10):
        snap = graph.get_state(cfg)
        if snap.next:
            break
        time.sleep(0.5)
    else:
        print("[agent] nothing to resume for", incident_id, flush=True)
        return
    try:
        graph.invoke(Command(resume=decision), cfg)
        print("[agent] incident %s %s by %s" % (incident_id, decision["decision"], decision.get("by")), flush=True)
    except Exception as exc:
        print("[agent] resume failed %r" % exc, flush=True)


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "agent",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe(["alerts", "approvals"])
    print("[agent] ready, mode=%s" % ("llm" if llm.enabled() else "heuristic (set ANTHROPIC_API_KEY for LLM)"),
          flush=True)
    while True:
        m = consumer.poll(0.5)
        if m is None or m.error():
            continue
        payload = json.loads(m.value())
        if m.topic() == "alerts":
            existing = state.open_incident(payload["service"])
            if existing:
                state.attach(existing, payload)
                continue
            incident_id = uuid.uuid4().hex[:10]
            state.create(incident_id, payload)
            pool.submit(run_alert, incident_id, payload)
        else:
            pool.submit(resume, payload["incident_id"], payload)


if __name__ == "__main__":
    main()
