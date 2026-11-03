import json
import os
import time

from confluent_kafka import Producer
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict

from . import heuristic, llm, state
from .context import gather

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")})


class S(TypedDict, total=False):
    alert: dict
    incident_id: str
    evidence: list
    hypothesis: dict
    engine: str
    attempts: int
    feedback: str
    decision: dict


def gather_context(s):
    t0 = time.time()
    evidence = gather(s["alert"])
    state.save(s["incident_id"], evidence=evidence, context_ms=int((time.time() - t0) * 1000),
               status="analyzing")
    return {"evidence": evidence, "attempts": 0, "feedback": ""}


def hypothesize(s):
    t0 = time.time()
    engine = "heuristic"
    hyp = None
    if llm.enabled():
        try:
            hyp = llm.analyze(s["alert"], s["evidence"], s.get("feedback", "")).model_dump()
            engine = "llm:" + os.getenv("STREAMMIND_MODEL", "claude-sonnet-5-5")
        except Exception as exc:
            print("[agent] llm failed, using heuristic engine:", exc, flush=True)
            engine = "heuristic-fallback"
    if hyp is None:
        hyp = heuristic.analyze(s["alert"], s["evidence"]).model_dump()
    hyp["analysis_ms"] = int((time.time() - t0) * 1000)
    return {"hypothesis": hyp, "engine": engine, "attempts": s.get("attempts", 0) + 1}


def validate(s):
    known = {e["id"] for e in s["evidence"]}
    hyp = dict(s["hypothesis"])
    valid = [c for c in hyp["citations"] if c in known]
    dropped = len(hyp["citations"]) - len(valid)
    hyp["citations"] = valid
    if hyp["category"] != "unknown" and not valid:
        if s["attempts"] < 2:
            return {"feedback": "no valid citations; cite evidence ids from the list", "hypothesis": hyp}
        hyp["confidence"] = round(hyp["confidence"] * 0.5, 2)
    if dropped:
        hyp["confidence"] = round(hyp["confidence"] * 0.9, 2)
    hyp["citations_validated"] = True
    return {"hypothesis": hyp, "feedback": ""}


def after_validate(s):
    return "retry" if s.get("feedback") else "publish"


def publish(s):
    hyp = s["hypothesis"]
    alert = s["alert"]
    now = int(time.time() * 1000)
    needs_approval = hyp["action"]["type"] != "none"
    state.save(s["incident_id"], hypothesis=hyp, engine=s["engine"], hypothesis_at=now,
               alert_to_hypothesis_s=round((now - alert["detected_at"]) / 1000, 2),
               status="pending_approval" if needs_approval else "no_action")
    producer.produce("hypotheses", json.dumps({"incident_id": s["incident_id"], "service": alert["service"],
                                              "hypothesis": hyp, "engine": s["engine"],
                                              "created_at": now}).encode(), key=alert["service"])
    producer.flush(2)
    print("[agent] %s -> %s (%s, conf %.2f, %.1fs after alert)" % (
        alert["service"], hyp["category"], s["engine"], hyp["confidence"],
        (now - alert["detected_at"]) / 1000), flush=True)
    return {}


def after_publish(s):
    return "approval" if s["hypothesis"]["action"]["type"] != "none" else "done"


def approval(s):
    decision = interrupt({"incident_id": s["incident_id"], "action": s["hypothesis"]["action"]})
    return {"decision": decision}


def execute(s):
    d = s["decision"]
    action = s["hypothesis"]["action"]
    now = int(time.time() * 1000)
    if d["decision"] == "approve":
        result = {"executed": True, "simulated": True, "at": now,
                  "message": "[simulated] %s on %s: %s" % (action["type"], action["target"], action["detail"])}
        state.save(s["incident_id"], status="executed", decided_by=d.get("by", "operator"),
                   decided_at=now, action_result=result)
    else:
        state.save(s["incident_id"], status="rejected", decided_by=d.get("by", "operator"),
                   decided_at=now, action_result={"executed": False, "at": now})
    return {}


def build():
    g = StateGraph(S)
    g.add_node("gather_context", gather_context)
    g.add_node("hypothesize", hypothesize)
    g.add_node("validate", validate)
    g.add_node("publish", publish)
    g.add_node("approval", approval)
    g.add_node("execute", execute)
    g.set_entry_point("gather_context")
    g.add_edge("gather_context", "hypothesize")
    g.add_edge("hypothesize", "validate")
    g.add_conditional_edges("validate", after_validate, {"retry": "hypothesize", "publish": "publish"})
    g.add_conditional_edges("publish", after_publish, {"approval": "approval", "done": END})
    g.add_edge("approval", "execute")
    g.add_edge("execute", END)
    return g.compile(checkpointer=MemorySaver())
