import os

from langchain_anthropic import ChatAnthropic

from .models import Hypothesis

SYSTEM = """You are an SRE assistant performing root-cause analysis for a real-time anomaly alert.
Use ONLY the numbered evidence provided. Every claim must cite evidence ids (E1, E2, ...).
Choose exactly one category:
- bad_deployment: a recent deploy of the alerted service explains new errors
- dependency_degradation: a downstream dependency is slow/exhausted and slows its callers (suspect is the dependency)
- traffic_surge: legitimate load increase with no fault
- upstream_outage: an upstream service is unreachable and traffic dropped (suspect is the upstream)
- unknown: evidence is insufficient
Ignore unrelated deployments and routine noise. Propose the single safest remediation; a human will approve it
before anything is executed. Use action type none when category is unknown."""

_model = None


def model():
    global _model
    if _model is None:
        llm = ChatAnthropic(model=os.getenv("STREAMMIND_MODEL", "claude-sonnet-5-5"),
                            temperature=0, max_tokens=1500, timeout=60, max_retries=1)
        _model = llm.with_structured_output(Hypothesis)
    return _model


def enabled():
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def analyze(alert, evidence, feedback=""):
    lines = ["Alert: %s %s on service %s (severity %s)" % (
        alert["metric"], alert["direction"], alert["service"], alert["severity"]), "", "Evidence:"]
    lines += ["%s [%s] %s" % (e["id"], e["source"], e["text"]) for e in evidence]
    if feedback:
        lines += ["", "Previous attempt was rejected: " + feedback]
    return model().invoke([("system", SYSTEM), ("human", "\n".join(lines))])
