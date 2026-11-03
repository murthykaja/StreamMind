import json
import time

from .db import incident, r

GRACE_MS = 45000
MATCH_SLACK_MS = 15000


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def summarize(values, scale=1.0, digits=2):
    if not values:
        return {"n": 0, "p50": None, "p95": None, "max": None}
    return {"n": len(values), "p50": round(percentile(values, 50) * scale, digits),
            "p95": round(percentile(values, 95) * scale, digits), "max": round(max(values) * scale, digits)}


def matches(alert, truth):
    return (alert["service"] in truth["affected"]
            and alert["window_end"] >= truth["start_ms"]
            and alert["window_start"] <= truth["end_ms"] + MATCH_SLACK_MS)


def compute():
    now = int(time.time() * 1000)
    truths = [json.loads(x) for x in r.lrange("groundtruth", 0, -1)]
    alerts = [json.loads(x) for x in r.lrange("alerts", 0, -1)]
    first_truth = min([t["start_ms"] for t in truths], default=now)
    alerts = [a for a in alerts if a["window_end"] >= first_truth - 30000]
    anomalies = [t for t in truths if t["kind"] == "anomaly"]
    settled = [t for t in anomalies if t["end_ms"] + GRACE_MS < now]

    detected, delays = [], []
    for t in settled:
        hits = [a for a in alerts if matches(a, t)]
        if hits:
            detected.append(t)
            delays.append((min(a["detected_at"] for a in hits) - t["start_ms"]) / 1000)
    missed = [t for t in settled if t not in detected]

    cutoff = now - 30000
    judged = [a for a in alerts if a["detected_at"] < cutoff]
    false_alerts = [a for a in judged if not any(matches(a, t) for t in truths if t["kind"] == "anomaly")]
    distractor_alerts = [a for a in false_alerts
                         if any(matches(a, t) for t in truths if t["kind"] == "distractor")]
    true_alerts = len(judged) - len(false_alerts)

    incidents = [incident(i) for i in r.lrange("incidents", 0, 199)]
    incidents = [i for i in incidents if i and "hypothesis" in i]
    a2h = [i["alert_to_hypothesis_s"] for i in incidents if "alert_to_hypothesis_s" in i]
    rca_total = rca_cat = rca_suspect = 0
    for inc in incidents:
        first = inc["first_alert"]
        gt = next((t for t in anomalies if matches(first, t)), None)
        if not gt:
            continue
        rca_total += 1
        rca_cat += inc["hypothesis"]["category"] == gt["category"]
        rca_suspect += inc["hypothesis"]["suspect_service"] == gt["suspect"]

    stats = {k: int(v) for k, v in r.hgetall("stats").items()}
    fresh_w = [float(x) for x in r.lrange("freshness:windows", 0, -1)]
    engines = {}
    for inc in incidents:
        engines[inc.get("engine", "?")] = engines.get(inc.get("engine", "?"), 0) + 1

    def ratio(a, b):
        return round(a / b, 3) if b else None

    return {
        "generated_at": now,
        "detection": {
            "injected_anomalies_settled": len(settled), "true_positives": len(detected),
            "false_negatives": len(missed), "alerts_judged": len(judged),
            "true_alerts": true_alerts, "false_positive_alerts": len(false_alerts),
            "false_positives_on_distractors": len(distractor_alerts),
            "recall": ratio(len(detected), len(settled)),
            "precision": ratio(true_alerts, len(judged)),
            "detection_delay_s": summarize(delays),
        },
        "freshness_ms": {"alerts": summarize([a["freshness_ms"] for a in alerts], digits=0),
                         "windows": summarize(fresh_w, digits=0)},
        "alert_to_hypothesis_s": summarize(a2h),
        "root_cause": {
            "incidents_scored": rca_total, "category_accuracy": ratio(rca_cat, rca_total),
            "suspect_accuracy": ratio(rca_suspect, rca_total), "engines": engines,
        },
        "pipeline": {
            "events_total": stats.get("events_total", 0),
            "duplicates_dropped": stats.get("duplicates_dropped", 0),
            "late_dropped": stats.get("late_dropped", 0),
            "out_of_order_accepted": stats.get("out_of_order_accepted", 0),
            "windows_closed": stats.get("windows_closed", 0),
            "alerts_emitted": stats.get("alerts_emitted", 0),
        },
    }
