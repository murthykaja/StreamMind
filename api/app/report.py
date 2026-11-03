import json

from .metrics import compute


def fmt(v, unit=""):
    return "n/a" if v is None else "%s%s" % (v, unit)


def main():
    m = compute()
    d, f, rc = m["detection"], m["freshness_ms"], m["root_cause"]
    a2h = m["alert_to_hypothesis_s"]
    print("| Metric | Value |")
    print("|---|---|")
    print("| Injected anomalies (settled) | %d |" % d["injected_anomalies_settled"])
    print("| True positives / false negatives | %d / %d |" % (d["true_positives"], d["false_negatives"]))
    print("| Detection recall | %s |" % fmt(d["recall"]))
    print("| Alert precision | %s (%d false alerts, %d on distractors) |" % (
        fmt(d["precision"]), d["false_positive_alerts"], d["false_positives_on_distractors"]))
    print("| Detection delay p50 / p95 | %s / %s s |" % (fmt(d["detection_delay_s"]["p50"]), fmt(d["detection_delay_s"]["p95"])))
    print("| Freshness (event -> alert) p50 / p95 | %s / %s ms |" % (fmt(f["alerts"]["p50"]), fmt(f["alerts"]["p95"])))
    print("| Freshness (event -> window result) p50 / p95 | %s / %s ms |" % (fmt(f["windows"]["p50"]), fmt(f["windows"]["p95"])))
    print("| Alert -> hypothesis p50 / p95 | %s / %s s (n=%d) |" % (fmt(a2h["p50"]), fmt(a2h["p95"]), a2h["n"]))
    print("| Root-cause category accuracy | %s (n=%d) |" % (fmt(rc["category_accuracy"]), rc["incidents_scored"]))
    print("| Root-cause suspect accuracy | %s |" % fmt(rc["suspect_accuracy"]))
    print("| Engines | %s |" % json.dumps(rc["engines"]))
    p = m["pipeline"]
    print("| Events processed | %d (duplicates dropped %d, late dropped %d) |" % (
        p["events_total"], p["duplicates_dropped"], p["late_dropped"]))


if __name__ == "__main__":
    main()
