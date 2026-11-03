import re
import time

from .clickhouse import query

DEPENDENCIES = {
    "checkout": ["payments", "inventory", "auth"],
    "payments": ["auth"],
    "inventory": [],
    "search": ["inventory"],
    "auth": [],
}

SAFE = re.compile(r"^[a-z0-9_-]+$")


def ms(x):
    return "fromUnixTimestamp64Milli(%d)" % int(x)


def num(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def gather(alert):
    service = alert["service"]
    if not SAFE.match(service):
        raise ValueError("bad service name")
    ws, we = alert["window_start"], alert["window_end"]
    time.sleep(1.5)
    evidence = []

    def add(source, text, **data):
        evidence.append({"id": "E%d" % (len(evidence) + 1), "source": source, "text": text, "data": data})

    add("metric", "%s %s on %s: observed %.3f vs expected %.3f (z=%.1f) in window ending %s UTC" % (
        alert["metric"], alert["direction"], service, alert["observed"], alert["expected"],
        alert["zscore"], time.strftime("%H:%M:%S", time.gmtime(we / 1000))),
        service=service, metric=alert["metric"], direction=alert["direction"])

    rows = query("""
        SELECT service,
          avgIf(requests, window_start >= {a} AND window_start <= {c}) AS req_cur,
          avgIf(requests, window_start < {b}) AS req_base,
          sumIf(errors, window_start >= {a} AND window_start <= {c}) /
            greatest(sumIf(requests, window_start >= {a} AND window_start <= {c}), 1) AS err_cur,
          sumIf(errors, window_start < {b}) / greatest(sumIf(requests, window_start < {b}), 1) AS err_base,
          avgIf(avg_latency_ms, window_start >= {a} AND window_start <= {c} AND requests > 0) AS lat_cur,
          avgIf(avg_latency_ms, window_start < {b} AND requests > 0) AS lat_base
        FROM streammind.window_metrics
        WHERE window_start >= {lo} AND window_start <= {c}
        GROUP BY service ORDER BY service
    """.format(a=ms(ws), b=ms(ws - 15000), c=ms(we + 5000), lo=ms(ws - 150000)))
    calm = []
    for r in rows:
        rc, rb = num(r["req_cur"]), num(r["req_base"])
        lc, lb = num(r["lat_cur"]), num(r["lat_base"])
        rate_ratio = rc / rb if rb else 1.0
        lat_ratio = lc / lb if lb else 1.0
        err_cur, err_base = num(r["err_cur"]), num(r["err_base"])
        notable = rate_ratio > 1.8 or rate_ratio < 0.5 or lat_ratio > 2 or err_cur > 0.1
        if notable or r["service"] == service:
            add("metric", "%s: request rate %.1fx baseline, error rate %.1f%% (baseline %.1f%%), "
                "mean latency %.1fx baseline (%.0fms vs %.0fms)" % (
                    r["service"], rate_ratio, err_cur * 100, err_base * 100, lat_ratio, lc, lb),
                service=r["service"], rate_ratio=rate_ratio, lat_ratio=lat_ratio,
                err_cur=err_cur, err_base=err_base)
        else:
            calm.append(r["service"])
    if calm:
        add("metric", "Within normal range: %s" % ", ".join(calm), calm=calm)

    logs = query("""
        SELECT service, level, message, count() AS c
        FROM streammind.logs
        WHERE ts >= {lo} AND ts <= {hi} AND level IN ('WARN', 'ERROR')
        GROUP BY service, level, message ORDER BY c DESC LIMIT 14
    """.format(lo=ms(ws - 20000), hi=ms(we + 8000)))
    for r in logs:
        add("log", "%s %s x%s: \"%s\"" % (r["service"], r["level"], r["c"], r["message"]),
            service=r["service"], level=r["level"], message=r["message"], count=int(r["c"]))

    deploys = query("""
        SELECT toUnixTimestamp64Milli(ts) AS t, service, version, previous_version, author, change_summary
        FROM streammind.deployments
        WHERE ts >= {lo} AND ts <= {hi} ORDER BY ts DESC LIMIT 8
    """.format(lo=ms(ws - 20 * 60000), hi=ms(we)))
    related = set([service] + DEPENDENCIES.get(service, []))
    for r in deploys:
        age = (ws - int(r["t"])) / 1000
        rel = "alerted service" if r["service"] == service else (
            "dependency" if r["service"] in related else "unrelated")
        add("deployment", "%s deployed %s (previous %s) by %s %.0fs before the anomaly window: \"%s\" [%s]" % (
            r["service"], r["version"], r["previous_version"], r["author"], age, r["change_summary"], rel),
            service=r["service"], version=r["version"], previous=r["previous_version"],
            age_s=age, relation=rel, summary=r["change_summary"])

    spans = query("""
        SELECT service, peer,
          countIf(ts >= {a}) AS n,
          avgIf(duration_ms, ts >= {a}) AS d_cur,
          sumIf(error, ts >= {a}) AS e_cur,
          avgIf(duration_ms, ts < {b}) AS d_base
        FROM streammind.spans
        WHERE ts >= {lo} AND ts <= {c}
        GROUP BY service, peer HAVING n > 0 ORDER BY service, peer
    """.format(a=ms(ws), b=ms(ws - 15000), c=ms(we + 5000), lo=ms(ws - 150000)))
    for r in spans:
        n, dc, db, ec = int(r["n"]), num(r["d_cur"]), num(r["d_base"]), int(r["e_cur"])
        if (db and dc > 3 * db) or ec > 0.3 * n:
            add("trace", "spans %s -> %s: n=%d, mean %.0fms (baseline %.0fms), %d errors" % (
                r["service"], r["peer"], n, dc, db, ec),
                service=r["service"], peer=r["peer"], n=n, d_cur=dc, d_base=db, errors=ec)
    return evidence
