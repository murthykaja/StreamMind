import re

from .models import Action, Hypothesis

POOL = re.compile(r"pool exhausted|slow query", re.I)
UNREACH = re.compile(r"(\w+) upstream unreachable|connection refused", re.I)
SURGE = re.compile(r"campaign|request rate .* above baseline|autoscaler", re.I)


def ids(items):
    return [e["id"] for e in items]


def analyze(alert, evidence):
    svc = alert["service"]
    metric, direction = alert["metric"], alert["direction"]
    logs = [e for e in evidence if e["source"] == "log"]
    deploys = [e for e in evidence if e["source"] == "deployment"]
    spans = [e for e in evidence if e["source"] == "trace"]
    deltas = {e["data"]["service"]: e for e in evidence
              if e["source"] == "metric" and "service" in e["data"] and "rate_ratio" in e["data"]}
    metric_ev = evidence[0]
    candidates = []

    own_deploy = next((d for d in deploys if d["data"]["relation"] == "alerted service"
                       and d["data"]["age_s"] < 1200), None)
    version_logs = [l for l in logs if l["data"]["service"] == svc and l["data"]["level"] == "ERROR"
                    and own_deploy and own_deploy["data"]["version"] in l["data"]["message"]]
    if metric == "error_rate" and own_deploy:
        score = 0.55 + (0.3 if version_logs else 0.0) + 0.1
        cites = [metric_ev["id"], own_deploy["id"]] + ids(version_logs[:2])
        d = own_deploy["data"]
        candidates.append((score, Hypothesis(
            category="bad_deployment", suspect_service=svc,
            summary="%s %s, deployed %.0fs before the error surge, introduced the failures." % (
                svc, d["version"], d["age_s"]),
            reasoning="Error rate rose sharply on %s shortly after a deployment of %s; %s." % (
                svc, d["version"], "ERROR logs reference the new version" if version_logs
                else "no other correlated change was found"),
            confidence=min(score, 0.97), citations=cites,
            action=Action(type="rollback", target=svc,
                          detail="Roll back %s from %s to %s" % (svc, d["version"], d["previous"])))))

    slow = [s for s in spans if s["data"]["d_base"] and s["data"]["d_cur"] > 3 * s["data"]["d_base"]]
    pool_logs = {}
    for l in logs:
        if POOL.search(l["data"]["message"]):
            pool_logs.setdefault(l["data"]["service"], []).append(l)
    if slow or pool_logs:
        suspect = None
        cites = [metric_ev["id"]]
        score = 0.35
        if slow:
            top = max(slow, key=lambda s: s["data"]["d_cur"] / max(s["data"]["d_base"], 1))
            suspect = top["data"]["peer"]
            cites.append(top["id"])
            score += 0.25
        if pool_logs:
            cand = suspect if suspect in pool_logs else max(pool_logs, key=lambda k: len(pool_logs[k]))
            if suspect is None or cand == suspect:
                suspect = cand
                cites += ids(pool_logs[cand][:2])
                score += 0.25
        if suspect in deltas and deltas[suspect]["data"]["lat_ratio"] > 2:
            cites.append(deltas[suspect]["id"])
            score += 0.12
        if suspect:
            candidates.append((score, Hypothesis(
                category="dependency_degradation", suspect_service=suspect,
                summary=("%s is degraded (resource exhaustion), slowing its callers including %s." % (suspect, svc)
                         if suspect != svc else "%s is degraded (resource exhaustion) and responding slowly." % svc),
                reasoning="Latency regressed on %s; calls toward %s are several times slower than baseline "
                          "and %s reports pool exhaustion or slow queries." % (svc, suspect, suspect),
                confidence=min(score, 0.95), citations=list(dict.fromkeys(cites)),
                action=Action(type="circuit_break", target=suspect,
                              detail="Open circuit breaker toward %s, shed non-critical traffic, page its owner" % suspect))))

    peers = {}
    for l in logs:
        m = UNREACH.search(l["data"]["message"])
        if m and l["data"]["level"] == "ERROR":
            peer = m.group(1) if m.group(1) and m.group(1).lower() != "connection" else None
            if peer:
                peers.setdefault(peer, []).append(l)
    err_spans = [s for s in spans if s["data"]["n"] and s["data"]["errors"] > 0.5 * s["data"]["n"]]
    if direction == "drop" or peers:
        if peers or err_spans:
            peer = (max(peers, key=lambda k: len(peers[k])) if peers else err_spans[0]["data"]["peer"])
            cites = [metric_ev["id"]] + ids(peers.get(peer, [])[:2])
            score = 0.4 + (0.25 if peers.get(peer) else 0)
            es = [s for s in err_spans if s["data"]["peer"] == peer]
            if es:
                cites.append(es[0]["id"])
                score += 0.2
            peer_logs = [l for l in logs if l["data"]["service"] == peer]
            cert = [l for l in peer_logs if "certificate" in l["data"]["message"].lower()]
            if peer_logs:
                cites.append((cert or peer_logs)[0]["id"])
                score += 0.12
            detail = "Fail over %s traffic to the secondary %s endpoint" % (svc, peer)
            if cert:
                detail += " and renew the expired certificate"
            candidates.append((score, Hypothesis(
                category="upstream_outage", suspect_service=peer,
                summary="Upstream %s is unreachable, so %s lost traffic." % (peer, svc),
                reasoning="Request rate on %s collapsed while it logs connection failures toward %s%s." % (
                    svc, peer, "; %s itself reports a certificate failure" % peer if cert else ""),
                confidence=min(score, 0.95), citations=list(dict.fromkeys(cites)),
                action=Action(type="failover", target=peer, detail=detail))))

    if direction == "spike":
        surge_logs = [l for l in logs if l["data"]["service"] == svc and SURGE.search(l["data"]["message"])]
        has_errors = deltas.get(svc, {"data": {"err_cur": 0}})["data"]["err_cur"] > 0.1
        score = 0.5 + (0.3 if surge_logs else 0) - (0.2 if has_errors or own_deploy else 0)
        cites = [metric_ev["id"]] + ids(surge_logs[:2])
        if svc in deltas:
            cites.append(deltas[svc]["id"])
        candidates.append((score, Hypothesis(
            category="traffic_surge", suspect_service=svc,
            summary="Organic or campaign-driven traffic surge on %s, without errors or a recent change." % svc,
            reasoning="Request rate rose several-fold on %s; %s" % (
                svc, "logs name the campaign traffic and autoscaler saturation." if surge_logs
                else "no deployment or dependency fault explains it."),
            confidence=max(0.3, min(score, 0.95)), citations=list(dict.fromkeys(cites)),
            action=Action(type="scale_out", target=svc,
                          detail="Scale out %s (+4 replicas) and enable per-client rate limiting" % svc))))

    if not candidates:
        return Hypothesis(category="unknown", suspect_service=svc,
                          summary="No correlated cause found in logs, traces or deployments.",
                          reasoning="Evidence did not match a known failure pattern.", confidence=0.2,
                          citations=[metric_ev["id"]], action=Action(type="none"))
    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates[0][1]
