import random
from dataclasses import dataclass, field

from .topology import DEPENDENCIES, SERVICES


@dataclass
class Fault:
    kind: str
    category: str
    service: str
    start: float
    end: float
    affected: list
    suspect: str = ""
    action: str = "none"
    params: dict = field(default_factory=dict)
    announced: bool = False

    def active(self, now):
        return self.start <= now < self.end

    def effect(self, service, now):
        eff = {"rate": 1.0, "lat": 1.0, "err": 0.0, "dup": False, "late": False}
        if not self.active(now):
            return eff
        if self.kind == "bad_deployment" and service == self.service:
            eff["err"] = 0.35
            eff["lat"] = 1.1
        elif self.kind == "dependency_degradation":
            if service == self.params["dependency"]:
                eff["lat"] = 6.0
                eff["err"] = 0.04
            elif service == self.service:
                eff["lat"] = 3.5
        elif self.kind == "traffic_surge" and service == self.service:
            eff["rate"] = 4.0
            eff["lat"] = 1.4
        elif self.kind == "upstream_outage" and service == self.service:
            eff["rate"] = 0.08
        elif self.kind == "duplicate_burst" and service == self.service:
            eff["dup"] = True
        elif self.kind == "late_data" and service == self.service:
            eff["late"] = True
        return eff


ANOMALY_KINDS = ["bad_deployment", "dependency_degradation", "traffic_surge", "upstream_outage"]
DISTRACTOR_KINDS = ["duplicate_burst", "late_data"]


def build_fault(rng, kind, now, versions):
    duration = rng.uniform(32, 42)
    if kind == "bad_deployment":
        svc = rng.choice(SERVICES)
        return Fault(kind, "bad_deployment", svc, now + 12, now + 12 + duration, [svc],
                     suspect=svc, action="rollback")
    if kind == "dependency_degradation":
        svc = rng.choice([s for s in SERVICES if DEPENDENCIES[s]])
        dep = rng.choice(DEPENDENCIES[svc])
        return Fault(kind, "dependency_degradation", svc, now, now + duration, [svc, dep],
                     suspect=dep, action="circuit_break", params={"dependency": dep})
    if kind == "traffic_surge":
        svc = rng.choice(SERVICES)
        return Fault(kind, "traffic_surge", svc, now, now + duration, [svc],
                     suspect=svc, action="scale_out")
    if kind == "upstream_outage":
        svc = rng.choice([s for s in SERVICES if "auth" in DEPENDENCIES[s]])
        return Fault(kind, "upstream_outage", svc, now, now + duration, [svc],
                     suspect="auth", action="failover", params={"upstream": "auth"})
    svc = rng.choice(SERVICES)
    return Fault(kind, "none", svc, now, now + 14, [svc], action="none")
