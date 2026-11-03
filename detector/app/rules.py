def evaluate(metric, value, base):
    mean, std = base.mean, base.std
    if metric == "request_rate":
        eff_std = max(std, 0.12 * mean, 1.0)
        z = (value - mean) / eff_std
        if z > 4 and value > 1.8 * mean:
            return "spike", z
        if z < -4 and value < 0.5 * mean:
            return "drop", z
    elif metric == "error_rate":
        eff_std = max(std, 0.02)
        z = (value - mean) / eff_std
        if value > 0.10 and z > 5:
            return "surge", z
    elif metric == "latency":
        eff_std = max(std, 0.12 * mean, 1.0)
        z = (value - mean) / eff_std
        if z > 4 and value > 2.0 * mean:
            return "degradation", z
    return None, 0.0


def severity(metric, direction, value, expected):
    ratio = value / expected if expected else 0
    if metric == "error_rate":
        return "critical" if value > 0.25 else "high"
    if direction in ("spike", "degradation"):
        return "critical" if ratio > 3.5 else "high"
    return "critical" if ratio < 0.2 else "high"
