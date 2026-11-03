# Architecture

## Principle: two paths, one seam

| Path | Latency budget | Components | Failure mode |
|---|---|---|---|
| Streaming | seconds, deterministic | generator → Kafka → detector → `alerts` | Never blocked by the agent or an LLM |
| Agent | seconds, best effort | agent → ClickHouse retrieval → hypothesis → approval | Slow or failing LLM only delays hypotheses; detection continues |

The seam is the `alerts` topic. If the agent is down, alerts queue in Kafka and are processed on return.

## Topics

| Topic | Producer | Consumer | Content |
|---|---|---|---|
| events | generator | detector | request events (id, service, event time, status, latency) |
| context.logs / .spans / .deployments | generator | sink | evidence stored in ClickHouse |
| alerts | detector | agent | anomaly with window, observed/expected, z-score, freshness |
| hypotheses | agent | (observers, Kafka UI) | published root-cause hypotheses |
| approvals | api | agent | human decision that resumes the paused graph |
| ground-truth | generator | api | labelled faults, used only for scoring |

## Detector semantics

- **Dedup**: `SET NX EX` on the event id in Redis; replays and duplicate bursts do not inflate counts.
- **Windows**: tumbling, event-time, 5 s. Empty windows are materialised for every known service so a total dropout is visible as a drop, not as silence.
- **Watermark**: `max(event_ts) − 5 s`. A window closes when the watermark passes its end. Events for closed windows are counted as `late_dropped`; out-of-order events inside the lateness bound are accepted.
- **Detection**: EWMA mean and variance per service and metric (request rate, error rate, mean latency) with a std floor, z-score plus ratio guards. Anomalous windows do not update the baseline. An alert requires 2 consecutive anomalous windows and stays open until 3 calm windows, so one episode yields one alert per metric.
- **Freshness**: alert time minus the newest event time in the triggering window.

## Agent graph

```
gather_context ─► hypothesize ─► validate ─┬─► publish ─┬─► approval (interrupt) ─► execute ─► END
                       ▲                   │            └─► END (no action proposed)
                       └──── retry ◄───────┘ (no valid citations)
```

- Evidence items carry ids (`E1…`). Hypotheses must cite them; unknown ids are dropped and confidence is reduced.
- Engines: Claude through LangChain structured output (`Hypothesis` pydantic model), or a rule-based engine that scores each failure pattern against the same evidence. An LLM failure falls back to the heuristic engine and is recorded in the incident.
- The graph is checkpointed in memory per incident. `interrupt` pauses before execution; the Kafka `approvals` message resumes it. After an agent restart, pending graphs are lost but incident state in Redis remains readable.

## Scoring

An alert matches an injected anomaly if its service is in the anomaly's affected set and its window overlaps the fault interval (plus 15 s slack). Anomalies are counted once 45 s have passed since they ended. Alerts matching no anomaly are false positives, with distractor overlaps reported separately.

## Known limits

- Single Kafka broker and single detector instance; state is in memory plus Redis.
- Detection thresholds are tuned for the synthetic traffic profile.
- Action execution is simulated.
