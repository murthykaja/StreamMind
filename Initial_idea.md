# Real-Time + AI Combined

## Problem statement
Real-time systems detect problems fast, but the alert alone does not explain what happened. Engineers still have to dig through logs, traces, and recent deployments to find the root cause. By the time they do, the window to act has passed.

## Goal
Combine a real-time streaming pipeline with an AI agent: the stream detects an anomaly and triggers the agent, which retrieves context and proposes a root cause — in seconds, not after manual digging.

## Technologies to use
- Apache Kafka for event ingestion
- Apache Flink or Spark Structured Streaming for real-time detection
- Redis and/or ClickHouse for fast state and serving
- LangChain / LangGraph for the agent (see Project 3 for the graph design)
- Python, FastAPI for the serving/API layer
- Docker Compose for a reproducible local setup

## What to build (guide, not code)
1. Generate synthetic events with injected anomalies (spikes, drops, late data, duplicates).
2. Stream them through Kafka to Flink/Spark, with windowed aggregation and simple anomaly rules.
3. On anomaly, publish an alert event — do not put the LLM inside the streaming path.
4. The agent consumes the alert, retrieves logs/traces/deployments for that window, and drafts a root-cause hypothesis with citations.
5. Add a human-approval step before any recommended action.

## What to measure
- Alert-to-hypothesis time in seconds (benchmarked)
- Detection accuracy on injected anomalies: true positives, false positives
- Freshness: event time to alert time, p50/p95


Note: keep the streaming path and the agent path separate — that separation is part of the design.
