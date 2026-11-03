import json
import os
import time
from datetime import datetime, timezone

import httpx
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CLICKHOUSE = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")

TABLES = {
    "context.logs": ("logs", ["ts", "service", "level", "message", "version"]),
    "context.spans": ("spans", ["ts", "trace_id", "service", "peer", "duration_ms", "error"]),
    "context.deployments": ("deployments", ["ts", "service", "version", "previous_version",
                                            "author", "change_summary"]),
}


def fmt_ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def flush(client, buffers):
    for topic, rows in buffers.items():
        if not rows:
            continue
        table = TABLES[topic][0]
        body = "\n".join(json.dumps(r) for r in rows)
        try:
            client.post("/", params={"query": "INSERT INTO streammind.%s FORMAT JSONEachRow" % table},
                        content=body).raise_for_status()
            rows.clear()
        except Exception as exc:
            print("[sink] insert into %s failed: %s" % (table, exc), flush=True)
            if len(rows) > 20000:
                rows.clear()


def main():
    client = httpx.Client(base_url=CLICKHOUSE, timeout=10)
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "sink",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe(list(TABLES))
    buffers = {t: [] for t in TABLES}
    last = time.time()
    print("[sink] consuming context topics", flush=True)
    while True:
        for m in consumer.consume(num_messages=500, timeout=0.5):
            if m.error():
                continue
            try:
                row = json.loads(m.value())
            except ValueError:
                continue
            cols = TABLES[m.topic()][1]
            row = {c: row.get(c) for c in cols}
            row["ts"] = fmt_ts(row["ts"])
            buffers[m.topic()].append(row)
        if time.time() - last >= 1:
            flush(client, buffers)
            last = time.time()


if __name__ == "__main__":
    main()
