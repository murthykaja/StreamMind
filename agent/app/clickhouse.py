import os

import httpx

_client = httpx.Client(base_url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"), timeout=15)


def query(sql):
    resp = _client.post("/", params={"default_format": "JSON"}, content=sql)
    resp.raise_for_status()
    return resp.json().get("data", [])
