SERVICES = ["checkout", "payments", "inventory", "search", "auth"]

DEPENDENCIES = {
    "checkout": ["payments", "inventory", "auth"],
    "payments": ["auth"],
    "inventory": [],
    "search": ["inventory"],
    "auth": [],
}

BASE_RATE = {"checkout": 16, "payments": 13, "inventory": 15, "search": 20, "auth": 22}
BASE_LATENCY = {"checkout": 120, "payments": 180, "inventory": 60, "search": 90, "auth": 35}
BASE_ERROR = 0.015

ENDPOINTS = {
    "checkout": ["POST /cart/submit", "GET /cart", "POST /order"],
    "payments": ["POST /charge", "POST /refund", "GET /status"],
    "inventory": ["GET /stock", "PUT /reserve"],
    "search": ["GET /search", "GET /suggest"],
    "auth": ["POST /token", "GET /verify"],
}

AUTHORS = ["alice", "bao", "carlos", "dmitri", "emeka", "farah"]
