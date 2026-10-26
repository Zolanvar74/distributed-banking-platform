"""Stage 01 service skeleton; no transaction/migration/healing logic yet."""
from fastapi import FastAPI

app = FastAPI(title="self-healing / Stage 01")


@app.get("/health")
def health():
    return {"status": "ok", "service": "self-healing", "implementation": "skeleton-only"}
