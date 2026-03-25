"""
nexus_data/engine/ui_server.py
Entry point for the `nexusdata-ui` console command.

Starts the FastAPI backend (app.py) and serves the chat UI from
nexus_data/ui/index.html at the root URL.

Usage:
    nexusdata-ui                          # http://127.0.0.1:7700
    nexusdata-ui --port 8080
    nexusdata-ui --host 0.0.0.0 --port 8080
    nexusdata-ui --reload                 # auto-reload on code changes (dev)

Environment variables:
    NEXUS_DB_URI       — auto-connect to this DB on startup
    NEXUS_API_KEY      — optional API key for all endpoints
    NEXUS_RATE_LIMIT   — requests per minute per IP (default 60)
    NEXUS_CORS_ORIGINS — comma-separated allowed origins (default *)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── Static UI wiring ──────────────────────────────────────────────────────────
_UI_DIR = Path(__file__).parent.parent / "ui"

# Import the shared FastAPI app and add static + root route to it
from nexus_data.engine.app import app
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Serve everything in nexus_data/ui/ under /ui/static/
if _UI_DIR.is_dir():
    app.mount("/ui/static", StaticFiles(directory=str(_UI_DIR)), name="ui-static")


@app.get("/", include_in_schema=False)
async def serve_index():
    """Serve the chat UI HTML."""
    index = _UI_DIR / "index.html"
    if not index.exists():
        from fastapi.responses import HTMLResponse
        return HTMLResponse(
            "<h2>NexusData UI</h2>"
            "<p>Place your <code>index.html</code> in <code>nexus_data/ui/</code>.</p>"
            "<p>API docs: <a href='/docs'>/docs</a></p>",
            status_code=200,
        )
    return FileResponse(str(index))


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:  # pragma: no cover
    import uvicorn

    parser = argparse.ArgumentParser(
        prog="nexusdata-ui",
        description="NexusData Web UI + API server",
    )
    parser.add_argument("--host", default="127.0.0.1",
                        help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7700,
                        help="Port to listen on (default: 7700)")
    parser.add_argument("--reload", action="store_true",
                        help="Enable auto-reload (development only)")
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}"
    print(f"\n{'='*55}")
    print(f"  NexusData UI  →  {url}")
    print(f"  API Docs      →  {url}/docs")
    print(f"{'='*55}\n")

    uvicorn.run(
        "nexus_data.engine.ui_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
