#!/usr/bin/env python3
"""
mcp-commvault — MCP server for Commvault Backup & Recovery REST API v2.

Tools:
  list_jobs(client_name?, status?, hours_back?, limit?)   — backup/restore jobs
  list_events(error_code?, level?, hours_back?)           — CommCell events & alerts
  list_clients(name_filter?, show_offline?)               — registered clients
  get_job_details(job_id)                                 — full job info + failure reason
  get_media_agents()                                      — MediaAgent status
"""

import asyncio
import base64
import configparser
import os
import time
from pathlib import Path
from typing import Any

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_CONFIG_LOCATIONS = [
    os.environ.get("COMMVAULT_MCP_CONFIG", ""),
    os.path.expanduser("~/.config/mcp-commvault/config.ini"),
    os.path.join(os.path.dirname(__file__), "..", "..", "config.ini"),
]


def load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    for path in _CONFIG_LOCATIONS:
        if path and Path(path).exists():
            cfg.read(path)
            return cfg
    raise FileNotFoundError(
        "Config not found. Copy config.example.ini to one of:\n"
        + "\n".join(f"  {p}" for p in _CONFIG_LOCATIONS if p)
    )


# ---------------------------------------------------------------------------
# Commvault REST API v2 client
# ---------------------------------------------------------------------------

class CommvaultClient:
    def __init__(self, cfg: configparser.ConfigParser):
        cv = cfg["commvault"]
        self.base_url = cv["url"].rstrip("/")
        self.username = cv["username"]
        pwd_env = cv.get("password_env", "COMMVAULT_PASSWORD")
        self.password = os.environ.get(pwd_env, "")
        proxy = cv.get("proxy", "").strip()

        transport = None
        if proxy:
            from httpx_socks import AsyncProxyTransport
            # httpx-socks supports socks5/socks4 but not socks5h — normalize
            proxy = proxy.replace("socks5h://", "socks5://").replace("socks4a://", "socks4://")
            transport = AsyncProxyTransport.from_url(proxy)

        self._http = httpx.AsyncClient(
            transport=transport,
            timeout=30.0,
            verify=False,
        )
        self._token: str | None = None
        self._token_ts: float = 0.0

    async def _ensure_token(self) -> str:
        # Re-auth if no token or older than 20 minutes
        if self._token and (time.time() - self._token_ts) < 1200:
            return self._token

        pwd_b64 = base64.b64encode(self.password.encode()).decode()
        resp = await self._http.post(
            f"{self.base_url}/SearchSvc/CVWebService.svc/Login",
            json={"username": self.username, "password": pwd_b64},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token", "")
        if not token:
            raise RuntimeError(f"Login failed: {data}")
        self._token = token
        self._token_ts = time.time()
        return token

    async def _get(self, path: str, params: dict | None = None) -> Any:
        token = await self._ensure_token()
        resp = await self._http.get(
            f"{self.base_url}/SearchSvc/CVWebService.svc{path}",
            params=params,
            headers={"Authtoken": token, "Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()

    # --- public methods ---

    async def list_jobs(
        self,
        client_name: str = "",
        status: str = "",
        hours_back: int = 24,
        limit: int = 50,
    ) -> list[dict]:
        params: dict = {
            "completedJobLookupTime": hours_back * 3600,
            "limit": limit,
        }
        if client_name:
            params["clientName"] = client_name
        if status:
            params["jobFilter"] = status

        data = await self._get("/Job", params)
        return [item["jobSummary"] for item in data.get("jobs", []) if "jobSummary" in item]

    async def get_job_details(self, job_id: int) -> dict:
        data = await self._get(f"/Job/{job_id}")
        items = data.get("jobs", [])
        if items and "jobSummary" in items[0]:
            return items[0]["jobSummary"]
        return {}

    async def list_events(
        self,
        error_code: str = "",
        level: int = 0,
        hours_back: int = 24,
    ) -> list[dict]:
        params: dict = {
            "limit": 200,
            "fromTime": int(time.time()) - hours_back * 3600,
        }
        if level:
            params["level"] = level

        data = await self._get("/Events", params)
        events = data.get("commservEvents", [])

        if error_code:
            events = [e for e in events if error_code in e.get("description", "")]
        return events

    async def list_clients(
        self,
        name_filter: str = "",
        show_offline: bool = False,
    ) -> list[dict]:
        params: dict = {}
        if name_filter:
            params["clientName"] = name_filter

        data = await self._get("/Client", params)
        clients = data.get("clientProperties", [])
        if not isinstance(clients, list):
            # sometimes wrapped differently
            clients = data.get("Client", {}).get("clientSummary", [])

        if not show_offline:
            clients = [c for c in clients if c.get("clientFlags", {}).get("clientActivityControl", {}).get("activityControlOptions", [{}])[0].get("enableActivityType", True) if isinstance(c, dict)]

        return clients

    async def get_media_agents(self) -> list[dict]:
        data = await self._get("/MediaAgent")
        return data.get("response", [])

    async def close(self) -> None:
        await self._http.aclose()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_jobs(jobs: list[dict]) -> str:
    if not jobs:
        return "(no jobs found)"
    lines = []
    for j in jobs:
        client = (j.get("subclient") or {}).get("clientName") or j.get("clientName", "?")
        lines.append(
            f"[{j.get('jobId','?')}] {client} / {j.get('subclientName','?')} "
            f"| {j.get('jobType','?')} | {j.get('status','?')} "
            f"| started: {j.get('jobStartTime','?')}"
        )
        reason = j.get("pendingReason") or j.get("failureReason") or ""
        if reason:
            lines.append(f"    reason: {reason}")
    return "\n".join(lines)


def _fmt_events(events: list[dict]) -> str:
    if not events:
        return "(no events found)"
    lines = []
    for e in events:
        lines.append(
            f"[{e.get('id','?')}] {e.get('timeSource','?')} "
            f"sev={e.get('severity','?')} | {e.get('description','')[:200]}"
        )
    return "\n".join(lines)


def _fmt_clients(clients: list[dict]) -> str:
    if not clients:
        return "(no clients found)"
    lines = []
    for c in clients:
        name = c.get("client", {}).get("clientName") or c.get("clientName", "?")
        status = c.get("clientStatus", "?")
        lines.append(f"{name} | status: {status}")
    return "\n".join(lines)


def _fmt_agents(agents: list[dict]) -> str:
    if not agents:
        return "(no media agents found)"
    lines = []
    for a in agents:
        info = a.get("entityInfo", {})
        name = info.get("name", "?")
        lines.append(f"[{info.get('id','?')}] {name}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

server = Server("mcp-commvault")
_client: CommvaultClient | None = None


def get_client() -> CommvaultClient:
    global _client
    if _client is None:
        _client = CommvaultClient(load_config())
    return _client


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="list_jobs",
            description=(
                "List Commvault backup/restore jobs. "
                "Filter by client name, status, and time window. "
                "Returns job ID, client, subclient, status, and failure reason."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "client_name": {
                        "type": "string",
                        "description": "Filter by client name (partial match supported by CommServer).",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["", "Backup", "Restore", "Running", "Completed", "Failed", "Pending", "Waiting"],
                        "description": "Filter by job type or status. Leave empty for all.",
                    },
                    "hours_back": {
                        "type": "integer",
                        "default": 24,
                        "description": "How many hours back to look (default 24).",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 50,
                        "description": "Max number of jobs to return (default 50).",
                    },
                },
            },
        ),
        Tool(
            name="list_events",
            description=(
                "List CommCell events and alerts. "
                "Optionally filter by error code (e.g. '30:316') or severity level. "
                "Useful for diagnosing recurring failures."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "error_code": {
                        "type": "string",
                        "description": "Filter events containing this error code, e.g. '30:316'.",
                    },
                    "level": {
                        "type": "integer",
                        "enum": [0, 1, 2, 3, 6],
                        "description": "Severity: 0=all, 1=critical, 2=warning, 3=info, 6=minor.",
                    },
                    "hours_back": {
                        "type": "integer",
                        "default": 24,
                        "description": "How many hours back to look (default 24).",
                    },
                },
            },
        ),
        Tool(
            name="list_clients",
            description=(
                "List clients registered in CommCell. "
                "Filter by name. "
                "Shows client name and status."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name_filter": {
                        "type": "string",
                        "description": "Filter by client name.",
                    },
                    "show_offline": {
                        "type": "boolean",
                        "default": False,
                        "description": "Include offline/inactive clients.",
                    },
                },
            },
        ),
        Tool(
            name="get_job_details",
            description=(
                "Get full details of a specific Commvault job by ID: "
                "status, failure reason, data transferred, duration, subclient, storage policy."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "integer",
                        "description": "Commvault job ID.",
                    },
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="get_media_agents",
            description="List MediaAgents and their status (online/offline).",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    client = get_client()

    if name == "list_jobs":
        jobs = await client.list_jobs(
            client_name=arguments.get("client_name", ""),
            status=arguments.get("status", ""),
            hours_back=arguments.get("hours_back", 24),
            limit=arguments.get("limit", 50),
        )
        return [TextContent(type="text", text=_fmt_jobs(jobs))]

    if name == "list_events":
        events = await client.list_events(
            error_code=arguments.get("error_code", ""),
            level=arguments.get("level", 0),
            hours_back=arguments.get("hours_back", 24),
        )
        return [TextContent(type="text", text=_fmt_events(events))]

    if name == "list_clients":
        clients = await client.list_clients(
            name_filter=arguments.get("name_filter", ""),
            show_offline=arguments.get("show_offline", False),
        )
        return [TextContent(type="text", text=_fmt_clients(clients))]

    if name == "get_job_details":
        job = await client.get_job_details(arguments["job_id"])
        if not job:
            return [TextContent(type="text", text="Job not found.")]
        import json
        return [TextContent(type="text", text=json.dumps(job, indent=2, ensure_ascii=False))]

    if name == "get_media_agents":
        agents = await client.get_media_agents()
        return [TextContent(type="text", text=_fmt_agents(agents))]

    raise ValueError(f"Unknown tool: {name}")


def main() -> None:
    asyncio.run(_run())


async def _run() -> None:
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


if __name__ == "__main__":
    main()
