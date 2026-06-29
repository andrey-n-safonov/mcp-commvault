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
        job_type: str = "",
        status_filter: str = "",
        hours_back: int = 24,
        limit: int = 50,
    ) -> list[dict]:
        params: dict = {
            "completedJobLookupTime": hours_back * 3600,
            "limit": limit,
        }
        if client_name:
            # server-side hint may be ignored; bump limit to avoid cutoff before client filter
            params["limit"] = max(limit, 500)
        if job_type:
            params["jobFilter"] = job_type  # Backup or Restore

        data = await self._get("/Job", params)
        jobs = [item["jobSummary"] for item in data.get("jobs", []) if "jobSummary" in item]

        if client_name:
            needle = client_name.lower()
            jobs = [
                j for j in jobs
                if needle in ((j.get("subclient") or {}).get("clientName") or "").lower()
                or needle in (j.get("clientName") or "").lower()
            ]

        if status_filter:
            jobs = [j for j in jobs if j.get("status", "").lower() == status_filter.lower()]
        return jobs[:limit]

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
            clients = data.get("Client", {}).get("clientSummary", [])

        if name_filter:
            needle = name_filter.lower()
            clients = [
                c for c in clients
                if isinstance(c, dict)
                and needle in (c.get("client", {}).get("clientName") or c.get("clientName", "")).lower()
            ]

        if not show_offline:
            clients = [
                c for c in clients
                if isinstance(c, dict)
                and c.get("clientFlags", {}).get("clientActivityControl", {})
                   .get("activityControlOptions", [{}])[0].get("enableActivityType", True)
            ]

        return clients

    async def get_media_agents(self) -> list[dict]:
        data = await self._get("/MediaAgent")
        return data.get("response", [])

    async def list_subclients(
        self,
        client_name: str = "",
        app_type: str = "",
    ) -> list[dict]:
        # Get all clients, filter by name, then fetch subclients per client
        data = await self._get("/Client")
        all_clients = data.get("clientProperties", [])

        if client_name:
            needle = client_name.lower()
            all_clients = [
                c for c in all_clients
                if needle in (c.get("client", {}).get("clientEntity", {})
                               .get("clientName", "")).lower()
                or needle in (c.get("client", {}).get("clientEntity", {})
                               .get("displayName", "")).lower()
            ]

        results: list[dict] = []
        for c in all_clients:
            entity = c.get("client", {}).get("clientEntity", {})
            cid = entity.get("clientId")
            cname = entity.get("clientName", "?")
            if not cid:
                continue
            sub_data = await self._get("/Subclient", {"clientId": cid})
            subs = sub_data.get("subClientProperties", [])
            for s in subs:
                e = s.get("subClientEntity", {})
                if app_type and app_type.lower() not in e.get("appName", "").lower():
                    continue
                sp = (s.get("commonProperties", {})
                      .get("storageDevice", {})
                      .get("dataBackupStoragePolicy", {})
                      .get("storagePolicyName", ""))
                results.append({
                    "clientName": cname,
                    "subclientId": e.get("subclientId"),
                    "subclientName": e.get("subclientName"),
                    "backupsetName": e.get("backupsetName"),
                    "appName": e.get("appName"),
                    "storagePolicyName": sp,
                })
        return results

    async def list_schedule_policies(
        self,
        name_filter: str = "",
        app_type_id: int = 0,
    ) -> list[dict]:
        data = await self._get("/SchedulePolicy")
        items = data.get("taskDetail", [])
        results = []
        for item in items:
            task = item.get("task", {})
            tname = task.get("taskName", "")
            if name_filter and name_filter.lower() not in tname.lower():
                continue
            app_types = [
                a.get("appTypeId")
                for a in item.get("appGroup", {}).get("appTypes", [])
            ]
            if app_type_id and app_type_id not in app_types:
                continue
            results.append({
                "taskId": task.get("taskId"),
                "taskName": tname,
                "description": task.get("description", ""),
                "appTypeIds": app_types,
                "associatedObjects": task.get("associatedObjects", 0),
                "disabled": task.get("taskFlags", {}).get("disabled", False),
            })
        return results

    async def get_schedule_policy_details(self, task_id: int) -> dict:
        data = await self._get(f"/SchedulePolicy/{task_id}")
        ti = data.get("taskInfo", {})
        task = ti.get("task", {})
        subtasks = ti.get("subTasks", [])
        schedules = []
        for st in subtasks:
            pattern = st.get("pattern", {})
            opts = st.get("options", {})
            backup_opts = opts.get("backupOpts", {})
            schedules.append({
                "subTaskName": st.get("subTask", {}).get("subTaskName", ""),
                "schedule": pattern.get("description", ""),
                "backupLevel": backup_opts.get("backupLevel", ""),
            })
        return {
            "taskId": task.get("taskId"),
            "taskName": task.get("taskName", ""),
            "description": task.get("description", ""),
            "schedules": schedules,
        }

    async def get_client_jobs_summary(self, hours_back: int = 48) -> list[dict]:
        jobs = await self.list_jobs(hours_back=hours_back, limit=200)
        # Group by client, keep latest job per client
        by_client: dict[str, dict] = {}
        for j in jobs:
            cname = (j.get("subclient") or {}).get("clientName") or "?"
            existing = by_client.get(cname)
            if not existing or j.get("jobStartTime", 0) > existing.get("jobStartTime", 0):
                by_client[cname] = j
        return sorted(by_client.values(), key=lambda x: x.get("jobStartTime", 0), reverse=True)

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


def _fmt_subclients(subs: list[dict]) -> str:
    if not subs:
        return "(no subclients found)"
    lines = []
    prev_client = None
    for s in sorted(subs, key=lambda x: (x.get("clientName",""), x.get("appName",""), x.get("subclientName",""))):
        if s.get("clientName") != prev_client:
            lines.append(f"\n[{s.get('clientName')}]")
            prev_client = s.get("clientName")
        sp = s.get("storagePolicyName") or "—"
        lines.append(f"  [{s.get('subclientId')}] {s.get('subclientName')} | {s.get('appName')} | bs:{s.get('backupsetName')} | sp:{sp}")
    return "\n".join(lines).strip()


def _fmt_schedule_policies(policies: list[dict]) -> str:
    if not policies:
        return "(no schedule policies found)"
    lines = []
    for p in sorted(policies, key=lambda x: x.get("taskName","")):
        disabled = " [DISABLED]" if p.get("disabled") else ""
        assoc = p.get("associatedObjects", 0)
        apps = ",".join(str(a) for a in p.get("appTypeIds", []))
        desc = p.get("description", "")
        lines.append(f"[{p.get('taskId')}] {p.get('taskName')}{disabled} | apps:{apps} | assoc:{assoc} | {desc}")
    return "\n".join(lines)


def _fmt_client_jobs_summary(jobs: list[dict]) -> str:
    if not jobs:
        return "(no data)"
    lines = []
    for j in jobs:
        client = (j.get("subclient") or {}).get("clientName") or "?"
        status = j.get("status", "?")
        jtype = j.get("jobType", "?")
        start = j.get("jobStartTime", 0)
        reason = j.get("pendingReason") or j.get("failureReason") or ""
        line = f"{client:40s} | {status:10s} | {jtype} | [{j.get('jobId')}]"
        if reason:
            short = reason[:100].replace("<br>", " ")
            line += f"\n    {short}"
        lines.append(line)
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
                "Filter by client name, job type, status, and time window. "
                "Returns job ID, client, subclient, status, and failure reason."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "client_name": {
                        "type": "string",
                        "description": "Filter by client name (partial match supported by CommServer).",
                    },
                    "job_type": {
                        "type": "string",
                        "enum": ["", "Backup", "Restore"],
                        "description": "Filter by operation type. Leave empty for all.",
                    },
                    "status_filter": {
                        "type": "string",
                        "enum": ["", "Running", "Completed", "Failed", "Pending", "Waiting", "Suspended"],
                        "description": "Filter by job status (client-side). Leave empty for all.",
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
        Tool(
            name="list_subclients",
            description=(
                "List subclients (backup groups) for one or all clients. "
                "Filter by client name and/or application type (e.g. 'Virtual Server', 'File System', 'SQL Server'). "
                "Returns subclient name, backupset, app type, storage policy."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "client_name": {
                        "type": "string",
                        "description": "Filter by client name (partial match). Leave empty to list all.",
                    },
                    "app_type": {
                        "type": "string",
                        "description": "Filter by application type, e.g. 'Virtual Server', 'File System', 'SQL Server'.",
                    },
                },
            },
        ),
        Tool(
            name="list_schedule_policies",
            description=(
                "List schedule policies configured in CommCell. "
                "Filter by name or application type ID (106=Virtual Server, 33=File System, 81=SQL Server). "
                "Returns policy name, description, associated object count, schedule summary."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name_filter": {
                        "type": "string",
                        "description": "Filter by policy name (partial match).",
                    },
                    "app_type_id": {
                        "type": "integer",
                        "description": "Filter by app type ID: 106=Virtual Server, 33=File System, 81=SQL Server.",
                    },
                },
            },
        ),
        Tool(
            name="get_schedule_policy_details",
            description=(
                "Get full schedule details for a specific policy by taskId: "
                "all sub-tasks with their cron-style descriptions and backup levels."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "integer",
                        "description": "Schedule policy task ID (from list_schedule_policies).",
                    },
                },
                "required": ["task_id"],
            },
        ),
        Tool(
            name="get_client_jobs_summary",
            description=(
                "Summary of the latest backup job per client over a time window. "
                "Shows which clients have recent successful/failed/running backups at a glance."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_back": {
                        "type": "integer",
                        "default": 48,
                        "description": "How many hours back to look (default 48).",
                    },
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    client = get_client()

    if name == "list_jobs":
        jobs = await client.list_jobs(
            client_name=arguments.get("client_name", ""),
            job_type=arguments.get("job_type", ""),
            status_filter=arguments.get("status_filter", ""),
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

    if name == "list_subclients":
        subs = await client.list_subclients(
            client_name=arguments.get("client_name", ""),
            app_type=arguments.get("app_type", ""),
        )
        return [TextContent(type="text", text=_fmt_subclients(subs))]

    if name == "list_schedule_policies":
        policies = await client.list_schedule_policies(
            name_filter=arguments.get("name_filter", ""),
            app_type_id=arguments.get("app_type_id", 0),
        )
        return [TextContent(type="text", text=_fmt_schedule_policies(policies))]

    if name == "get_schedule_policy_details":
        details = await client.get_schedule_policy_details(arguments["task_id"])
        import json
        return [TextContent(type="text", text=json.dumps(details, indent=2, ensure_ascii=False))]

    if name == "get_client_jobs_summary":
        jobs = await client.get_client_jobs_summary(
            hours_back=arguments.get("hours_back", 48),
        )
        return [TextContent(type="text", text=_fmt_client_jobs_summary(jobs))]

    raise ValueError(f"Unknown tool: {name}")


def main() -> None:
    asyncio.run(_run())


async def _run() -> None:
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


if __name__ == "__main__":
    main()
