"""
Deploy indonesia-ocean-collector to Airbyte at maritime.lokakara.com.

This script:
1. Authenticates to Airbyte via Keycloak (must run ON the VM or with port-forward)
2. Creates/updates a custom Python source connector using Airbyte's declarative manifest
3. Creates a PostgreSQL destination pointing to master_oceanography
4. Creates a connection with daily schedule at 01:00 UTC

Usage (run on the VM):
    cd /path/to/indonesia-ocean-collector
    pip install requests python-dotenv
    python deploy_airbyte.py

    # Or with explicit Airbyte URL (if running remotely with port-forward):
    AIRBYTE_URL=http://localhost:8000 python deploy_airbyte.py

Environment variables (from .env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    AIRBYTE_URL          (default: http://localhost:8000)
    AIRBYTE_EMAIL        (default: mr.danangharissetiawan@gmail.com)
    AIRBYTE_PASSWORD     (default: qj6fCwZ2oNNYi2rWQBnPvuK73u7ccQps)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("deploy_airbyte")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
AIRBYTE_URL   = os.getenv("AIRBYTE_URL", "http://localhost:8000")
AIRBYTE_EMAIL = os.getenv("AIRBYTE_EMAIL", "mr.danangharissetiawan@gmail.com")
AIRBYTE_PASS  = os.getenv("AIRBYTE_PASSWORD", "qj6fCwZ2oNNYi2rWQBnPvuK73u7ccQps")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "maritime-os")
DB_USER = os.getenv("DB_USER", "maritime-os")
DB_PASS = os.getenv("DB_PASSWORD", "")

CONNECTOR_NAME    = "Indonesia Ocean Collector"
DESTINATION_NAME  = "maritime-os PostgreSQL"
CONNECTION_NAME   = "ocean-collector → master_oceanography"

# Docker image for the custom connector (built from this repo)
# If not using Docker, set to empty string and use the declarative manifest approach
CONNECTOR_IMAGE = os.getenv("CONNECTOR_IMAGE", "")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class AirbyteClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._workspace_id: str | None = None

    def _auth_keycloak(self, email: str, password: str) -> str:
        """Get JWT token from Keycloak (works when running on the VM)."""
        token_url = f"{self.base}/auth/realms/airbyte/protocol/openid-connect/token"
        resp = self.session.post(token_url, data={
            "grant_type": "password",
            "client_id": "airbyte-webapp",
            "username": email,
            "password": password,
            "scope": "openid",
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"
        logger.info("Authenticated via Keycloak")
        return token

    def _auth_basic(self, email: str, password: str) -> None:
        """Fallback: HTTP basic auth (Airbyte OSS without Keycloak)."""
        self.session.auth = (email, password)
        logger.info("Using HTTP basic auth")

    def login(self, email: str, password: str) -> None:
        try:
            self._auth_keycloak(email, password)
        except Exception as exc:
            logger.warning("Keycloak auth failed (%s), trying basic auth", exc)
            self._auth_basic(email, password)

    def _get(self, path: str, **kwargs) -> dict:
        resp = self.session.get(f"{self.base}{path}", **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict, **kwargs) -> dict:
        resp = self.session.post(f"{self.base}{path}", json=body, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, body: dict, **kwargs) -> dict:
        resp = self.session.put(f"{self.base}{path}", json=body, **kwargs)
        resp.raise_for_status()
        return resp.json()

    # ---- Workspace ----

    def get_workspace_id(self) -> str:
        if self._workspace_id:
            return self._workspace_id
        data = self._post("/api/v1/workspaces/list", {})
        workspaces = data.get("workspaces", [])
        if not workspaces:
            raise RuntimeError("No workspaces found in Airbyte")
        self._workspace_id = workspaces[0]["workspaceId"]
        logger.info("Workspace ID: %s", self._workspace_id)
        return self._workspace_id

    # ---- Source definitions ----

    def find_source_definition(self, name: str) -> dict | None:
        workspace_id = self.get_workspace_id()
        data = self._post("/api/v1/source_definitions/list_for_workspace",
                          {"workspaceId": workspace_id})
        for sd in data.get("sourceDefinitions", []):
            if sd["name"] == name:
                return sd
        return None

    def create_custom_source_definition(self, name: str, docker_image: str,
                                         docker_tag: str = "latest") -> dict:
        workspace_id = self.get_workspace_id()
        body = {
            "workspaceId": workspace_id,
            "sourceDefinition": {
                "name": name,
                "dockerRepository": docker_image,
                "dockerImageTag": docker_tag,
                "documentationUrl": "https://github.com/rissets/indonesia-ocean-collector",
            }
        }
        result = self._post("/api/v1/source_definitions/create_custom", body)
        logger.info("Created custom source definition: %s", result.get("sourceDefinitionId"))
        return result

    # ---- Sources ----

    def find_source(self, name: str) -> dict | None:
        workspace_id = self.get_workspace_id()
        data = self._post("/api/v1/sources/list", {"workspaceId": workspace_id})
        for s in data.get("sources", []):
            if s["name"] == name:
                return s
        return None

    def create_source(self, name: str, source_definition_id: str,
                      connection_config: dict) -> dict:
        workspace_id = self.get_workspace_id()
        body = {
            "workspaceId": workspace_id,
            "name": name,
            "sourceDefinitionId": source_definition_id,
            "connectionConfiguration": connection_config,
        }
        result = self._post("/api/v1/sources/create", body)
        logger.info("Created source: %s (%s)", name, result.get("sourceId"))
        return result

    # ---- Destinations ----

    def find_destination_definition(self, name: str) -> dict | None:
        workspace_id = self.get_workspace_id()
        data = self._post("/api/v1/destination_definitions/list_for_workspace",
                          {"workspaceId": workspace_id})
        for dd in data.get("destinationDefinitions", []):
            if name.lower() in dd["name"].lower():
                return dd
        return None

    def find_destination(self, name: str) -> dict | None:
        workspace_id = self.get_workspace_id()
        data = self._post("/api/v1/destinations/list", {"workspaceId": workspace_id})
        for d in data.get("destinations", []):
            if d["name"] == name:
                return d
        return None

    def create_destination(self, name: str, destination_definition_id: str,
                           connection_config: dict) -> dict:
        workspace_id = self.get_workspace_id()
        body = {
            "workspaceId": workspace_id,
            "name": name,
            "destinationDefinitionId": destination_definition_id,
            "connectionConfiguration": connection_config,
        }
        result = self._post("/api/v1/destinations/create", body)
        logger.info("Created destination: %s (%s)", name, result.get("destinationId"))
        return result

    # ---- Connections ----

    def find_connection(self, name: str) -> dict | None:
        workspace_id = self.get_workspace_id()
        data = self._post("/api/v1/connections/list", {"workspaceId": workspace_id})
        for c in data.get("connections", []):
            if c.get("name") == name:
                return c
        return None

    def create_connection(self, source_id: str, destination_id: str,
                          name: str, streams: list[dict]) -> dict:
        body = {
            "sourceId": source_id,
            "destinationId": destination_id,
            "name": name,
            "status": "active",
            "scheduleType": "cron",
            "scheduleData": {
                "cron": {
                    "cronExpression": "0 1 * * *",  # 01:00 UTC daily
                    "cronTimeZone": "UTC",
                }
            },
            "syncCatalog": {
                "streams": streams,
            },
            "namespaceDefinition": "destination",
            "nonBreakingChangesPreference": "ignore",
        }
        result = self._post("/api/v1/connections/create", body)
        logger.info("Created connection: %s (%s)", name, result.get("connectionId"))
        return result

    def trigger_sync(self, connection_id: str) -> dict:
        result = self._post("/api/v1/connections/sync", {"connectionId": connection_id})
        logger.info("Triggered sync for connection %s: job %s", connection_id,
                    result.get("job", {}).get("id"))
        return result


# ---------------------------------------------------------------------------
# Stream catalog helpers
# ---------------------------------------------------------------------------

def _oceanography_stream_config() -> dict:
    """Airbyte stream config for master_oceanography."""
    return {
        "stream": {
            "name": "master_oceanography",
            "jsonSchema": {
                "type": "object",
                "properties": {
                    "tanggal":          {"type": ["null", "string"], "format": "date"},
                    "latitude":         {"type": ["null", "number"]},
                    "longitude":        {"type": ["null", "number"]},
                    "suhu_permukaan":   {"type": ["null", "number"]},
                    "sst":              {"type": ["null", "number"]},
                    "ssh":              {"type": ["null", "number"]},
                    "klorofil":         {"type": ["null", "number"]},
                    "arus_laut_u":      {"type": ["null", "number"]},
                    "arus_laut_v":      {"type": ["null", "number"]},
                    "kecepatan_arus":   {"type": ["null", "number"]},
                    "tinggi_gelombang": {"type": ["null", "number"]},
                    "periode_gelombang":{"type": ["null", "number"]},
                    "kecepatan_angin":  {"type": ["null", "number"]},
                    "arah_angin":       {"type": ["null", "number"]},
                    "radiasi_matahari": {"type": ["null", "number"]},
                    "cuaca":            {"type": ["null", "string"]},
                    "sumber_data":      {"type": ["null", "string"]},
                    "wpp":              {"type": ["null", "string"]},
                },
            },
            "supportedSyncModes": ["full_refresh", "incremental"],
            "sourceDefinedCursor": False,
            "defaultCursorField": ["tanggal"],
        },
        "config": {
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "cursorField": ["tanggal"],
            "primaryKey": [["tanggal"], ["latitude"], ["longitude"], ["sumber_data"]],
            "selected": True,
        },
    }


# ---------------------------------------------------------------------------
# Main deploy logic
# ---------------------------------------------------------------------------

def deploy(client: AirbyteClient) -> None:
    workspace_id = client.get_workspace_id()
    logger.info("Deploying to workspace %s", workspace_id)

    # ---- 1. Source ----
    source = client.find_source(CONNECTOR_NAME)
    if source:
        logger.info("Source '%s' already exists: %s", CONNECTOR_NAME, source["sourceId"])
        source_id = source["sourceId"]
    elif CONNECTOR_IMAGE:
        # Docker-based custom connector
        sd = client.find_source_definition(CONNECTOR_NAME)
        if not sd:
            sd = client.create_custom_source_definition(CONNECTOR_NAME, CONNECTOR_IMAGE)
        source_def_id = sd["sourceDefinitionId"]

        source_config = {
            "start_date": "2021-01-01",
            "end_date": "today",
            "sources": ["erddap", "cmems", "openmeteo"],
            "max_records_per_month": 5000,
            "db_host": DB_HOST,
            "db_port": DB_PORT,
            "db_name": DB_NAME,
            "db_user": DB_USER,
            "db_password": DB_PASS,
        }
        source = client.create_source(CONNECTOR_NAME, source_def_id, source_config)
        source_id = source["sourceId"]
    else:
        logger.warning(
            "CONNECTOR_IMAGE not set — skipping source creation.\n"
            "Set CONNECTOR_IMAGE=<your-docker-image> or use the manual setup below."
        )
        source_id = None

    # ---- 2. Destination (PostgreSQL) ----
    dest = client.find_destination(DESTINATION_NAME)
    if dest:
        logger.info("Destination '%s' already exists: %s", DESTINATION_NAME, dest["destinationId"])
        dest_id = dest["destinationId"]
    else:
        pg_def = client.find_destination_definition("postgres")
        if not pg_def:
            raise RuntimeError("PostgreSQL destination definition not found in Airbyte")
        pg_def_id = pg_def["destinationDefinitionId"]

        dest_config = {
            "host": DB_HOST,
            "port": DB_PORT,
            "database": DB_NAME,
            "username": DB_USER,
            "password": DB_PASS,
            "schema": "public",
            "ssl_mode": {"mode": "disable"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        }
        dest = client.create_destination(DESTINATION_NAME, pg_def_id, dest_config)
        dest_id = dest["destinationId"]

    # ---- 3. Connection ----
    if source_id and dest_id:
        conn = client.find_connection(CONNECTION_NAME)
        if conn:
            logger.info("Connection '%s' already exists: %s", CONNECTION_NAME, conn["connectionId"])
            connection_id = conn["connectionId"]
        else:
            streams = [_oceanography_stream_config()]
            conn = client.create_connection(source_id, dest_id, CONNECTION_NAME, streams)
            connection_id = conn["connectionId"]

        # Trigger initial sync
        logger.info("Triggering initial sync...")
        client.trigger_sync(connection_id)
        logger.info("Done. Connection ID: %s", connection_id)
        logger.info("Monitor at: %s/workspaces/%s/connections/%s",
                    AIRBYTE_URL.replace("localhost:8000", "maritime.lokakara.com"),
                    workspace_id, connection_id)
    else:
        logger.info("Destination created: %s", dest_id)
        logger.info(
            "\nManual step required:\n"
            "  1. Go to https://maritime.lokakara.com\n"
            "  2. Create a new source using 'Custom Connector' or 'Python HTTP API'\n"
            "  3. Connect it to destination '%s'\n"
            "  4. Set schedule: cron '0 1 * * *' UTC\n",
            DESTINATION_NAME,
        )


def print_manual_setup() -> None:
    """Print manual Airbyte setup instructions when API access is not available."""
    print("""
=== Manual Airbyte Setup (maritime.lokakara.com) ===

Since the Airbyte API is behind a reverse proxy, set up via the UI:

1. LOGIN
   URL:      https://maritime.lokakara.com
   Email:    mr.danangharissetiawan@gmail.com
   Password: qj6fCwZ2oNNYi2rWQBnPvuK73u7ccQps

2. CREATE DESTINATION (PostgreSQL)
   Name:     maritime-os PostgreSQL
   Host:     DB_HOST (from .env)
   Port:     15432
   Database: maritime-os
   Username: maritime-os
   Password: @Maritime210526
   Schema:   public

3. CREATE SOURCE
   Option A — Custom Python connector (requires Docker on VM):
     - Build: docker build -t ocean-collector:latest .
     - Use "Custom Connector" source type
     - Image: ocean-collector:latest

   Option B — Use existing PostgreSQL source (read from master_oceanography):
     - Source type: PostgreSQL
     - Same DB credentials as destination
     - Table: master_oceanography

4. CREATE CONNECTION
   Source → Destination
   Schedule: Cron  0 1 * * *  (UTC)  = 01:00 UTC daily
   Sync mode: Incremental | Append+Dedup
   Primary key: tanggal, latitude, longitude, sumber_data
   Cursor: tanggal

5. TRIGGER MANUAL SYNC to backfill historical data

=== Alternative: Run batch_collect.py directly on VM ===

  ssh <vm>
  cd /path/to/indonesia-ocean-collector
  source .venv/bin/activate
  python batch_collect.py --start 2021-01-01   # backfill
  python scheduler.py                           # start daily cron at 01:00 UTC
""")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Deploy ocean collector to Airbyte")
    parser.add_argument("--manual", action="store_true",
                        help="Print manual setup instructions instead of using API")
    parser.add_argument("--url", default=AIRBYTE_URL, help="Airbyte base URL")
    args = parser.parse_args()

    if args.manual:
        print_manual_setup()
        sys.exit(0)

    client = AirbyteClient(args.url)
    try:
        client.login(AIRBYTE_EMAIL, AIRBYTE_PASS)
        deploy(client)
    except requests.exceptions.ConnectionError as exc:
        logger.error("Cannot connect to Airbyte at %s: %s", args.url, exc)
        logger.info("If running remotely, set up an SSH tunnel first:")
        logger.info("  ssh -L 8000:localhost:8000 <vm-user>@<vm-host>")
        logger.info("Then re-run: AIRBYTE_URL=http://localhost:8000 python deploy_airbyte.py")
        logger.info("")
        logger.info("Or run with --manual for UI setup instructions:")
        logger.info("  python deploy_airbyte.py --manual")
        sys.exit(1)
    except Exception as exc:
        logger.error("Deploy failed: %s", exc)
        logger.info("Run with --manual for UI setup instructions")
        sys.exit(1)
