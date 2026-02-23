import os
import threading
import time
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
import requests

from app.harvester import (
    apply_incremental_harvest,
    build_dcat_catalog,
    build_stac_item_collection,
    collect_metadata,
    load_json_file,
    login_and_get_token,
    save_json_file,
)

app = FastAPI(title="SensorThings Metadata API", version="1.0.0")

_LOCK = threading.Lock()
_LAST_REFRESH_TS = 0.0
_LAST_INCREMENTAL_STATS: Dict[str, int] = {}


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _get_config() -> Dict[str, Any]:
    return {
        "endpoint": _env("METADATA_ENDPOINT", "http://localhost:8018/istsos4/v1.1"),
        "token": _env("METADATA_TOKEN", ""),
        "username": _env("METADATA_USERNAME", ""),
        "password": _env("METADATA_PASSWORD", ""),
        "timeout": float(_env("METADATA_TIMEOUT", "30")),
        "metadata_output": _env("METADATA_OUTPUT", "metadata.json"),
        "stac_output": _env("STAC_OUTPUT", "stac_items.json"),
        "dcat_output": _env("DCAT_OUTPUT", "dcat_catalog.json"),
        "state_file": _env("METADATA_STATE_FILE", "metadata_state.json"),
        "stac_collection_id": _env("STAC_COLLECTION_ID", "istsos-datastreams"),
        "stac_root_href": _env("STAC_ROOT_HREF", "http://localhost:8020/stac"),
        "incremental": _env("METADATA_INCREMENTAL", "1") == "1",
        "harvest_interval_seconds": int(_env("HARVEST_INTERVAL_SECONDS", "300")),
    }


def _resolve_token(config: Dict[str, Any]) -> str:
    token = config["token"]
    if token:
        return token

    username = config["username"]
    password = config["password"]
    if username and password:
        return login_and_get_token(
            endpoint=config["endpoint"],
            username=username,
            password=password,
            timeout=config["timeout"],
        )

    return ""


def refresh_metadata(force: bool = False) -> None:
    global _LAST_REFRESH_TS, _LAST_INCREMENTAL_STATS

    config = _get_config()
    now = time.time()
    if not force and _LAST_REFRESH_TS and now - _LAST_REFRESH_TS < config["harvest_interval_seconds"]:
        return

    with _LOCK:
        now = time.time()
        if not force and _LAST_REFRESH_TS and now - _LAST_REFRESH_TS < config["harvest_interval_seconds"]:
            return

        try:
            token = _resolve_token(config)
            records = collect_metadata(
                endpoint=config["endpoint"], token=token or None, timeout=config["timeout"]
            )
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"Harvest failed: {exc}") from exc

        if config["incremental"]:
            previous_records = load_json_file(config["metadata_output"], [])
            state_payload = load_json_file(config["state_file"], {"signatures": {}})
            previous_signatures = state_payload.get("signatures", {})
            if not isinstance(previous_signatures, dict):
                previous_signatures = {}

            records, signatures, stats = apply_incremental_harvest(
                current_records=records,
                previous_records=previous_records if isinstance(previous_records, list) else [],
                previous_signatures=previous_signatures,
            )
            save_json_file(config["state_file"], {"signatures": signatures})
            _LAST_INCREMENTAL_STATS = stats
        else:
            _LAST_INCREMENTAL_STATS = {
                "created": len(records),
                "updated": 0,
                "unchanged": 0,
                "total": len(records),
            }

        stac = build_stac_item_collection(
            records=records,
            collection_id=config["stac_collection_id"],
            root_href=config["stac_root_href"],
        )
        dcat = build_dcat_catalog(records)

        save_json_file(config["metadata_output"], records)
        save_json_file(config["stac_output"], stac)
        save_json_file(config["dcat_output"], dcat)

        _LAST_REFRESH_TS = time.time()


def _ensure_data() -> None:
    refresh_metadata(force=False)


@app.get("/datasets")
def get_datasets() -> Dict[str, Any]:
    _ensure_data()
    config = _get_config()
    records: List[Dict[str, Any]] = load_json_file(config["metadata_output"], [])
    return {
        "count": len(records),
        "records": records,
        "incremental": _LAST_INCREMENTAL_STATS,
    }


@app.get("/stac/items")
def get_stac_items() -> Dict[str, Any]:
    _ensure_data()
    config = _get_config()
    return load_json_file(config["stac_output"], {"type": "FeatureCollection", "features": []})


@app.get("/dcat/catalog")
def get_dcat_catalog() -> Dict[str, Any]:
    _ensure_data()
    config = _get_config()
    return load_json_file(config["dcat_output"], {"@type": "dcat:Catalog", "dcat:dataset": []})
