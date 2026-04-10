import os
import threading
import time
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
import requests

from app.harvester import (
    apply_incremental_harvest,
    build_dcat_catalog,
    build_stac_catalog,
    build_stac_collection,
    build_stac_collections,
    build_stac_conformance,
    build_stac_item_map,
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
        "stac_output": _env("STAC_OUTPUT", "stac_catalog.json"),
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

        stac = build_stac_catalog(
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


def _load_records() -> List[Dict[str, Any]]:
    _ensure_data()
    config = _get_config()
    records = load_json_file(config["metadata_output"], [])
    if isinstance(records, list):
        return records
    return []


def _build_stac_resources() -> Dict[str, Any]:
    config = _get_config()
    records = _load_records()
    collection_id = config["stac_collection_id"]
    root_href = config["stac_root_href"]
    return {
        "catalog": build_stac_catalog(records, collection_id=collection_id, root_href=root_href),
        "collections": build_stac_collections(records, collection_id=collection_id, root_href=root_href),
        "collection": build_stac_collection(records, collection_id=collection_id, root_href=root_href),
        "items": build_stac_item_collection(records, collection_id=collection_id, root_href=root_href),
        "item_map": build_stac_item_map(records, collection_id=collection_id, root_href=root_href),
        "collection_id": collection_id,
    }


def _filter_items(
    item_map: Dict[str, Dict[str, Any]],
    collection_id: str,
    collections: Optional[List[str]] = None,
    ids: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    observed_property: Optional[str] = None,
    thing_id: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    items = list(item_map.values())

    if collections:
        allowed = set(collections)
        items = [item for item in items if item.get("collection") in allowed]
    else:
        items = [item for item in items if item.get("collection") == collection_id]

    if ids:
        allowed_ids = set(ids)
        items = [item for item in items if item.get("id") in allowed_ids]

    if bbox and len(bbox) == 4:
        minx, miny, maxx, maxy = bbox

        def _bbox_matches(item: Dict[str, Any]) -> bool:
            item_bbox = item.get("bbox")
            if not isinstance(item_bbox, list) or len(item_bbox) != 4:
                return False
            return (
                item_bbox[0] >= minx
                and item_bbox[1] >= miny
                and item_bbox[2] <= maxx
                and item_bbox[3] <= maxy
            )

        items = [item for item in items if _bbox_matches(item)]

    if observed_property:
        expected = observed_property.strip().lower()
        items = [
            item
            for item in items
            if str(item.get("properties", {}).get("observed_property", "")).lower() == expected
        ]

    if thing_id:
        items = [
            item
            for item in items
            if str(item.get("properties", {}).get("thing_id", "")) == str(thing_id)
        ]

    number_matched = len(items)
    limited_items = items[: max(limit, 1)]
    config = _get_config()
    root_href = config["stac_root_href"].rstrip("/")
    collection_href = f"{root_href}/collections/{collection_id}"
    return {
        "type": "FeatureCollection",
        "links": [
            {"rel": "self", "href": f"{root_href}/search", "type": "application/geo+json"},
            {"rel": "root", "href": root_href, "type": "application/json"},
            {"rel": "parent", "href": collection_href, "type": "application/json"},
            {"rel": "collection", "href": collection_href, "type": "application/json"},
        ],
        "numberMatched": number_matched,
        "numberReturned": len(limited_items),
        "features": limited_items,
    }


@app.get("/datasets")
def get_datasets() -> Dict[str, Any]:
    records = _load_records()
    return {
        "count": len(records),
        "records": records,
        "incremental": _LAST_INCREMENTAL_STATS,
    }


@app.get("/stac")
def get_stac_catalog() -> Dict[str, Any]:
    return _build_stac_resources()["catalog"]


@app.get("/stac/conformance")
def get_stac_conformance() -> Dict[str, Any]:
    _ensure_data()
    return build_stac_conformance()


@app.get("/stac/collections")
def get_stac_collections() -> Dict[str, Any]:
    return _build_stac_resources()["collections"]


@app.get("/stac/collections/{collection_id}")
def get_stac_collection(collection_id: str) -> Dict[str, Any]:
    resources = _build_stac_resources()
    if collection_id != resources["collection_id"]:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")
    return resources["collection"]


@app.get("/stac/collections/{collection_id}/items")
def get_stac_collection_items(collection_id: str) -> Dict[str, Any]:
    resources = _build_stac_resources()
    if collection_id != resources["collection_id"]:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")
    return resources["items"]


@app.get("/stac/collections/{collection_id}/items/{item_id}")
def get_stac_item(collection_id: str, item_id: str) -> Dict[str, Any]:
    resources = _build_stac_resources()
    if collection_id != resources["collection_id"]:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")
    item = resources["item_map"].get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found")
    return item


@app.get("/stac/items")
def get_stac_items() -> Dict[str, Any]:
    return _build_stac_resources()["items"]


@app.get("/stac/search")
def search_stac_items(
    collections: Optional[List[str]] = Query(default=None),
    ids: Optional[List[str]] = Query(default=None),
    bbox: Optional[List[float]] = Query(default=None),
    observed_property: Optional[str] = None,
    thing_id: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    resources = _build_stac_resources()
    return _filter_items(
        item_map=resources["item_map"],
        collection_id=resources["collection_id"],
        collections=collections,
        ids=ids,
        bbox=bbox,
        observed_property=observed_property,
        thing_id=thing_id,
        limit=limit,
    )


@app.post("/stac/search")
def search_stac_items_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    resources = _build_stac_resources()
    return _filter_items(
        item_map=resources["item_map"],
        collection_id=resources["collection_id"],
        collections=payload.get("collections"),
        ids=payload.get("ids"),
        bbox=payload.get("bbox"),
        observed_property=payload.get("observed_property"),
        thing_id=payload.get("thing_id"),
        limit=int(payload.get("limit", 10)),
    )


@app.get("/dcat/catalog")
def get_dcat_catalog_view() -> Dict[str, Any]:
    _ensure_data()
    config = _get_config()
    return load_json_file(config["dcat_output"], {"@type": "dcat:Catalog", "dcat:dataset": []})
