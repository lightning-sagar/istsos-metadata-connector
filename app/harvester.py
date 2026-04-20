import argparse
import getpass
import json
from pathlib import Path
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect SensorThings metadata by expanding related entities in a single request."
        )
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8018/istsos4/v1.1",
        help="SensorThings API base endpoint",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Optional Bearer token for authenticated endpoints",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Optional username for IST SOS login",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Optional password for IST SOS login",
    )
    parser.add_argument(
        "--ask-login",
        action="store_true",
        help="Prompt for username/password if token is not provided",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output JSON file path. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--stac-output",
        default=None,
        help="Optional output path for STAC landing page/catalog JSON.",
    )
    parser.add_argument(
        "--dcat-output",
        default=None,
        help="Optional output path for DCAT JSON-LD catalog.",
    )
    parser.add_argument(
        "--dcat-catalog-url",
        default=None,
        help="Canonical URL or URI for the generated DCAT catalog.",
    )
    parser.add_argument(
        "--dcat-catalog-title",
        default="istSOS DCAT Catalog",
        help="Title for the generated DCAT catalog.",
    )
    parser.add_argument(
        "--dcat-catalog-description",
        default=(
            "DCAT-AP-style catalog generated from harvested istSOS SensorThings datastream metadata."
        ),
        help="Description for the generated DCAT catalog.",
    )
    parser.add_argument(
        "--dcat-homepage",
        default=None,
        help="Homepage URL advertised by the generated DCAT catalog.",
    )
    parser.add_argument(
        "--dcat-language",
        default="en",
        help="Language tag to attach to the generated DCAT catalog and datasets.",
    )
    parser.add_argument(
        "--dcat-theme-taxonomy",
        default=None,
        help="Optional theme taxonomy URI for the generated DCAT catalog.",
    )
    parser.add_argument(
        "--dcat-publisher-name",
        default="istSOS Metadata Connector",
        help="Publisher name for the generated DCAT catalog and datasets.",
    )
    parser.add_argument(
        "--dcat-publisher-homepage",
        default=None,
        help="Publisher homepage URL for the generated DCAT catalog and datasets.",
    )
    parser.add_argument(
        "--dcat-publisher-mbox",
        default=None,
        help="Publisher mailbox for the generated DCAT catalog and datasets.",
    )
    parser.add_argument(
        "--stac-collection-id",
        default="istsos-datastreams",
        help="STAC collection ID to embed in each STAC item.",
    )
    parser.add_argument(
        "--stac-root-href",
        default=None,
        help="Base STAC root URL used to build item self/root links.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only update records that changed since the previous harvest.",
    )
    parser.add_argument(
        "--state-file",
        default="metadata_state.json",
        help="State file used for incremental harvest signatures.",
    )
    return parser.parse_args()


def load_json_file(file_path: str, default: Any) -> Any:
    path = Path(file_path)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def save_json_file(file_path: str, payload: Any) -> None:
    path = Path(file_path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def record_signature(record: Dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, ensure_ascii=False)


def apply_incremental_harvest(
    current_records: List[Dict[str, Any]],
    previous_records: List[Dict[str, Any]],
    previous_signatures: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, int]]:
    previous_by_id: Dict[str, Dict[str, Any]] = {}
    for record in previous_records:
        datastream_id = record.get("datastream_id")
        if datastream_id is not None:
            previous_by_id[str(datastream_id)] = record

    final_records: List[Dict[str, Any]] = []
    signatures: Dict[str, str] = {}
    created = 0
    updated = 0
    unchanged = 0

    for record in current_records:
        datastream_id = record.get("datastream_id")
        if datastream_id is None:
            continue

        key = str(datastream_id)
        current_signature = record_signature(record)
        old_signature = previous_signatures.get(key)

        if old_signature == current_signature and key in previous_by_id:
            final_records.append(previous_by_id[key])
            unchanged += 1
        else:
            final_records.append(record)
            if key in previous_signatures:
                updated += 1
            else:
                created += 1

        signatures[key] = current_signature

    stats = {
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "total": len(final_records),
    }
    return final_records, signatures, stats


def _get_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def login_and_get_token(
    endpoint: str,
    username: str,
    password: str,
    timeout: float,
) -> str:
    response = requests.post(
        f"{endpoint.rstrip('/')}/Login",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"username": username, "password": password, "grant_type": "password"},
        timeout=timeout,
    )
    response.raise_for_status()

    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise requests.RequestException("Login succeeded but no access_token was returned")
    return token


def _extract_time_range(datastream: Dict[str, Any]) -> str:
    phenomenon_time = datastream.get("phenomenonTime")
    if isinstance(phenomenon_time, str) and phenomenon_time:
        return phenomenon_time

    result_time = datastream.get("resultTime")
    if isinstance(result_time, str) and result_time:
        return result_time

    return ""


def _extract_start_end(time_range: str) -> Tuple[str, str]:
    if not isinstance(time_range, str) or "/" not in time_range:
        return "", ""
    start_time, end_time = time_range.split("/", 1)
    return start_time, end_time


def _extract_location(thing: Dict[str, Any]) -> Optional[Dict[str, float]]:
    locations = thing.get("Locations") or []
    if not locations:
        return None

    first_location = locations[0]
    location_obj = first_location.get("location")
    if isinstance(location_obj, dict):
        coordinates = location_obj.get("coordinates")
        if (
            isinstance(coordinates, list)
            and len(coordinates) >= 2
            and isinstance(coordinates[0], (int, float))
            and isinstance(coordinates[1], (int, float))
        ):
            return {"lat": float(coordinates[1]), "lon": float(coordinates[0])}

    return None


def _extract_unit(datastream: Dict[str, Any]) -> str:
    unit = datastream.get("unitOfMeasurement")
    if not isinstance(unit, dict):
        return ""
    symbol = unit.get("symbol")
    if isinstance(symbol, str) and symbol:
        return symbol
    name = unit.get("name")
    if isinstance(name, str):
        return name
    return ""


def _extract_sampling_frequency(datastream: Dict[str, Any]) -> str:
    properties = datastream.get("properties")
    if not isinstance(properties, dict):
        return ""
    for key in ("sampling_frequency", "samplingFrequency", "frequency"):
        value = properties.get(key)
        if value is not None:
            return str(value)
    return ""


def _record_from_datastream(thing: Dict[str, Any], datastream: Dict[str, Any]) -> Dict[str, Any]:
    sensor = datastream.get("Sensor") or {}
    observed_property = datastream.get("ObservedProperty") or {}
    time_range = _extract_time_range(datastream)
    start_time, end_time = _extract_start_end(time_range)
    thing_description = thing.get("description", "")
    datastream_description = datastream.get("description", "")

    return {
        "thing_id": thing.get("@iot.id"),
        "thing_name": thing.get("name", ""),
        "datastream_name": datastream.get("name", ""),
        "description": datastream_description or thing_description,
        "location": _extract_location(thing),
        "sensor_type": sensor.get("name", ""),
        "observed_property": observed_property.get("name", ""),
        "unit_of_measurement": _extract_unit(datastream),
        "observation_type": datastream.get("observationType", ""),
        "sampling_frequency": _extract_sampling_frequency(datastream),
        "time_range": time_range,
        "start_time": start_time,
        "end_time": end_time,
        "last_observation_time": end_time,
        "datastream_id": datastream.get("@iot.id"),
    }


def fetch_things_with_expand(
    endpoint: str,
    token: Optional[str],
    timeout: float,
) -> List[Dict[str, Any]]:
    url = (
        f"{endpoint.rstrip('/')}/Things"
        "?$expand=Locations,Datastreams($expand=Sensor,ObservedProperty)"
    )

    headers = _get_headers(token)
    things: List[Dict[str, Any]] = []

    while url:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

        things.extend(payload.get("value", []))
        url = payload.get("@iot.nextLink")

    return things


def collect_metadata(
    endpoint: str,
    token: Optional[str],
    timeout: float,
) -> List[Dict[str, Any]]:
    things = fetch_things_with_expand(endpoint=endpoint, token=token, timeout=timeout)
    records: List[Dict[str, Any]] = []

    for thing in things:
        datastreams = thing.get("Datastreams") or []
        if not datastreams:
            continue

        for datastream in datastreams:
            records.append(_record_from_datastream(thing, datastream))

    return records


STAC_CONFORMANCE_CLASSES = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/collections",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/collections",
]


def _normalize_root_href(root_href: str) -> str:
    return root_href.rstrip("/")


def _item_id(record: Dict[str, Any]) -> Optional[str]:
    datastream_id = record.get("datastream_id")
    if datastream_id is None:
        return None
    return f"datastream-{datastream_id}"


def _build_item_geometry(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[List[float]]]:
    location = record.get("location")
    if isinstance(location, dict) and "lat" in location and "lon" in location:
        lat = location["lat"]
        lon = location["lon"]
        return {"type": "Point", "coordinates": [lon, lat]}, [lon, lat, lon, lat]
    return None, None


def _build_stac_item(record: Dict[str, Any], collection_id: str, root_href: str) -> Optional[Dict[str, Any]]:
    item_id = _item_id(record)
    if item_id is None:
        return None

    normalized_root = _normalize_root_href(root_href)
    collection_href = f"{normalized_root}/collections/{collection_id}"
    item_self_href = f"{collection_href}/items/{item_id}"
    geometry, bbox = _build_item_geometry(record)

    properties = {
        "title": record.get("datastream_name") or record.get("thing_name", ""),
        "description": record.get("description", ""),
        "thing_id": record.get("thing_id"),
        "thing_name": record.get("thing_name", ""),
        "sensor_type": record.get("sensor_type", ""),
        "observed_property": record.get("observed_property", ""),
        "unit_of_measurement": record.get("unit_of_measurement", ""),
        "observation_type": record.get("observation_type", ""),
        "sampling_frequency": record.get("sampling_frequency", ""),
        "time_range": record.get("time_range", ""),
        "datastream_id": record.get("datastream_id"),
        "start_datetime": record.get("start_time") or None,
        "end_datetime": record.get("end_time") or None,
        "datetime": record.get("last_observation_time") or None,
    }

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "collection": collection_id,
        "bbox": bbox,
        "geometry": geometry,
        "properties": properties,
        "links": [
            {"rel": "self", "href": item_self_href, "type": "application/geo+json"},
            {"rel": "root", "href": normalized_root, "type": "application/json"},
            {"rel": "parent", "href": collection_href, "type": "application/json"},
            {"rel": "collection", "href": collection_href, "type": "application/json"},
        ],
        "assets": {},
    }


def _compute_extent(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    boxes: List[List[float]] = []
    starts: List[str] = []
    ends: List[str] = []

    for record in records:
        _, bbox = _build_item_geometry(record)
        if bbox:
            boxes.append(bbox)
        start_time = record.get("start_time")
        end_time = record.get("end_time")
        if isinstance(start_time, str) and start_time:
            starts.append(start_time)
        if isinstance(end_time, str) and end_time:
            ends.append(end_time)

    spatial_bbox = [[-180.0, -90.0, 180.0, 90.0]]
    if boxes:
        spatial_bbox = [[
            min(box[0] for box in boxes),
            min(box[1] for box in boxes),
            max(box[2] for box in boxes),
            max(box[3] for box in boxes),
        ]]

    temporal_interval = [[None, None]]
    if starts or ends:
        temporal_interval = [[min(starts) if starts else None, max(ends) if ends else None]]

    return {
        "spatial": {"bbox": spatial_bbox},
        "temporal": {"interval": temporal_interval},
    }


def _collection_summaries(records: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    summaries: Dict[str, List[str]] = {}
    for field in ("sensor_type", "observed_property", "unit_of_measurement", "thing_name"):
        values = sorted(
            {
                str(record.get(field))
                for record in records
                if isinstance(record.get(field), str) and str(record.get(field)).strip()
            }
        )
        if values:
            summaries[field] = values
    return summaries


def build_stac_collection(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Any]:
    normalized_root = _normalize_root_href(root_href)
    collection_href = f"{normalized_root}/collections/{collection_id}"

    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": collection_id,
        "title": "istSOS Datastream Metadata",
        "description": (
            "STAC collection exposing istSOS SensorThings datastream metadata "
            "as geospatially discoverable items."
        ),
        "license": "proprietary",
        "extent": _compute_extent(records),
        "links": [
            {"rel": "self", "href": collection_href, "type": "application/json"},
            {"rel": "root", "href": normalized_root, "type": "application/json"},
            {"rel": "parent", "href": normalized_root, "type": "application/json"},
            {"rel": "items", "href": f"{collection_href}/items", "type": "application/geo+json"},
        ],
        "summaries": _collection_summaries(records),
        "item_assets": {},
    }


def build_stac_item_collection(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []
    normalized_root = _normalize_root_href(root_href)
    collection_href = f"{normalized_root}/collections/{collection_id}"

    for record in records:
        item = _build_stac_item(record, collection_id=collection_id, root_href=root_href)
        if item is not None:
            features.append(item)

    return {
        "type": "FeatureCollection",
        "links": [
            {"rel": "self", "href": f"{collection_href}/items", "type": "application/geo+json"},
            {"rel": "root", "href": normalized_root, "type": "application/json"},
            {"rel": "parent", "href": collection_href, "type": "application/json"},
            {"rel": "collection", "href": collection_href, "type": "application/json"},
        ],
        "numberMatched": len(features),
        "numberReturned": len(features),
        "features": features,
    }


def build_stac_catalog(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Any]:
    normalized_root = _normalize_root_href(root_href)
    collection = build_stac_collection(records, collection_id=collection_id, root_href=root_href)

    return {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "istsos-stac-catalog",
        "title": "istSOS STAC API",
        "description": (
            "Landing page for harvested istSOS SensorThings metadata exposed "
            "through STAC collections and item discovery endpoints."
        ),
        "conformsTo": STAC_CONFORMANCE_CLASSES,
        "links": [
            {"rel": "self", "href": normalized_root, "type": "application/json"},
            {"rel": "root", "href": normalized_root, "type": "application/json"},
            {"rel": "data", "href": f"{normalized_root}/collections", "type": "application/json"},
            {
                "rel": "conformance",
                "href": f"{normalized_root}/conformance",
                "type": "application/json",
            },
            {"rel": "search", "href": f"{normalized_root}/search", "type": "application/geo+json"},
            {"rel": "child", "href": collection["links"][0]["href"], "type": "application/json"},
        ],
    }


def build_stac_collections(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Any]:
    normalized_root = _normalize_root_href(root_href)
    collection = build_stac_collection(records, collection_id=collection_id, root_href=root_href)
    return {
        "collections": [collection],
        "links": [
            {"rel": "self", "href": f"{normalized_root}/collections", "type": "application/json"},
            {"rel": "root", "href": normalized_root, "type": "application/json"},
            {"rel": "parent", "href": normalized_root, "type": "application/json"},
        ],
    }


def build_stac_conformance() -> Dict[str, Any]:
    return {"conformsTo": STAC_CONFORMANCE_CLASSES}


def build_stac_item_map(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Dict[str, Any]]:
    items: Dict[str, Dict[str, Any]] = {}
    for record in records:
        item = _build_stac_item(record, collection_id=collection_id, root_href=root_href)
        if item is not None:
            items[item["id"]] = item
    return items


def _as_non_empty_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value)


def _unique_strings(values: Sequence[Any]) -> List[str]:
    seen = set()
    output: List[str] = []
    for value in values:
        string_value = _as_non_empty_string(value)
        if string_value is None:
            continue
        if string_value in seen:
            continue
        seen.add(string_value)
        output.append(string_value)
    return output


def _sensor_things_entity_url(endpoint: Optional[str], entity_path: str) -> Optional[str]:
    endpoint_value = _as_non_empty_string(endpoint)
    if endpoint_value is None:
        return None
    return f"{endpoint_value.rstrip('/')}/{entity_path.lstrip('/')}"


def _build_publisher(name: Any, homepage: Any = None, mbox: Any = None) -> Optional[Dict[str, Any]]:
    publisher_name = _as_non_empty_string(name)
    if publisher_name is None:
        return None

    publisher: Dict[str, Any] = {
        "@type": "foaf:Agent",
        "foaf:name": publisher_name,
    }

    homepage_value = _as_non_empty_string(homepage)
    if homepage_value is not None:
        publisher["foaf:homepage"] = {"@id": homepage_value}

    mbox_value = _as_non_empty_string(mbox)
    if mbox_value is not None:
        publisher["foaf:mbox"] = mbox_value if mbox_value.startswith("mailto:") else f"mailto:{mbox_value}"

    return publisher


def _build_temporal_coverage(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start_time = _as_non_empty_string(record.get("start_time"))
    end_time = _as_non_empty_string(record.get("end_time"))
    if start_time is None and end_time is None:
        return None

    temporal: Dict[str, Any] = {"@type": "dct:PeriodOfTime"}
    if start_time is not None:
        temporal["schema:startDate"] = start_time
    if end_time is not None:
        temporal["schema:endDate"] = end_time
    return temporal


def _build_spatial_coverage(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    location = record.get("location")
    if not isinstance(location, dict) or "lat" not in location or "lon" not in location:
        return None

    return {
        "@type": "dct:Location",
        "dct:conformsTo": {"@id": "http://www.opengis.net/def/crs/EPSG/0/4326"},
        "locn:geometry": {
            "type": "Point",
            "coordinates": [location["lon"], location["lat"]],
        },
    }


def _build_dataset_distributions(
    record: Dict[str, Any],
    dataset_id: str,
    endpoint: Optional[str],
) -> List[Dict[str, Any]]:
    datastream_id = record.get("datastream_id")
    if datastream_id is None:
        return []

    datastream_path = f"Datastreams({quote(str(datastream_id), safe='')})"
    datastream_url = _sensor_things_entity_url(endpoint, datastream_path)
    observations_url = _sensor_things_entity_url(endpoint, f"{datastream_path}/Observations")
    if datastream_url is None and observations_url is None:
        return []

    distributions: List[Dict[str, Any]] = []

    if datastream_url is not None:
        distributions.append(
            {
                "@id": f"{dataset_id}#datastream",
                "@type": "dcat:Distribution",
                "dct:title": "SensorThings Datastream resource",
                "dct:description": "Source datastream metadata exposed through the OGC SensorThings API.",
                "dcat:accessURL": {"@id": datastream_url},
                "dcat:mediaType": "application/json",
                "dct:format": "application/json",
                "dct:conformsTo": {"@id": "http://www.opengis.net/doc/IS/SensorThingsAPI/1.1"},
            }
        )

    if observations_url is not None:
        distributions.append(
            {
                "@id": f"{dataset_id}#observations",
                "@type": "dcat:Distribution",
                "dct:title": "SensorThings observations feed",
                "dct:description": "Observations associated with the harvested datastream.",
                "dcat:accessURL": {"@id": observations_url},
                "dcat:downloadURL": {"@id": observations_url},
                "dcat:mediaType": "application/json",
                "dct:format": "application/json",
                "dct:conformsTo": {"@id": "http://www.opengis.net/doc/IS/SensorThingsAPI/1.1"},
            }
        )

    return distributions


def build_dcat_catalog(
    records: List[Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metadata = metadata or {}
    catalog_id = _as_non_empty_string(metadata.get("catalog_url")) or "urn:catalog:istsos:dcat"
    catalog_title = _as_non_empty_string(metadata.get("title")) or "istSOS DCAT Catalog"
    catalog_description = _as_non_empty_string(metadata.get("description")) or (
        "DCAT-AP-style catalog generated from harvested istSOS SensorThings datastream metadata."
    )
    endpoint = _as_non_empty_string(metadata.get("endpoint"))
    homepage = _as_non_empty_string(metadata.get("homepage")) or endpoint
    language = _as_non_empty_string(metadata.get("language"))
    theme_taxonomy = _as_non_empty_string(metadata.get("theme_taxonomy"))
    publisher = _build_publisher(
        metadata.get("publisher_name"),
        homepage=metadata.get("publisher_homepage") or homepage,
        mbox=metadata.get("publisher_mbox"),
    )

    datasets: List[Dict[str, Any]] = []
    for record in records:
        if record.get("datastream_id") is None:
            continue
        dataset_id = f"datastream-{record.get('datastream_id')}"
        temporal = _build_temporal_coverage(record)
        spatial = _build_spatial_coverage(record)
        issued = _as_non_empty_string(record.get("start_time"))
        modified = _as_non_empty_string(record.get("last_observation_time")) or _as_non_empty_string(
            record.get("end_time")
        )
        landing_page = _sensor_things_entity_url(
            endpoint, f"Datastreams({quote(str(record.get('datastream_id')), safe='')})"
        )
        dataset: Dict[str, Any] = {
            "@id": dataset_id,
            "@type": "dcat:Dataset",
            "dct:identifier": dataset_id,
            "dct:title": record.get("datastream_name") or record.get("thing_name", ""),
            "dct:description": record.get("description", ""),
            "dcat:keyword": _unique_strings(
                [
                    record.get("observed_property"),
                    record.get("sensor_type"),
                    record.get("thing_name"),
                    record.get("unit_of_measurement"),
                ]
            ),
        }

        if temporal is not None:
            dataset["dct:temporal"] = temporal
        if spatial is not None:
            dataset["dct:spatial"] = spatial
        if issued is not None:
            dataset["dct:issued"] = issued
        if modified is not None:
            dataset["dct:modified"] = modified
        if landing_page is not None:
            dataset["dcat:landingPage"] = {"@id": landing_page}
        if language is not None:
            dataset["dct:language"] = language
        if publisher is not None:
            dataset["dct:publisher"] = publisher

        distribution_list = _build_dataset_distributions(record, dataset_id=dataset_id, endpoint=endpoint)
        if distribution_list:
            dataset["dcat:distribution"] = distribution_list

        sampling_frequency = _as_non_empty_string(record.get("sampling_frequency"))
        if sampling_frequency is not None:
            dataset["dct:accrualPeriodicity"] = sampling_frequency

        observation_type = _as_non_empty_string(record.get("observation_type"))
        if observation_type is not None:
            dataset["dct:conformsTo"] = {"@id": observation_type}

        datasets.append(dataset)

    catalog: Dict[str, Any] = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "locn": "http://www.w3.org/ns/locn#",
            "schema": "https://schema.org/",
            "foaf": "http://xmlns.com/foaf/0.1/",
        },
        "@id": catalog_id,
        "@type": "dcat:Catalog",
        "dct:title": catalog_title,
        "dct:description": catalog_description,
        "dcat:dataset": datasets,
    }

    if homepage is not None:
        catalog["foaf:homepage"] = {"@id": homepage}
    if publisher is not None:
        catalog["dct:publisher"] = publisher
    if language is not None:
        catalog["dct:language"] = language
    if theme_taxonomy is not None:
        catalog["dcat:themeTaxonomy"] = {"@id": theme_taxonomy}

    return catalog


def main() -> Tuple[int, str]:
    args = parse_args()
    token = args.token

    if not token and (args.username or args.password):
        if not args.username or not args.password:
            return 1, "Both --username and --password are required when using credential login"
        try:
            token = login_and_get_token(
                endpoint=args.endpoint,
                username=args.username,
                password=args.password,
                timeout=args.timeout,
            )
        except requests.RequestException as exc:
            return 1, f"Login error: {exc}"

    if not token and args.ask_login:
        try:
            username = input("Enter your istsos username: ").strip()
            password = getpass.getpass("Enter your istsos password: ")
        except EOFError:
            return 1, "No interactive input available for login prompt"
        if not username or not password:
            return 1, "Username or password is empty"
        try:
            token = login_and_get_token(
                endpoint=args.endpoint,
                username=username,
                password=password,
                timeout=args.timeout,
            )
        except requests.RequestException as exc:
            return 1, f"Login error: {exc}"

    try:
        records = collect_metadata(
            endpoint=args.endpoint,
            token=token,
            timeout=args.timeout,
        )
    except requests.RequestException as exc:
        if getattr(exc.response, "status_code", None) == 401 and not token:
            try:
                username = input("Unauthorized. Enter your istsos username: ").strip()
                password = getpass.getpass("Enter your istsos password: ")
            except EOFError:
                return 1, f"Request error: {exc}"
            if not username or not password:
                return 1, "Username or password is empty"
            try:
                token = login_and_get_token(
                    endpoint=args.endpoint,
                    username=username,
                    password=password,
                    timeout=args.timeout,
                )
                records = collect_metadata(
                    endpoint=args.endpoint,
                    token=token,
                    timeout=args.timeout,
                )
            except requests.RequestException as login_exc:
                return 1, f"Login/request error: {login_exc}"
        else:
            return 1, f"Request error: {exc}"

    incremental_stats: Optional[Dict[str, int]] = None
    if args.incremental:
        if not args.output:
            return 1, "--incremental requires --output to persist and compare previous metadata"
        previous_records = load_json_file(args.output, [])
        state_payload = load_json_file(args.state_file, {"signatures": {}})
        previous_signatures = state_payload.get("signatures", {})
        if not isinstance(previous_signatures, dict):
            previous_signatures = {}

        records, signatures, incremental_stats = apply_incremental_harvest(
            current_records=records,
            previous_records=previous_records if isinstance(previous_records, list) else [],
            previous_signatures=previous_signatures,
        )
        save_json_file(args.state_file, {"signatures": signatures})

    output = json.dumps(records, indent=2, ensure_ascii=False)

    if args.stac_output:
        stac_root_href = args.stac_root_href or f"{args.endpoint.rstrip('/')}/stac"
        stac_catalog = build_stac_catalog(
            records,
            collection_id=args.stac_collection_id,
            root_href=stac_root_href,
        )
        with open(args.stac_output, "w", encoding="utf-8") as file:
            json.dump(stac_catalog, file, indent=2, ensure_ascii=False)
            file.write("\n")

    if args.dcat_output:
        dcat_catalog = build_dcat_catalog(
            records,
            metadata={
                "catalog_url": args.dcat_catalog_url,
                "title": args.dcat_catalog_title,
                "description": args.dcat_catalog_description,
                "homepage": args.dcat_homepage,
                "endpoint": args.endpoint,
                "language": args.dcat_language,
                "theme_taxonomy": args.dcat_theme_taxonomy,
                "publisher_name": args.dcat_publisher_name,
                "publisher_homepage": args.dcat_publisher_homepage,
                "publisher_mbox": args.dcat_publisher_mbox,
            },
        )
        with open(args.dcat_output, "w", encoding="utf-8") as file:
            json.dump(dcat_catalog, file, indent=2, ensure_ascii=False)
            file.write("\n")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as file:
            file.write(output + "\n")
        message = f"Wrote {len(records)} records to {args.output}"
        if args.incremental and incremental_stats is not None:
            message += (
                f" (created={incremental_stats['created']},"
                f" updated={incremental_stats['updated']},"
                f" unchanged={incremental_stats['unchanged']})"
            )
        if args.stac_output:
            message += f"; STAC to {args.stac_output}"
        if args.dcat_output:
            message += f"; DCAT to {args.dcat_output}"
        return 0, message

    return 0, output


if __name__ == "__main__":
    code, message = main()
    if message:
        print(message)
    raise SystemExit(code)
