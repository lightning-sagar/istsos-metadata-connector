import argparse
import getpass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
        help="Optional output path for STAC ItemCollection JSON.",
    )
    parser.add_argument(
        "--dcat-output",
        default=None,
        help="Optional output path for DCAT JSON-LD catalog.",
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


def build_stac_item_collection(
    records: List[Dict[str, Any]],
    collection_id: str,
    root_href: str,
) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []
    normalized_root = root_href.rstrip("/")

    for record in records:
        if record.get("datastream_id") is None:
            continue
        item_id = f"datastream-{record.get('datastream_id')}"
        item_self_href = f"{normalized_root}/items/{item_id}"
        location = record.get("location")
        geometry = None
        bbox = None
        if isinstance(location, dict) and "lat" in location and "lon" in location:
            lat = location["lat"]
            lon = location["lon"]
            geometry = {"type": "Point", "coordinates": [lon, lat]}
            bbox = [lon, lat, lon, lat]

        properties = {
            "thing_id": record.get("thing_id"),
            "thing_name": record.get("thing_name", ""),
            "description": record.get("description", ""),
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

        features.append(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": item_id,
                "collection": collection_id,
                "geometry": geometry,
                "bbox": bbox,
                "properties": properties,
                "links": [
                    {"rel": "self", "href": item_self_href},
                    {"rel": "root", "href": normalized_root},
                ],
                "assets": {},
            }
        )

    return {
        "type": "FeatureCollection",
        "links": [
            {"rel": "self", "href": f"{normalized_root}/items"},
            {"rel": "root", "href": normalized_root},
        ],
        "features": features,
    }


def build_dcat_catalog(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    datasets: List[Dict[str, Any]] = []
    for record in records:
        if record.get("datastream_id") is None:
            continue
        dataset_id = f"datastream-{record.get('datastream_id')}"
        dataset: Dict[str, Any] = {
            "@id": dataset_id,
            "@type": "dcat:Dataset",
            "dct:identifier": dataset_id,
            "dct:title": record.get("datastream_name") or record.get("thing_name", ""),
            "dct:description": record.get("description", ""),
            "dcat:keyword": [
                record.get("observed_property", ""),
                record.get("sensor_type", ""),
            ],
            "dct:temporal": {
                "schema:startDate": record.get("start_time", ""),
                "schema:endDate": record.get("end_time", ""),
            },
        }

        location = record.get("location")
        if isinstance(location, dict) and "lat" in location and "lon" in location:
            dataset["dct:spatial"] = {
                "@type": "dct:Location",
                "locn:geometry": {
                    "type": "Point",
                    "coordinates": [location["lon"], location["lat"]],
                },
            }

        datasets.append(dataset)

    return {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "locn": "http://www.w3.org/ns/locn#",
            "schema": "https://schema.org/",
        },
        "@type": "dcat:Catalog",
        "dcat:dataset": datasets,
    }


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
        stac_collection = build_stac_item_collection(
            records,
            collection_id=args.stac_collection_id,
            root_href=stac_root_href,
        )
        with open(args.stac_output, "w", encoding="utf-8") as file:
            json.dump(stac_collection, file, indent=2, ensure_ascii=False)
            file.write("\n")

    if args.dcat_output:
        dcat_catalog = build_dcat_catalog(records)
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
