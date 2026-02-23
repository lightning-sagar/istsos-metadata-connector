# SensorThings Metadata Harvester + STAC/DCAT API

This project collects metadata from an istSOS SensorThings API, converts it to:

- enriched metadata JSON
- STAC Item-style FeatureCollection JSON
- DCAT catalog JSON-LD

and exposes the results via a lightweight FastAPI service.

## What problem I solved

Prototype metadata connector for istSOS SensorThings → STAC/DCAT catalogs.

## How I approached it

I started by reading the istSOS tutorial documentation and getting familiar with the SensorThings API entities, authentication, and request flow:

- https://istsos.org/foss4g-asia/tutorial/sta_entity/

From there, I tested authentication and entity endpoints in the istSOS API docs UI, then implemented the harvester and API outputs in this project.

## What this project does

1. Authenticates to SensorThings (token or username/password).
2. Harvests entities using `$expand` (Things + Locations + Datastreams + Sensor + ObservedProperty).
3. Produces normalized dataset records.
4. Converts those records to STAC and DCAT views.
5. Supports incremental harvest (only changed metadata is updated logically).
6. Serves outputs through REST endpoints.

## Project structure

```text
foss4g/
├── app/
│   ├── __init__.py
│   ├── harvester.py            # harvesting + transforms + CLI main
│   └── api.py                  # FastAPI application
├── collect_sensorthings_metadata.py  # compatibility CLI wrapper
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── test.ipynb                  # notebook workflow and quick execution
└── fakedata2sta.py
```

## Metadata fields generated

Each record includes (when available):

- `thing_id`
- `thing_name`
- `datastream_name`
- `description`
- `location` as `{ "lat": <float>, "lon": <float> }`
- `sensor_type`
- `observed_property`
- `unit_of_measurement`
- `observation_type`
- `sampling_frequency`
- `time_range`
- `start_time`
- `end_time`
- `last_observation_time`
- `datastream_id`

Records without `datastream_id` are skipped.

## REST API

Base URL (when using Docker Compose below): `http://localhost:8020`

- `GET /datasets` → normalized metadata records
- `GET /stac/items` → STAC FeatureCollection
- `GET /dcat/catalog` → DCAT JSON-LD catalog

## Quick start (Docker)

### 1) Start all services

```bash
docker compose up -d --build
```

This starts:

- istSOS database/API stack
- metadata FastAPI service (`metadata-api`) on port `8020`

### 2) Test endpoints

```bash
curl http://localhost:8020/datasets
curl http://localhost:8020/stac/items
curl http://localhost:8020/dcat/catalog
```

## Local CLI usage (without API)

Use the compatibility script (recommended for quick manual runs):

```bash
python3 collect_sensorthings_metadata.py \
  --endpoint http://localhost:8018/istsos4/v1.1 \
  --username admin \
  --password admin \
  --incremental \
  --output metadata.json \
  --stac-output stac_items.json \
  --dcat-output dcat_catalog.json \
  --stac-collection-id istsos-datastreams \
  --stac-root-href http://localhost:8020/stac
```

Alternative auth mode:

```bash
python3 collect_sensorthings_metadata.py --ask-login --output metadata.json
```

## Incremental harvest behavior

When `--incremental` is enabled:

- current records are compared with prior signatures from `metadata_state.json`
- unchanged records are preserved
- changed/new records are updated
- removed datastreams disappear from latest output

State file can be customized via:

```bash
--state-file metadata_state.json
```

## Environment variables (API service)

Configured in `docker-compose.yml` under `metadata-api`:

- `METADATA_ENDPOINT`
- `METADATA_TOKEN` or (`METADATA_USERNAME`, `METADATA_PASSWORD`)
- `METADATA_INCREMENTAL` (`1` or `0`)
- `METADATA_OUTPUT`
- `STAC_OUTPUT`
- `DCAT_OUTPUT`
- `METADATA_STATE_FILE`
- `STAC_COLLECTION_ID`
- `STAC_ROOT_HREF`
- `HARVEST_INTERVAL_SECONDS`

## Notebook workflow

`test.ipynb` includes a run cell that:

1. asks for username/password
2. runs the collector script
3. saves metadata/STAC/DCAT outputs
4. prints counts and preview records

If needed, run cells in sequence so credentials/token context is available.

## Troubleshooting

### `401 Unauthorized`

- verify endpoint is correct (`http://localhost:8018/istsos4/v1.1`)
- verify credentials (`admin/admin` unless changed)
- verify istSOS API container is healthy

### Empty output

- ensure datastreams exist in istSOS
- ensure auth user can read Things/Datastreams
- check API logs:

```bash
docker compose logs -f api
docker compose logs -f metadata-api
```

### Rebuild after code changes

```bash
docker compose up -d --build
```

## Notes

- STAC output includes `collection` and minimal `self`/`root` links.
- DCAT output is generated as practical JSON-LD for catalog-style interoperability.

## Screenshots

### OAuth authorization in istSOS docs

![OAuth authorization in istSOS docs](image.png)

### Things endpoint test in Swagger UI

![Things endpoint test in Swagger UI](WhatsApp%20Image%202026-02-23%20at%2011.27.30%20AM.jpeg)

### Tutorial reference used during implementation

![Tutorial reference used during implementation](WhatsApp%20Image%202026-02-23%20at%2011.27.30%20AM%20(1).jpeg)

## Terminal output (sample)

### API response sample (`GET /datasets`)

```json
{
  "count": 1,
  "records": [
    {
      "thing_id": 2,
      "thing_name": "lightning-sagar-FIU_VALL",
      "datastream_name": "lightning-sagar-RSSI_FIU_VALL",
      "description": "Water level, water temperature and water electrical conductivity recorder Ticino river",
      "location": {
        "lat": 46.172245,
        "lon": 8.956099
      },
      "sensor_type": "lightning-sagar-Ecolog 10000",
      "observed_property": "ground:water:signal_strength",
      "unit_of_measurement": "RSSI",
      "observation_type": "",
      "sampling_frequency": "",
      "time_range": "2024-01-21T00:00:00Z/2024-01-21T06:00:00Z",
      "start_time": "2024-01-21T00:00:00Z",
      "end_time": "2024-01-21T06:00:00Z",
      "last_observation_time": "2024-01-21T06:00:00Z",
      "datastream_id": 1
    }
  ],
  "incremental": {
    "created": 1,
    "updated": 0,
    "unchanged": 0,
    "total": 1
  }
}
```

### CLI / notebook run sample

```text
Wrote 1 records to metadata.json; STAC to stac_items.json; DCAT to dcat_catalog.json
Total metadata records: 1
STAC features: 1
DCAT datasets: 1
[{'thing_id': 2, 'thing_name': 'lightning-sagar-FIU_VALL', 'datastream_name': 'lightning-sagar-RSSI_FIU_VALL', 'description': 'Water level, water temperature and water electrical conductivity recorder Ticino river', 'location': {'lat': 46.172245, 'lon': 8.956099}, 'sensor_type': 'lightning-sagar-Ecolog 10000', 'observed_property': 'ground:water:signal_strength', 'unit_of_measurement': 'RSSI', 'observation_type': '', 'sampling_frequency': '', 'time_range': '2024-01-21T00:00:00Z/2024-01-21T06:00:00Z', 'start_time': '2024-01-21T00:00:00Z', 'end_time': '2024-01-21T06:00:00Z', 'last_observation_time': '2024-01-21T06:00:00Z', 'datastream_id': 1}]
```
