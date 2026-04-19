"""
Unified API Handler Lambda

Single Lambda behind API Gateway — routes ALL endpoints:

  Live (DynamoDB):
    A1. GET /api/live/vessels          → all active vessels (bbox, vessel_type filter)
    A2. GET /api/live/vessels/{mmsi}   → single vessel detail with track
    A3. GET /api/live/heatmap          → pre-aggregated density grid

  Historical (S3 Data Lake):
    B1. GET /api/historical/cargo/total
    B2. GET /api/historical/cargo/breakdown
    B3. GET /api/historical/container
    B4. GET /api/historical/vessel-calls

Design notes:
  - bbox filtering is done in Python memory after full tenant query (DynamoDB
    has no native 2D geo index; with <500 vessels this is well within 1MB limit)
  - Internal fields (route_id, route_progress) are stripped before returning
  - Decimal → float conversion for DynamoDB numeric types
  - Global cache for S3 CSV reads (warm Lambda reuse)
"""

import json
import csv
import io
import os
import re
import logging
from datetime import datetime, timezone
from decimal import Decimal

try:
    import boto3
    from boto3.dynamodb.conditions import Key
    _HAS_BOTO3 = True
except ImportError:
    _HAS_BOTO3 = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables 
S3_BUCKET = os.environ.get('S3_BUCKET', 'portsync-datalake-team18')
DYNAMODB_TABLE_LIVE = os.environ.get('DYNAMODB_TABLE_LIVE', 'PortSync-LiveState')
DYNAMODB_TABLE_HEATMAP = os.environ.get('DYNAMODB_TABLE_HEATMAP', 'PortSync-HeatmapAggregates')
LOCAL_DATA_DIR = os.environ.get('LOCAL_DATA_DIR', '')  # local CSV testing

# S3 CSV mapping 
CSV_PATHS = {
    'cargo_total':     'historical/cargo_total.csv',
    'cargo_breakdown': 'historical/cargo_breakdown.csv',
    'container':       'historical/container.csv',
    'vessel_calls':    'historical/vessel_calls.csv',
}

# Fields that are internal to the simulator — never exposed to frontend
_INTERNAL_FIELDS = {'route_id', 'route_progress', 'tenant_id'}


# Helpers: HTTP Response

def api_response(status_code: int, body: dict | list) -> dict:
    """API Gateway proxy-integration response with CORS."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        },
        'body': json.dumps(body, ensure_ascii=False, default=_json_serial),
    }


def error_response(status_code: int, error_code: str, message: str) -> dict:
    """Unified error format per API Contract Section C."""
    return api_response(status_code, {'error': error_code, 'message': message})


def _json_serial(obj):
    """Handle Decimal and datetime serialisation for json.dumps."""
    if isinstance(obj, Decimal):
        # Return int if no fractional part, else float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat() + 'Z'
    raise TypeError(f"Type {type(obj)} not serializable")


# Helpers: Auth / Tenant Extraction

def extract_tenant_id(event: dict) -> str:
    """Extract tenant_id from Cognito JWT claims."""
    try:
        claims = event['requestContext']['authorizer']['claims']
        return claims.get('custom:tenant_id', 'default')
    except (KeyError, TypeError):
        return 'default'



# DynamoDB: Decimal → Python conversion

def _decimal_to_native(item: dict) -> dict:
    """Recursively convert all Decimal values in a DynamoDB item."""
    result = {}
    for k, v in item.items():
        if isinstance(v, Decimal):
            result[k] = int(v) if v % 1 == 0 else float(v)
        elif isinstance(v, dict):
            result[k] = _decimal_to_native(v)
        elif isinstance(v, list):
            result[k] = [
                _decimal_to_native(i) if isinstance(i, dict)
                else (float(i) if isinstance(i, Decimal) else i)
                for i in v
            ]
        else:
            result[k] = v
    return result


def strip_internal_fields(vessel: dict) -> dict:
    """Remove simulator-internal fields before returning to frontend."""
    return {k: v for k, v in vessel.items() if k not in _INTERNAL_FIELDS}


# A. Live Endpoints (DynamoDB)

def _get_dynamodb_table(table_name: str):
    """Get a DynamoDB Table resource."""
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    return dynamodb.Table(table_name)


def _query_all_vessels(tenant_id: str) -> list[dict]:
    """
    Full tenant query on LiveState table.
    Returns all vessels for the tenant (typically <500).
    bbox/vessel_type filtering is done afterwards in Python.
    """
    table = _get_dynamodb_table(DYNAMODB_TABLE_LIVE)
    response = table.query(
        KeyConditionExpression=Key('tenant_id').eq(tenant_id),
    )
    items = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key('tenant_id').eq(tenant_id),
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        items.extend(response.get('Items', []))

    return [_decimal_to_native(item) for item in items]


def _parse_bbox(bbox_str: str | None) -> dict | None:
    """Parse bbox param: 'min_lat,min_lng,max_lat,max_lng'."""
    if not bbox_str:
        return None
    try:
        parts = [float(x.strip()) for x in bbox_str.split(',')]
        if len(parts) != 4:
            return None
        return {
            'min_lat': parts[0], 'min_lng': parts[1],
            'max_lat': parts[2], 'max_lng': parts[3],
        }
    except ValueError:
        return None


def handle_live_vessels(event: dict, tenant_id: str, params: dict) -> dict:
    """
    A1. GET /api/live/vessels
    Returns all active vessels with optional bbox and vessel_type filtering.
    All filtering is done in Python memory (DynamoDB has no geo index).
    """
    # Parse query params
    bbox = _parse_bbox(params.get('bbox'))
    vessel_type = params.get('vessel_type')
    limit = min(int(params.get('limit', 200)), 500)

    # Full tenant query
    vessels = _query_all_vessels(tenant_id)

    # In-memory filtering 
    if bbox:
        vessels = [
            v for v in vessels
            if (bbox['min_lat'] <= v.get('lat', 0) <= bbox['max_lat'] and
                bbox['min_lng'] <= v.get('lng', 0) <= bbox['max_lng'])
        ]

    if vessel_type:
        vessels = [v for v in vessels if v.get('vessel_type') == vessel_type]

    # Apply limit
    vessels = vessels[:limit]

    # Strip internal fields
    clean_vessels = [strip_internal_fields(v) for v in vessels]

    # Remove track from list view (only shown in detail view A2)
    for v in clean_vessels:
        v.pop('track', None)

    return api_response(200, {
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'count': len(clean_vessels),
        'vessels': clean_vessels,
    })


def handle_live_vessel_detail(event: dict, tenant_id: str, mmsi: str) -> dict:
    """
    A2. GET /api/live/vessels/{mmsi}
    Returns single vessel with track history.
    """
    table = _get_dynamodb_table(DYNAMODB_TABLE_LIVE)

    try:
        response = table.get_item(
            Key={'tenant_id': tenant_id, 'mmsi': mmsi},
        )
    except Exception as e:
        logger.error(f"DynamoDB get_item error: {e}")
        return error_response(500, 'internal_error', str(e))

    item = response.get('Item')
    if not item:
        return error_response(404, 'vessel_not_found',
                              f'No active vessel with MMSI {mmsi}')

    vessel = _decimal_to_native(item)
    vessel = strip_internal_fields(vessel)

    # Ensure track exists (API contract guarantees it)
    if 'track' not in vessel:
        vessel['track'] = []

    return api_response(200, vessel)


def handle_live_heatmap(event: dict, tenant_id: str) -> dict:
    """
    A3. GET /api/live/heatmap
    Returns pre-aggregated vessel density grid from HeatmapAggregates table.
    """
    table = _get_dynamodb_table(DYNAMODB_TABLE_HEATMAP)

    # Scan all cells (small table, <1000 items)
    response = table.scan()
    items = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    cells = []
    for item in items:
        item = _decimal_to_native(item)
        cells.append({
            'lat': item.get('lat', 0),
            'lng': item.get('lng', 0),
            'vessel_count': item.get('vessel_count', 0),
            'avg_sog': item.get('avg_sog', 0),
        })

    return api_response(200, {
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'grid_resolution_deg': 0.01,
        'bbox': {
            'min_lat': 1.10, 'min_lng': 103.60,
            'max_lat': 1.38, 'max_lng': 104.10,
        },
        'cells': cells,
    })

# B. Historical Endpoints (S3 Data Lake)

_csv_cache: dict[str, list[dict]] = {}


def read_csv(dataset_key: str) -> list[dict]:
    """Read CSV from S3 with global caching for warm container reuse."""
    if dataset_key in _csv_cache:
        return _csv_cache[dataset_key]

    s3_key = CSV_PATHS[dataset_key]

    if LOCAL_DATA_DIR:
        filepath = os.path.join(LOCAL_DATA_DIR, os.path.basename(s3_key))
        with open(filepath, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
    else:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        text = obj['Body'].read().decode('utf-8')
        rows = list(csv.DictReader(io.StringIO(text)))

    _csv_cache[dataset_key] = rows
    logger.info(f"Cached {dataset_key}: {len(rows)} rows")
    return rows


def filter_by_date_range(rows: list[dict], from_m: str | None, to_m: str | None,
                         field: str = 'month') -> list[dict]:
    """Filter by YYYY-MM range (inclusive)."""
    f = from_m or '2020-01'
    t = to_m or '9999-12'
    return [r for r in rows if f <= r[field] <= t]


def handle_cargo_total(params: dict) -> dict:
    """B1. /api/historical/cargo/total"""
    rows = read_csv('cargo_total')
    filtered = filter_by_date_range(rows, params.get('from'), params.get('to'))
    return api_response(200, {
        'unit': 'thousand_tonnes',
        'source': 'MPA via data.gov.sg',
        'data': [
            {'month': r['month'], 'cargo_throughput': float(r['cargo_throughput'])}
            for r in filtered
        ],
    })


def handle_cargo_breakdown(params: dict) -> dict:
    """B2. /api/historical/cargo/breakdown"""
    rows = read_csv('cargo_breakdown')
    filtered = filter_by_date_range(rows, params.get('from'), params.get('to'))

    months: dict[str, list] = {}
    for r in filtered:
        m = r['month']
        if m not in months:
            months[m] = []
        months[m].append({
            'primary': r['cargo_type_primary'],
            'secondary': r['cargo_type_secondary'],
            'throughput': float(r['cargo_throughput']),
        })

    return api_response(200, {
        'unit': 'thousand_tonnes',
        'source': 'MPA via data.gov.sg',
        'categories': {
            'primary': ['General Cargo', 'Bulk Cargo'],
            'secondary': ['Containerised', 'Conventional', 'Oil', 'Non-Oil Bulk'],
        },
        'data': [
            {'month': m, 'breakdown': bd}
            for m, bd in sorted(months.items())
        ],
    })


def handle_container(params: dict) -> dict:
    """B3. /api/historical/container"""
    rows = read_csv('container')
    filtered = filter_by_date_range(rows, params.get('from'), params.get('to'))
    return api_response(200, {
        'unit': 'thousand_TEUs',
        'source': 'MPA via data.gov.sg',
        'data': [
            {'month': r['month'], 'container_throughput': float(r['container_throughput'])}
            for r in filtered
        ],
    })


def handle_vessel_calls(params: dict) -> dict:
    """B4. /api/historical/vessel-calls"""
    rows = read_csv('vessel_calls')
    filtered = filter_by_date_range(rows, params.get('from'), params.get('to'))

    purpose_filter = params.get('purpose')
    if purpose_filter and purpose_filter.lower() != 'all':
        filtered = [r for r in filtered if r['purpose_type'].lower() == purpose_filter.lower()]

    months: dict[str, list] = {}
    for r in filtered:
        m = r['month']
        if m not in months:
            months[m] = []
        months[m].append({
            'purpose': r['purpose_type'],
            'vessel_calls': int(r['number_of_vessel_calls']),
            'gross_tonnage': float(r['gross_tonnage']),
        })

    return api_response(200, {
        'unit_calls': 'count',
        'unit_tonnage': 'thousand_GT',
        'source': 'MPA via data.gov.sg',
        'data': [
            {'month': m, 'by_purpose': bp}
            for m, bp in sorted(months.items())
        ],
    })


#  Main Router

# Regex to extract MMSI from path: /api/live/vessels/354499000
_VESSEL_DETAIL_RE = re.compile(r'^/api/live/vessels/(\d+)$')


def handler(event: dict, context) -> dict:
    """
    Unified Lambda handler — single entry point for all API routes.

    Routing logic:
      /api/live/vessels          → A1 (list)
      /api/live/vessels/{mmsi}   → A2 (detail)
      /api/live/heatmap          → A3
      /api/historical/cargo/total     → B1
      /api/historical/cargo/breakdown → B2
      /api/historical/container       → B3
      /api/historical/vessel-calls    → B4
    """
    logger.info(f"RAW EVENT: {json.dumps(event, default=str)[:2000]}")
    path = event.get('path', '')
    http_method = event.get('httpMethod', 'GET')
    params = event.get('queryStringParameters') or {}

    # CORS preflight
    if http_method == 'OPTIONS':
        return api_response(200, {'message': 'OK'})

    tenant_id = extract_tenant_id(event)
    logger.info(f"[{http_method}] {path} | tenant={tenant_id} | params={params}")

    # Validate date params for historical endpoints 
    if '/historical/' in path:
        for label in ('from', 'to'):
            val = params.get(label)
            if val:
                try:
                    datetime.strptime(val, '%Y-%m')
                except ValueError:
                    return error_response(
                        400, 'invalid_params',
                        f"Parameter '{label}' must be YYYY-MM format, got '{val}'"
                    )

    try:
        # Live endpoints 
        if path == '/api/live/vessels':
            return handle_live_vessels(event, tenant_id, params)

        match = _VESSEL_DETAIL_RE.match(path)
        if match:
            mmsi = match.group(1)
            return handle_live_vessel_detail(event, tenant_id, mmsi)

        if path == '/api/live/heatmap':
            return handle_live_heatmap(event, tenant_id)

        # Historical endpoints 
        if path.endswith('/cargo/total'):
            return handle_cargo_total(params)

        if path.endswith('/cargo/breakdown'):
            return handle_cargo_breakdown(params)

        if path.endswith('/container'):
            return handle_container(params)

        if path.endswith('/vessel-calls'):
            return handle_vessel_calls(params)

        # 404 fallback 
        return error_response(404, 'not_found', f'Unknown endpoint: {path}')

    except Exception as e:
        logger.error(f"Internal error on {path}: {e}", exc_info=True)
        return error_response(500, 'internal_error', str(e))
