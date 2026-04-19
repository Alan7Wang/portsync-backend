"""
Data Simulator Lambda

Triggered by EventBridge every 30 seconds.
Implements the "Parameterized Route Advancement Model":
  - Vessels follow pre-defined Singapore route templates
  - Speed sampled from NOAA AIS statistical distributions
  - Heading = route target direction + Gaussian noise
  - Boundary detection: reset to route entry if out-of-bounds or on land

DynamoDB Schema:
  Table: PortSync-LiveState
  PK: tenant_id (String)
  SK: mmsi (String)
"""

import json
import math
import random
import os
import logging
from datetime import datetime, timezone
from decimal import Decimal
from copy import deepcopy

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
S3_CONFIG_KEY = os.environ.get('S3_CONFIG_KEY', 'config/simulator_params.json')
NUM_VESSELS = int(os.environ.get('NUM_VESSELS', '50'))
TENANT_ID = os.environ.get('DEFAULT_TENANT_ID', 'default')
DT = 30  # Time step in seconds (EventBridge interval)

#  Singapore Maritime Geography (Hardcoded)

BOUNDING_BOX = {
    'min_lat': 1.10,
    'max_lat': 1.38,
    'min_lng': 103.60,
    'max_lng': 104.10,
}

# Route Templates 
# Each route is either a shipping lane (waypoints) or an anchorage (center+radius).
ROUTES = [
    # === Main Traffic Separation Scheme (TSS) ===
    {
        'id': 'eastbound',
        'name': 'Eastbound Main Channel (Malacca → SCS)',
        'weight': 0.25,
        'base_heading': 85,
        'waypoints': [
            (1.210, 103.60), (1.195, 103.75), (1.195, 103.85),
            (1.195, 103.95), (1.190, 104.10),
        ],
        'lane_width_deg': 0.01,
        'is_anchorage': False,
    },
    {
        'id': 'westbound',
        'name': 'Westbound Main Channel (SCS → Malacca)',
        'weight': 0.20,
        'base_heading': 265,
        'waypoints': [
            (1.175, 104.10), (1.180, 103.95), (1.175, 103.85),
            (1.170, 103.75), (1.190, 103.60),
        ],
        'lane_width_deg': 0.01,
        'is_anchorage': False,
    },
    # === Port Approach Channels ===
    {
        'id': 'tuas_approach',
        'name': 'Tuas Port Approach',
        'weight': 0.08,
        'base_heading': 350,
        'waypoints': [
            (1.190, 103.65), (1.220, 103.66), (1.250, 103.67),
        ],
        'lane_width_deg': 0.005,
        'is_anchorage': False,
    },
    {
        'id': 'psa_approach',
        'name': 'PSA Terminal Approach',
        'weight': 0.10,
        'base_heading': 355,
        'waypoints': [
            (1.195, 103.82), (1.220, 103.83), (1.250, 103.83),
        ],
        'lane_width_deg': 0.005,
        'is_anchorage': False,
    },
    {
        'id': 'changi_approach',
        'name': 'Changi Approach',
        'weight': 0.02,
        'base_heading': 10,
        'waypoints': [
            (1.195, 104.03), (1.230, 104.04), (1.280, 104.04),
        ],
        'lane_width_deg': 0.005,
        'is_anchorage': False,
    },
    {
        'id': 'keppel_approach',
        'name': 'Keppel Terminal Approach',
        'weight': 0.05,
        'base_heading': 0,
        'waypoints': [
            (1.195, 103.85), (1.220, 103.85), (1.250, 103.84),
        ],
        'lane_width_deg': 0.005,
        'is_anchorage': False,
    },
    # === Anchorages (waiting areas) ===
    {
        'id': 'western_anchorage',
        'name': 'Western Anchorage',
        'weight': 0.10,
        'base_heading': None,
        'center': (1.200, 103.68),
        'radius_deg': 0.02,
        'is_anchorage': True,
    },
    {
        'id': 'eastern_anchorage',
        'name': 'Eastern Anchorage (off Changi)',
        'weight': 0.08,
        'base_heading': None,
        'center': (1.220, 104.05),
        'radius_deg': 0.015,
        'is_anchorage': True,
    },
    {
        'id': 'southern_anchorage',
        'name': 'Southern Anchorage (mid-strait)',
        'weight': 0.12,
        'base_heading': None,
        'center': (1.150, 103.85),
        'radius_deg': 0.025,
        'is_anchorage': True,
    },
]

# Land Exclusion Zones (simplified rectangles) 
LAND_ZONES = [
    {'min_lat': 1.270, 'max_lat': 1.470, 'min_lng': 103.60, 'max_lng': 104.10},  # Singapore mainland
    {'min_lat': 1.243, 'max_lat': 1.260, 'min_lng': 103.820, 'max_lng': 103.860},  # Sentosa
    {'min_lat': 1.255, 'max_lat': 1.290, 'min_lng': 103.660, 'max_lng': 103.710},  # Jurong Island
    {'min_lat': 1.050, 'max_lat': 1.120, 'min_lng': 103.80, 'max_lng': 104.15},    # Batam (north coast)
]

# Fleet Composition (based on MPA 2026-01 vessel calls) 
FLEET_COMPOSITION = [
    {'type': 'cargo',     'code': 70, 'weight': 0.24, 'speed_cap': 18},
    {'type': 'bunker',    'code': 90, 'weight': 0.21, 'speed_cap': 8},
    {'type': 'tanker',    'code': 80, 'weight': 0.20, 'speed_cap': 14},
    {'type': 'other',     'code': 99, 'weight': 0.15, 'speed_cap': 12},
    {'type': 'passenger', 'code': 60, 'weight': 0.08, 'speed_cap': 22},
    {'type': 'tug',       'code': 52, 'weight': 0.05, 'speed_cap': 12},
    {'type': 'bunker',    'code': 90, 'weight': 0.07, 'speed_cap': 8},   # remainder → extra bunkers
]


# Utility Functions

def weighted_choice(items: list[dict], key: str = 'weight') -> dict:
    """Weighted random selection from a list of dicts with a 'weight' field."""
    weights = [item[key] for item in items]
    return random.choices(items, weights=weights, k=1)[0]


def in_bbox(lat: float, lng: float) -> bool:
    """Check if coordinates are within the simulation bounding box."""
    return (BOUNDING_BOX['min_lat'] <= lat <= BOUNDING_BOX['max_lat'] and
            BOUNDING_BOX['min_lng'] <= lng <= BOUNDING_BOX['max_lng'])


def in_land_zone(lat: float, lng: float) -> bool:
    """Check if coordinates fall inside any land exclusion zone."""
    for z in LAND_ZONES:
        if (z['min_lat'] <= lat <= z['max_lat'] and
                z['min_lng'] <= lng <= z['max_lng']):
            return True
    return False


def classify_status(sog: float) -> str:
    """Classify vessel navigational status from Speed Over Ground."""
    if sog < 0.5:
        return 'anchored'
    elif sog < 5.0:
        return 'maneuvering'
    elif sog < 15.0:
        return 'underway'
    return 'fast'


def current_utc_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def haversine_bearing(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate bearing (degrees) from point 1 to point 2."""
    dlng = math.radians(lng2 - lng1)
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    x = math.sin(dlng) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - \
        math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlng)
    bearing = math.degrees(math.atan2(x, y))
    return bearing % 360


def distance_deg(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Simple Euclidean distance in degrees (sufficient for short distances)."""
    return math.sqrt((lat2 - lat1) ** 2 + (lng2 - lng1) ** 2)


# Route Logic

def get_route_by_id(route_id: str) -> dict | None:
    """Look up a route by its ID."""
    for r in ROUTES:
        if r['id'] == route_id:
            return r
    return None


def get_target_heading(route: dict, lat: float, lng: float, route_progress: float) -> float:
    """
    Determine the target heading for a vessel on a given route.
    For shipping lanes: bearing toward the next waypoint.
    For anchorages: random heading (drifting in place).
    """
    if route['is_anchorage']:
        # Anchored vessels drift randomly
        return random.uniform(0, 360)

    waypoints = route['waypoints']
    # Determine which segment the vessel is on based on route_progress
    n_segments = len(waypoints) - 1
    if n_segments <= 0:
        return route.get('base_heading', 90)

    seg_idx = min(int(route_progress * n_segments), n_segments - 1)
    target_wp = waypoints[seg_idx + 1]

    return haversine_bearing(lat, lng, target_wp[0], target_wp[1])


def get_entry_point(route: dict) -> tuple[float, float]:
    """
    Get the entry point for a route (first waypoint or random point in anchorage).
    Adds small random offset to prevent vessels stacking at the exact same spot.
    """
    if route['is_anchorage']:
        center = route['center']
        r = route['radius_deg']
        lat = center[0] + random.uniform(-r, r)
        lng = center[1] + random.uniform(-r, r)
        # Clamp to bbox with margin
        margin = 0.005
        lat = max(BOUNDING_BOX['min_lat'] + margin, min(lat, BOUNDING_BOX['max_lat'] - margin))
        lng = max(BOUNDING_BOX['min_lng'] + margin, min(lng, BOUNDING_BOX['max_lng'] - margin))
        while in_land_zone(lat, lng):
            lat -= 0.005
        return (lat, lng)

    wp = route['waypoints'][0]
    lane_w = route.get('lane_width_deg', 0.005)
    lat = wp[0] + random.uniform(-lane_w, lane_w)
    lng = wp[1] + random.uniform(-lane_w * 0.5, lane_w * 0.5)

    # Clamp to bounding box with margin to avoid edge cases
    margin = 0.005
    lat = max(BOUNDING_BOX['min_lat'] + margin, min(lat, BOUNDING_BOX['max_lat'] - margin))
    lng = max(BOUNDING_BOX['min_lng'] + margin, min(lng, BOUNDING_BOX['max_lng'] - margin))

    # Ensure not on land; if so, nudge south
    while in_land_zone(lat, lng):
        lat -= 0.005

    return (lat, lng)


def advance_route_progress(route: dict, lat: float, lng: float, current_progress: float) -> float:
    """
    Update route progress (0→1) based on how close the vessel is to waypoints.
    """
    if route['is_anchorage']:
        return 0.0  # Anchored vessels don't progress

    waypoints = route['waypoints']
    n_segments = len(waypoints) - 1
    if n_segments <= 0:
        return 1.0

    # Find closest segment
    min_dist = float('inf')
    closest_seg = 0
    for i in range(n_segments):
        wp = waypoints[i + 1]
        d = distance_deg(lat, lng, wp[0], wp[1])
        if d < min_dist:
            min_dist = d
            closest_seg = i

    # Progress = segment fraction
    new_progress = (closest_seg + 0.5) / n_segments
    # Only allow forward progression (no going backwards)
    return max(current_progress, min(new_progress, 1.0))

#  Speed Profile Sampling (AIS-calibrated)


def get_speed_profile(vessel_type: str, status: str, params: dict) -> dict:
    """
    Get speed distribution parameters for a vessel type + status combination.
    Base values from NOAA AIS; adjusted for vessel type physical limits.
    """
    profiles = params['speed_profiles']

    # Map status → profile key
    if status == 'anchored':
        return profiles['anchored']
    elif status == 'maneuvering':
        return profiles['maneuvering']
    elif status == 'fast':
        return profiles['fast']
    else:
        return profiles['cruising']


def sample_speed(vessel_type: str, current_status: str, params: dict) -> float:
    """Sample a new speed from the appropriate distribution, with type-based caps."""
    profile = get_speed_profile(vessel_type, current_status, params)
    mean = profile['mean_knots']
    std = profile['std_knots']

    new_sog = max(0.0, random.gauss(mean, std))

    # Apply vessel type speed caps
    speed_caps = {
        'cargo': 18, 'tanker': 14, 'bunker': 8,
        'tug': 12, 'passenger': 22, 'other': 15,
    }
    cap = speed_caps.get(vessel_type, 15)
    return min(new_sog, cap)


#  Core Simulation: Fleet Initialization

def initialize_fleet(params: dict) -> list[dict]:
    """
    Create the initial fleet of N vessels.
    Each vessel gets:
      - Real metadata from the AIS vessel pool
      - A vessel type sampled from Singapore fleet composition
      - A route assignment
      - An initial position on that route
    """
    vessel_pool = params['vessel_pool']
    fleet = []
    used_mmsi = set()  
    now = current_utc_iso()

    for i in range(NUM_VESSELS):
        # 1. Select vessel type by MPA fleet composition weights
        vtype = weighted_choice(FLEET_COMPOSITION)

        # 2. Select route by weight
        #    Anchorage routes more likely for bunkers/tugs; main lanes for cargo/tanker
        if vtype['type'] in ('bunker', 'tug'):
            # Bias toward anchorages
            route_weights = []
            for r in ROUTES:
                w = r['weight'] * (3.0 if r['is_anchorage'] else 0.5)
                route_weights.append(w)
        else:
            route_weights = [r['weight'] for r in ROUTES]

        route = random.choices(ROUTES, weights=route_weights, k=1)[0]

        # 3. Pick vessel metadata from a COMPATIBLE type bucket.
        # This avoids semantic mismatch (e.g. a 52m pleasure craft
        # labelled as "cargo"). Mapping from SG types to AIS type codes:
        _TYPE_CODE_BUCKETS = {
            'cargo':     [70, 71, 72, 73, 74, 75, 76, 77, 78, 79],
            'tanker':    [80, 81, 82, 83, 84, 85, 86, 87, 88, 89],
            'passenger': [60, 61, 62, 63, 64, 65, 66, 67, 68, 69],
            'tug':       [50, 51, 52, 53],
            'bunker':    [90, 91, 92, 93, 99],
            'other':     [30, 31, 33, 35, 36, 37, 55, 57],
        }
        
        compatible_codes = set(_TYPE_CODE_BUCKETS.get(vtype['type'], []))
        compatible_pool = [
            v for v in vessel_pool
            if v.get('vessel_type_code', 0) in compatible_codes
            and v['mmsi'] not in used_mmsi
        ]
        if compatible_pool:
            meta = random.choice(compatible_pool)
        else:
            # Fallback: pick any unused vessel
            fallback_pool = [v for v in vessel_pool if v['mmsi'] not in used_mmsi]
            if fallback_pool:
                meta = random.choice(fallback_pool)
            else:
                logger.warning(f"Vessel pool exhausted at index {i}, stopping")
                break
        used_mmsi.add(meta['mmsi'])

        # 4. Generate initial position on the route
        lat, lng = get_entry_point(route)

        # 5. Initial speed based on route type
        if route['is_anchorage']:
            sog = max(0.0, random.gauss(0.03, 0.07))
            initial_status = 'anchored'
        else:
            sog = sample_speed(vtype['type'], 'underway', params)
            initial_status = classify_status(sog)

        # 6. Initial heading
        if route['is_anchorage']:
            cog = random.uniform(0, 360)
        else:
            cog = (route['base_heading'] + random.gauss(0, 3)) % 360

        vessel = {
            'tenant_id': TENANT_ID,
            'mmsi': meta['mmsi'],
            'vessel_name': meta['name'],
            'imo': meta.get('imo', ''),
            'callsign': meta.get('callsign', ''),
            'vessel_type': vtype['type'],
            'vessel_type_code': vtype['code'],
            'lat': round(lat, 4),
            'lng': round(lng, 4),
            'sog': round(sog, 1),
            'cog': round(cog, 1),
            'heading': int(cog) % 360,
            'status': initial_status,
            'length': meta.get('length', 100.0),
            'width': meta.get('width', 20.0),
            'draft': meta.get('draft', 8.0),
            'destination': 'SGSIN',
            'updated_at': now,
            # Internal fields (not returned to frontend via API)
            'route_id': route['id'],
            'route_progress': 0.0,
            # Track: list of recent positions (kept to last 10)
            'track': [],
        }
        fleet.append(vessel)

    logger.info(f"Initialized fleet of {len(fleet)} vessels")
    return fleet


#  Core Simulation: Per-Step Vessel Update

def update_vessel(vessel: dict, params: dict, dt: int = DT) -> dict:
    """
    Update a single vessel's position for one time step.

    Algorithm (Parameterized Route Advancement Model):
      1. Determine target heading from route waypoints
      2. Add Gaussian heading noise (σ from AIS data)
      3. Sample new speed from AIS-calibrated distribution
      4. Compute new lat/lng via trigonometric projection
      5. Boundary check: reset if out-of-bounds or on land
      6. Append current position to track (keep last 10)
      7. Update status classification
    """
    route = get_route_by_id(vessel['route_id'])
    if not route:
        # Fallback: assign to eastbound
        route = ROUTES[0]
        vessel['route_id'] = route['id']

    # Step 0: Append current position to track BEFORE updating 
    track = vessel.get('track', [])
    track.append({
        'lat': vessel['lat'],
        'lng': vessel['lng'],
        'sog': vessel['sog'],
        'ts': vessel['updated_at'],
    })
    # Keep only last 10 positions (~5 min of history at 30s intervals)
    vessel['track'] = track[-10:]

    # Step 1: Target heading 
    target_heading = get_target_heading(
        route, vessel['lat'], vessel['lng'], vessel['route_progress']
    )

    # Step 2: Add heading noise 
    # Original AIS data gives a combined std of ~12.7°.
    # We split it: cruising vessels hold course tightly (2.5°),
    # anchored vessels swing freely (8.0°).
    # Justification: AIS p5=-6°, p95=6° → 90% within ±6°,
    # so cruising component must be well below 6°.
    CRUISING_HEADING_STD = 2.5
    ANCHORED_HEADING_STD = 8.0

    if vessel['status'] == 'anchored':
        heading_noise = random.gauss(0, ANCHORED_HEADING_STD)
    else:
        heading_noise = random.gauss(0, CRUISING_HEADING_STD)

    new_heading = (target_heading + heading_noise) % 360

    # Step 3: Sample new speed 
    # Occasionally change status (e.g., anchored vessel starts moving)
    current_status = vessel['status']

    # Status-aware speed sampling 
    # Key design: on shipping lanes, "anchored" must NOT be an absorbing
    # state. We sample behavior state first, then sample speed accordingly.
    if route['is_anchorage']:
        # Anchorage: vessels mostly stay anchored
        if current_status == 'anchored' and random.random() < 0.02:
            current_status = 'maneuvering'
        elif current_status != 'anchored':
            current_status = 'anchored'
    else:
        # Non-anchorage route: prevent status collapse into anchored.
        # If vessel was "anchored" on a shipping lane (from a low-speed
        # sample last step), force it back to maneuvering.
        if current_status == 'anchored':
            current_status = 'maneuvering'

    new_sog = sample_speed(vessel['vessel_type'], current_status, params)

    # On non-anchorage routes, clamp speed floor to prevent
    # re-entering anchored status via low-speed sampling
    if not route['is_anchorage'] and new_sog < 0.5:
        new_sog = max(0.5, random.gauss(2.59, 1.45))  # resample as maneuvering

    # Step 4: Compute new coordinates 
    speed_mps = new_sog * 0.5144   # knots → meters/second
    distance_m = speed_mps * dt     # distance traveled this step

    heading_rad = math.radians(new_heading)
    dlat = distance_m * math.cos(heading_rad) / 111320.0
    dlng = distance_m * math.sin(heading_rad) / \
        (111320.0 * math.cos(math.radians(vessel['lat'])))

    new_lat = vessel['lat'] + dlat
    new_lng = vessel['lng'] + dlng

    # Step 5: Boundary & land detection 
    if not in_bbox(new_lat, new_lng) or in_land_zone(new_lat, new_lng):
        # Route completed or hit boundary → reset to entry point
        new_lat, new_lng = get_entry_point(route)
        vessel['route_progress'] = 0.0
        logger.debug(f"Vessel {vessel['mmsi']} reset to entry point of {route['id']}")
    else:
        # Update route progress
        vessel['route_progress'] = advance_route_progress(
            route, new_lat, new_lng, vessel['route_progress']
        )

        # Check if vessel has completed its route (progress ≈ 1.0)
        if vessel['route_progress'] >= 0.95 and not route['is_anchorage']:
            # Assign a new random route for the next journey
            new_route = weighted_choice(ROUTES)
            vessel['route_id'] = new_route['id']
            vessel['route_progress'] = 0.0
            new_lat, new_lng = get_entry_point(new_route)

    # Step 6: Anchorage drift containment 
    if route['is_anchorage'] and 'center' in route:
        center = route['center']
        radius = route['radius_deg']
        dist_from_center = distance_deg(new_lat, new_lng, center[0], center[1])
        if dist_from_center > radius:
            # Pull back toward center
            bearing_to_center = haversine_bearing(new_lat, new_lng, center[0], center[1])
            pull_rad = math.radians(bearing_to_center)
            pull_dist = (dist_from_center - radius * 0.7) * 0.5
            new_lat += pull_dist * math.cos(pull_rad)
            new_lng += pull_dist * math.sin(pull_rad)

    # Step 7: Update vessel state 
    vessel['lat'] = round(new_lat, 4)
    vessel['lng'] = round(new_lng, 4)
    vessel['sog'] = round(new_sog, 1)
    vessel['cog'] = round(new_heading, 1)
    vessel['heading'] = int(new_heading) % 360
    vessel['status'] = classify_status(new_sog)
    vessel['updated_at'] = current_utc_iso()

    return vessel


#  DynamoDB I/O

def _float_to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_float_to_decimal(i) for i in obj]
    return obj


def read_fleet_from_dynamodb(table) -> list[dict]:
    """Read all vessels for the tenant from DynamoDB."""
    response = table.query(
        KeyConditionExpression=Key('tenant_id').eq(TENANT_ID),
    )
    items = response.get('Items', [])

    # Handle pagination (unlikely with <500 vessels, but be safe)
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key('tenant_id').eq(TENANT_ID),
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        items.extend(response.get('Items', []))

    # Convert Decimal back to float for calculations
    for item in items:
        for key in ('lat', 'lng', 'sog', 'cog', 'length', 'width', 'draft', 'route_progress'):
            if key in item and isinstance(item[key], Decimal):
                item[key] = float(item[key])
        if 'vessel_type_code' in item and isinstance(item['vessel_type_code'], Decimal):
            item['vessel_type_code'] = int(item['vessel_type_code'])
        if 'heading' in item and isinstance(item['heading'], Decimal):
            item['heading'] = int(item['heading'])
        # Convert track entries
        if 'track' in item:
            for t in item['track']:
                for tk in ('lat', 'lng', 'sog'):
                    if tk in t and isinstance(t[tk], Decimal):
                        t[tk] = float(t[tk])

    return items


def write_fleet_to_dynamodb(table, fleet: list[dict]):
    """Batch write all vessels back to DynamoDB."""
    with table.batch_writer() as batch:
        for vessel in fleet:
            item = _float_to_decimal(deepcopy(vessel))
            batch.put_item(Item=item)
    logger.info(f"Wrote {len(fleet)} vessels to DynamoDB")


#  Simulator Parameters Loading (with caching)

_params_cache: dict | None = None


def load_params() -> dict:
    """Load simulator parameters from S3 (cached for warm starts)."""
    global _params_cache
    if _params_cache is not None:
        return _params_cache

    if os.environ.get('LOCAL_CONFIG_PATH'):
        # Local testing mode
        with open(os.environ['LOCAL_CONFIG_PATH'], 'r') as f:
            _params_cache = json.load(f)
    else:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_CONFIG_KEY)
        _params_cache = json.loads(obj['Body'].read().decode('utf-8'))

    logger.info(f"Loaded params: {len(_params_cache['vessel_pool'])} vessels in pool")
    return _params_cache


#  Lambda Entry Point

def handler(event: dict, context) -> dict:
    """
    Main Lambda handler — triggered by EventBridge every 30 seconds.

    Workflow:
      1. Load simulator parameters (cached)
      2. Read current fleet from DynamoDB
      3. If table is empty → initialize fleet
      4. Update each vessel's position
      5. Batch write back to DynamoDB
    """
    params = load_params()

    # Connect to DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    table = dynamodb.Table(DYNAMODB_TABLE_LIVE)

    # Read current fleet
    fleet = read_fleet_from_dynamodb(table)

    if not fleet:
        # First run — initialize fleet
        logger.info("Empty table detected — initializing fleet")
        fleet = initialize_fleet(params)
    else:
        # Update each vessel
        for vessel in fleet:
            update_vessel(vessel, params, dt=DT)

    # Write updated fleet back to DynamoDB
    write_fleet_to_dynamodb(table, fleet)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Updated {len(fleet)} vessels",
            "timestamp": current_utc_iso(),
        }),
    }


#  Local Simulation Runner (no AWS dependency)

def run_local_simulation(config_path: str, steps: int = 10):
    """
    Run the simulation locally without DynamoDB.
    Useful for testing and debugging the algorithm.
    """
    with open(config_path, 'r') as f:
        params = json.load(f)

    # Initialize fleet
    fleet = initialize_fleet(params)

    print(f"Initialized {len(fleet)} vessels")
    print(f"{'Step':>4} | {'Vessel':18} | {'Type':10} | {'Status':12} | "
          f"{'Lat':>9} | {'Lng':>10} | {'SOG':>5} | {'COG':>6} | {'Route':18} | Track#")
    print("-" * 130)

    for step in range(steps):
        for vessel in fleet:
            update_vessel(vessel, params, dt=DT)

        # Print snapshot of first 5 vessels
        if step % 2 == 0:  # Print every other step
            for v in fleet[:5]:
                print(f"{step:4d} | {v['vessel_name']:18s} | {v['vessel_type']:10s} | "
                      f"{v['status']:12s} | {v['lat']:9.4f} | {v['lng']:10.4f} | "
                      f"{v['sog']:5.1f} | {v['cog']:6.1f} | {v['route_id']:18s} | "
                      f"{len(v.get('track', []))}")
            print("-" * 130)

    return fleet
