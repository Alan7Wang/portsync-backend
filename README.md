# PortSync — Smart Ocean Shipping Tracker (Backend)

**CS5224 Cloud Computing · Team 18 · AY2024-25 Semester 2**

Serverless backend for the PortSync marine traffic tracking system, deployed on AWS.

## Architecture

```
EventBridge (1 min)  ──▶  Data Simulator Lambda  ──▶  DynamoDB (Live State)

React Frontend  ──▶  API Gateway  ──▶  API Handler Lambda  ──▶  DynamoDB (live)
                         │                                  ──▶  S3 Data Lake (historical)
                    Cognito JWT
```

## Project Structure

```
portsync-backend/
├── api_handler/
│   ├── api_handler.py          # REST API — routes, bbox filtering, S3 CSV reads
│   └── data/                   # Historical datasets (data.gov.sg)
│       ├── cargo_total.csv
│       ├── cargo_breakdown.csv
│       ├── container.csv
│       └── vessel_calls.csv
└── data_simulator/
    ├── data_simulator.py       # Parameterized Route Advancement Model
    └── config/
        └── simulator_params.json   # NOAA AIS-calibrated vessel pool & speed distributions
```

## Tech Stack

| Component | Service |
|-----------|---------|
| Compute | AWS Lambda (Python 3.12) |
| API | Amazon API Gateway (REST, Lambda Proxy) |
| Real-time DB | Amazon DynamoDB (`PK=tenant_id`, `SK=mmsi`) |
| Historical Data | Amazon S3 (CSV data lake) |
| Scheduler | Amazon EventBridge — `rate(1 minute)` |
| Auth | Amazon Cognito (JWT) |
| Frontend | React + AWS Amplify |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/live/vessels` | List all live vessels (supports `bbox`, `vessel_type` filters) |
| `GET /api/live/vessels/{mmsi}` | Single vessel detail with 10-point track history |
| `GET /api/historical/cargo/total` | Monthly cargo throughput |
| `GET /api/historical/cargo/breakdown` | Cargo by category |
| `GET /api/historical/container` | Container throughput in TEUs |
| `GET /api/historical/vessel-calls` | Vessel arrivals by purpose |

## Simulation Algorithm

The Data Simulator implements a **Parameterized Route Advancement Model**:

- Vessels follow pre-defined Singapore Strait route templates (TSS lanes, port approaches, anchorages)
- Speed and heading constraints are sampled from real **NOAA AIS statistical distributions**
- Position updated every 60s using dead-reckoning with Gaussian heading jitter
- Boundary detection resets vessels to route entry points when out-of-bounds

## Related Repositories

- **Frontend Dashboard**: [portsync-dashboard](https://github.com/xa0412/portsync-dashboard)
- **Live Demo**: https://main.d3quyf2hw1edm4.amplifyapp.com/login
