# FDA Adverse Events Pipeline

A production-grade **Streaming-to-Analytics Bridge** that processes FDA drug adverse event reports through a medallion architecture, demonstrating real-world data engineering patterns for pharmacovigilance analytics.

## Overview

This pipeline ingests real adverse event data from the [OpenFDA API](https://open.fda.gov/apis/drug/event/), processes it through Bronze → Silver → Gold layers, and delivers actionable analytics for drug safety monitoring.

**Why This Data?**
- **Real & Messy**: Inconsistent drug names, missing fields, varied reporting formats
- **Late-Arriving Events**: Reports submitted weeks/months after incidents (`receivedate` vs `occurdate`)
- **High Volume**: 2M+ adverse event reports, updated weekly
- **Industry Relevance**: Pharmaceutical companies invest millions in this exact analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FDA Adverse Events Pipeline                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
    │   OpenFDA    │     │    Kafka     │     │  PostgreSQL  │
    │     API      │────▶│   (Events)   │────▶│   (Bronze)   │
    └──────────────┘     └──────────────┘     └──────────────┘
                                                     │
                              ┌───────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MEDALLION ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   🥉 BRONZE (Raw)          🥈 SILVER (Clean)         🥇 GOLD (Analytics)    │
│   ┌──────────────┐        ┌──────────────┐         ┌──────────────┐        │
│   │ raw_adverse_ │        │ clean_events │         │ daily_kpis   │        │
│   │ events       │───────▶│ clean_drugs  │────────▶│ drug_trends  │        │
│   │ raw_drugs    │        │ clean_       │         │ geo_hotspots │        │
│   │ raw_patients │        │ reactions    │         │ severity_    │        │
│   └──────────────┘        └──────────────┘         │ metrics      │        │
│                                                     └──────────────┘        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │    Streamlit     │
                    │    Dashboard     │
                    └──────────────────┘
```

## Data Flow

### 1. Ingestion Layer
- **Source**: OpenFDA Drug Adverse Event API
- **Method**: Incremental pulls based on `receivedate`
- **Format**: Nested JSON with variable schema
- **Streaming Simulation**: Kafka topics for event-driven processing

### 2. Bronze Layer (Raw)
- Raw API responses stored as-is
- Metadata: `ingested_at`, `api_version`, `batch_id`
- No transformations - preserves data lineage

### 3. Silver Layer (Clean)
- Schema enforcement and type casting
- Null handling and deduplication
- Drug name standardization (brand → generic)
- Patient demographic normalization

### 4. Gold Layer (Analytics)
- **Daily KPIs**: Report volumes, severity distribution
- **Drug Trends**: Adverse events by drug over time
- **Geographic Analysis**: Hotspots by state/country
- **Severity Scoring**: Weighted risk metrics

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Ingestion** | Python + Requests | API data extraction |
| **Streaming** | Apache Kafka | Event-driven processing |
| **Database** | PostgreSQL | Medallion layer storage |
| **Transformation** | dbt | SQL-based transformations |
| **Orchestration** | Prefect | Workflow management |
| **Data Quality** | Great Expectations | Validation & monitoring |
| **Dashboard** | Streamlit | Analytics visualization |
| **Containerization** | Docker | Reproducible environments |

## Key Features

### Late-Arriving Event Handling
```
Event Timeline:

  Incident occurs     Report submitted      Processed
       │                    │                  │
       ▼                    ▼                  ▼
  ────[X]────────────────[R]────────────────[P]────▶ time
       │                    │
       └────── Gap: days to months ──────┘

Strategy: Watermarks + incremental aggregation with backfill support
```

### Out-of-Order Processing
- Events may arrive out of sequence relative to occurrence
- Implemented using event-time windowing
- Configurable late-arrival tolerance

### Backfill & Replay
- Full historical backfill capability
- Idempotent processing for replay scenarios
- Checkpoint-based recovery

## Project Structure

```
fda-adverse-events-pipeline/
├── src/
│   ├── ingestion/          # API extraction scripts
│   │   ├── fda_client.py   # OpenFDA API client
│   │   └── kafka_producer.py
│   ├── bronze/             # Raw data loaders
│   │   └── load_raw_events.py
│   ├── silver/             # Cleaning transformations
│   │   └── clean_events.py
│   ├── gold/               # Analytics aggregations
│   │   └── build_kpis.py
│   ├── orchestration/      # Prefect flows
│   │   └── pipeline_flow.py
│   └── monitoring/         # Data quality checks
│       └── quality_checks.py
├── dbt/                    # dbt transformations
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── dashboards/             # Streamlit app
│   └── app.py
├── config/                 # Configuration files
│   ├── settings.yaml
│   └── .env.template
├── tests/                  # Unit & integration tests
├── docs/                   # Documentation
│   └── architecture.md
├── docker-compose.yml      # Local development stack
├── requirements.txt        # Python dependencies
└── README.md
```

## OpenFDA API Reference

**Endpoint**: `https://api.fda.gov/drug/event.json`

**Key Fields**:
| Field | Description |
|-------|-------------|
| `safetyreportid` | Unique report identifier |
| `receivedate` | When FDA received the report |
| `occurcountry` | Country where event occurred |
| `serious` | Severity indicator (1 = serious) |
| `seriousnessdeath` | Death outcome flag |
| `patient.drug[]` | Array of drugs involved |
| `patient.reaction[]` | Array of adverse reactions |

**Example Request**:
```bash
curl "https://api.fda.gov/drug/event.json?search=receivedate:[20240101+TO+20240131]&limit=100"
```

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Docker & Docker Compose (optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/fda-adverse-events-pipeline.git
cd fda-adverse-events-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp config/.env.template config/.env
# Edit .env with your database credentials

# Initialize database
python src/bronze/init_db.py

# Run the pipeline
python src/orchestration/pipeline_flow.py
```

### Docker Setup (Recommended)

```bash
# Start all services (Postgres, Kafka, Streamlit)
docker-compose up -d

# Run pipeline
docker-compose exec app python src/orchestration/pipeline_flow.py

# Access dashboard
open http://localhost:8501
```

## Analytics Use Cases

### 1. Drug Safety Monitoring
- Track adverse event trends for specific drugs
- Identify emerging safety signals
- Compare safety profiles across drug classes

### 2. Geographic Analysis
- Map adverse event hotspots
- Identify regional reporting patterns
- Cross-reference with population data

### 3. Severity Scoring
- Weighted risk scoring algorithm
- Seriousness indicator analysis
- Outcome-based severity rankings

### 4. Temporal Analysis
- Seasonality in adverse events
- Time-to-report distributions
- Late-arriving event patterns

## Data Quality Checks

| Check | Layer | Description |
|-------|-------|-------------|
| Schema validation | Bronze | Ensure required fields present |
| Null percentage | Silver | Flag tables exceeding null thresholds |
| Duplicate detection | Silver | Identify and remove duplicates |
| Row count anomaly | Gold | Alert on unexpected volume changes |
| Freshness check | All | Ensure data recency SLAs |

## Roadmap

- [x] Project scaffolding
- [ ] OpenFDA API client implementation
- [ ] Kafka streaming infrastructure
- [ ] Bronze layer PostgreSQL schema
- [ ] Silver layer transformations (dbt)
- [ ] Gold layer aggregations
- [ ] Prefect orchestration flows
- [ ] Great Expectations integration
- [ ] Streamlit dashboard
- [ ] Docker containerization
- [ ] CI/CD pipeline
- [ ] Documentation & architecture diagrams

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [OpenFDA](https://open.fda.gov/) for providing free access to FDA data
- The data engineering community for medallion architecture patterns
