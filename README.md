# 💹 Financial Market ETL Pipeline

> **A production-grade, multi-cloud ETL pipeline** that ingests real-time financial market data, transforms it through a modern data warehouse, and visualizes insights through an interactive BI dashboard — fully automated with Apache Airflow.

<div align="center">

![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.0-017CEE?logo=apache-airflow)
![Azure](https://img.shields.io/badge/Azure%20Blob-Storage-0078D4?logo=microsoft-azure)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Tableau](https://img.shields.io/badge/Tableau-Dashboard-E97627?logo=tableau)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python)

</div>

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Pipeline Details](#-pipeline-details)
- [Snowflake Schema](#-snowflake-schema)
- [Dashboard](#-dashboard)
- [Troubleshooting](#-troubleshooting)
- [Team Setup Guide](#-team-setup-guide)

---

## 🔍 Overview

This project implements an **automated financial data pipeline** that:

- 📥 **Ingests** real-time stock prices (AAPL, GOOGL, MSFT, AMZN, TSLA) and forex rates (EUR/USD, USD/EGP, GBP/USD) from the Alpha Vantage API
- ☁️ **Stores** raw JSON data in Microsoft Azure Blob Storage as a data lake
- ❄️ **Transforms** data through a 3-layer Snowflake warehouse (RAW → STAGING → ANALYTICS)
- 📊 **Visualizes** insights through an interactive Tableau dashboard
- ⏰ **Automates** the entire workflow with Apache Airflow, running every weekday after market close

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE FLOW                         │
│                                                              │
│  ┌─────────────┐    ┌────────────┐    ┌──────────────────┐  │
│  │Alpha Vantage│───▶│  Apache    │───▶│  Azure Blob      │  │
│  │    API      │    │  Airflow   │    │  Storage         │  │
│  │             │    │  (Docker)  │    │  financial-raw/  │  │
│  │ 5 Stocks    │    │            │    │  raw_YYYYMMDD    │  │
│  │ 3 Forex     │    │etl_pipeline│    │  .json           │  │
│  └─────────────┘    └────────────┘    └────────┬─────────┘  │
│                                                │             │
│                                                ▼             │
│                           ┌────────────────────────────┐    │
│                           │        SNOWFLAKE           │    │
│                           │      FINANCIAL_DW          │    │
│                           │  RAW → STAGING → ANALYTICS │    │
│                           └──────────────┬─────────────┘    │
│                                          │                   │
│                                          ▼                   │
│                           ┌─────────────────────────┐       │
│                           │   TABLEAU DASHBOARD     │       │
│                           │  Financial Market View  │       │
│                           └─────────────────────────┘       │
└──────────────────────────────────────────────────────────────┘
```

### Airflow DAG Flow

```
[ingest] ──▶ [load_raw] ──▶ [transform] ──▶ [validate]
    │              │               │               │
Fetch API     Upload to       Snowflake        Row count
data with     Azure Blob    RAW→STAGING→       quality
rate limit    Storage        ANALYTICS          checks
handling
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8.0 (Docker) | Workflow scheduling & monitoring |
| **Data Source** | Alpha Vantage API (Free Tier) | Real-time financial market data |
| **Data Lake** | Azure Blob Storage | Raw JSON file storage |
| **Data Warehouse** | Snowflake | 3-layer transformation & analytics |
| **Visualization** | Tableau Desktop | Interactive BI dashboard |
| **Language** | Python 3.8+ | ETL scripting |
| **Containerization** | Docker & Docker Compose | Airflow deployment |

---

## 📁 Project Structure

```
etl-project/
│
├── dags/
│   ├── etl_pipeline.py          ✅ Main unified DAG (use this)
│   ├── financial_to_azure.py    Ingestion-only DAG
│   └── azure_to_snowflake.py    Load-only DAG
│
├── logs/                        Airflow task logs (auto-generated)
├── plugins/                     Airflow custom plugins
├── docker-compose.yml           Airflow Docker setup
└── README.md
```

---

## ✅ Prerequisites

```
□ Docker Desktop installed and running
□ Python 3.8+
□ Alpha Vantage API Key   →  https://www.alphavantage.co/support/#api-key
□ Azure Account           →  https://portal.azure.com
□ Snowflake Account       →  https://signup.snowflake.com  (Free 30-day trial)
□ Tableau Desktop         →  https://www.tableau.com/products/desktop/download
```

---

## 🚀 Quick Start

### Step 1 — Setup Project Folder

```bash
mkdir etl-project && cd etl-project
mkdir dags logs plugins
```

### Step 2 — Configure Your Credentials

Open `dags/etl_pipeline.py` and update these 3 values:

```python
ALPHA_API_KEY = "YOUR_API_KEY_HERE"

AZURE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

SNOWFLAKE_CFG = {
    "account":   "YOUR_ACCOUNT",   # e.g. yhzjeyv-vc91298
    "user":      "YOUR_USERNAME",
    "password":  "YOUR_PASSWORD",
    "database":  "FINANCIAL_DW",
    "warehouse": "ETL_WH",
    "role":      "ACCOUNTADMIN"
}
```

> 💡 **Finding your Snowflake account ID:** Open Snowflake in browser → copy the identifier shown on your profile (e.g. `yhzjeyv-vc91298`)

### Step 3 — Start Airflow with Docker

```bash
docker-compose up -d

docker exec -it etl-project-airflow-webserver-1 airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

docker exec -it etl-project-airflow-webserver-1 \
  pip install azure-storage-blob snowflake-connector-python requests
```

Open Airflow UI → **http://localhost:8080** `(admin / admin)`

### Step 4 — Setup Snowflake (run once)

```sql
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS FINANCIAL_DW;
USE DATABASE FINANCIAL_DW;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE TABLE IF NOT EXISTS RAW.STOCKS (
    SYMBOL VARCHAR(10), TRADE_DATE DATE,
    OPEN_PRICE FLOAT, HIGH_PRICE FLOAT, LOW_PRICE FLOAT,
    CLOSE_PRICE FLOAT, VOLUME BIGINT,
    INGESTED_AT TIMESTAMP_NTZ, SOURCE_FILE VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS RAW.FOREX (
    FROM_CURRENCY VARCHAR(5), TO_CURRENCY VARCHAR(5),
    EXCHANGE_RATE FLOAT, BID_PRICE FLOAT, ASK_PRICE FLOAT,
    LAST_UPDATED TIMESTAMP_NTZ, INGESTED_AT TIMESTAMP_NTZ,
    SOURCE_FILE VARCHAR(255)
);
```

### Step 5 — Run the Pipeline

```bash
docker exec -it etl-project-airflow-webserver-1 bash
airflow dags trigger etl_pipeline
airflow dags list-runs -d etl_pipeline   # get the run_id
airflow tasks run etl_pipeline ingest    PASTE_RUN_ID_HERE --local
airflow tasks run etl_pipeline load_raw  PASTE_RUN_ID_HERE --local
airflow tasks run etl_pipeline transform PASTE_RUN_ID_HERE --local
airflow tasks run etl_pipeline validate  PASTE_RUN_ID_HERE --local
```

### Step 6 — Verify in Snowflake

```sql
SELECT COUNT(*) FROM FINANCIAL_DW.RAW.STOCKS;
SELECT COUNT(*) FROM FINANCIAL_DW.RAW.FOREX;
SELECT * FROM FINANCIAL_DW.ANALYTICS.DAILY_STOCK_SUMMARY ORDER BY TRADE_DATE DESC;
SELECT * FROM FINANCIAL_DW.ANALYTICS.FOREX_RATES;
```

### Step 7 — Connect Tableau

1. Open Tableau Desktop → **Connect** → **Snowflake**
2. Server: `YOUR_ACCOUNT.snowflakecomputing.com`
3. Database: `FINANCIAL_DW` / Schema: `ANALYTICS`
4. Drag views onto canvas → build dashboard!

---

## 🔄 Pipeline Details

| Task | What it does | Time |
|------|-------------|------|
| `ingest` | Fetches API data with 13s delay between calls (rate limit) | ~2 min |
| `load_raw` | Reads Azure Blob JSON → inserts into Snowflake RAW | ~30s |
| `transform` | Rebuilds STAGING with PRICE_CHANGE, CHANGE_PCT, SPREAD, etc. | ~20s |
| `validate` | Row count checks — raises error if tables empty | ~10s |

**Schedule:** `0 18 * * 1-5` — Every weekday at 18:00 UTC

| Category | Symbols |
|----------|---------|
| US Stocks | AAPL, GOOGL, MSFT, AMZN, TSLA |
| Forex Pairs | EUR/USD, USD/EGP, GBP/USD |

---

## ❄️ Snowflake Schema

```
FINANCIAL_DW
├── RAW
│   ├── STOCKS          Raw daily stock prices
│   └── FOREX           Raw forex exchange rates
├── STAGING
│   ├── STOCKS_CLEAN    + PRICE_CHANGE, CHANGE_PCT, DAILY_RANGE, TYPICAL_PRICE
│   └── FOREX_CLEAN     + PAIR, SPREAD
└── ANALYTICS (Views for Tableau)
    ├── DAILY_STOCK_SUMMARY   with BUY/SELL signal
    ├── STOCK_COMPARISON      aggregated stats per symbol
    └── FOREX_RATES           with spread type
```

**Signal Logic:**

```
CHANGE_PCT >  2%  →  Strong Buy  🟢
CHANGE_PCT >  0%  →  Buy         🟩
CHANGE_PCT =  0%  →  Neutral     ⬜
CHANGE_PCT > -2%  →  Sell        🟧
CHANGE_PCT <= -2% →  Strong Sell 🔴
```

---

## 📊 Dashboard

| Chart | Type | Insight |
|-------|------|---------|
| KPI Cards | Bar Chart | Avg close price per stock |
| Price Over Time | Line Chart | Historical prices — AAPL, AMZN, TSLA |
| Volume Analysis | Stacked Bar | Volume colored by signal |
| Signal Distribution | Pie Chart | Buy/Sell breakdown |
| Forex Rates | Text Table | EUR/USD, GBP/USD, USD/EGP |
| Price Change % | Bar Chart | Daily change % per stock |

---

## 🔧 Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| Azure region not available | UAE North blocked on Student sub | Use **West Europe** or **East US** |
| Snowflake 404 Not Found | Wrong account identifier | Copy full account from browser URL |
| API rate limit message | 25 req/day free tier | Add `time.sleep(13)` between calls |
| DAG stays queued | Scheduler not running | Use `airflow tasks run --local` |
| XCom key not found | Tasks ran in separate sessions | Use single end-to-end Python script |
| Duplicate rows in Snowflake | Pipeline ran multiple times | `CREATE OR REPLACE TABLE AS SELECT DISTINCT *` |
| DagRunNotFound | Wrong execution date | Use full run_id from `airflow dags list-runs` |

---

## 👥 Team Setup Guide

Each team member needs:

1. **Their own free API keys** (Alpha Vantage, Azure, Snowflake)
2. **Update the 3 config values** in `etl_pipeline.py`
3. **Follow Steps 1–7** from Quick Start

| Service | Link |
|---------|------|
| Alpha Vantage | https://www.alphavantage.co/support/#api-key |
| Azure | https://portal.azure.com → Storage Account → West Europe region |
| Snowflake | https://signup.snowflake.com |

> ⚠️ **Never commit credentials to Git!**

```bash
echo ".env" >> .gitignore
echo "logs/" >> .gitignore
echo "__pycache__/" >> .gitignore
```

---

## 📋 Milestones

| # | Milestone | Status |
|---|-----------|--------|
| M1 | Data Collection — API → Azure Blob Storage | ✅ Done |
| M2 | Data Warehouse — Snowflake 3-layer schema | ✅ Done |
| M3 | Deployment — Airflow DAG end-to-end | ✅ Done |
| M4 | Monitoring — Tableau dashboard (6 charts) | ✅ Done |
| M5 | Documentation — README + Word report | ✅ Done |

---

## 👨‍💻 Author

**Ibraheem** — Computer & Systems Engineering, Zagazig University (Class of 2028)
Project 10 — Multi-Cloud ETL Pipeline · June 2026

---

<div align="center">
  <strong>Built with ❤️ using Apache Airflow · Microsoft Azure · Snowflake · Tableau</strong>
</div>
(Add instructions here once your repo is ready, e.g., how to run the DAGs or set up environment variables.)

