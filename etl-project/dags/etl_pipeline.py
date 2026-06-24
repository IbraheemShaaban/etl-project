from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, time, snowflake.connector
from azure.storage.blob import BlobServiceClient

# ── Config ────────────────────────────────────────────
ALPHA_API_KEY  = "6JXBBVLASZJVR1F9"
AZURE_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=rawdataaamaria;AccountKey=K4cKENc6uI0d76Y+6ltd3fc7CsWjfYQj2Gjbhh5BC6HxtWgyNlaGngiwaLCfk0vqrzPEW5EHbEiF+AStZ9NEIw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "financial-raw"
BASE_URL       = "https://www.alphavantage.co/query"

SNOWFLAKE_CFG = {
    "account":   "yhzjeyv-vc91298",
    "user":      "HIMASHAABAN",
    "password":  "hima@01200694266G",
    "database":  "FINANCIAL_DW",
    "warehouse": "ETL_WH",
    "role":      "ACCOUNTADMIN"
}

STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
FOREX  = [("EUR","USD"), ("USD","EGP"), ("GBP","USD")]

# ── Task 1: Ingest ────────────────────────────────────
def ingest_to_azure(**context):
    stocks, forex = [], []

    for symbol in STOCKS:
        r    = requests.get(BASE_URL, params={"function":"TIME_SERIES_DAILY","symbol":symbol,"apikey":ALPHA_API_KEY,"outputsize":"compact"})
        data = r.json()
        if "Time Series (Daily)" in data:
            ts   = data["Time Series (Daily)"]
            date = sorted(ts.keys())[-1]
            row  = ts[date]
            stocks.append({"symbol":symbol,"date":date,"open":float(row["1. open"]),"high":float(row["2. high"]),"low":float(row["3. low"]),"close":float(row["4. close"]),"volume":int(row["5. volume"]),"ingested_at":datetime.utcnow().isoformat()})
            print(f"✅ {symbol}: ${row['4. close']}")
        else:
            print(f"⚠️ {symbol}: rate limit")
        time.sleep(13)

    for fc, tc in FOREX:
        r    = requests.get(BASE_URL, params={"function":"CURRENCY_EXCHANGE_RATE","from_currency":fc,"to_currency":tc,"apikey":ALPHA_API_KEY})
        data = r.json()
        if "Realtime Currency Exchange Rate" in data:
            rate = data["Realtime Currency Exchange Rate"]
            forex.append({"from_currency":fc,"to_currency":tc,"exchange_rate":float(rate["5. Exchange Rate"]),"bid_price":float(rate["8. Bid Price"]),"ask_price":float(rate["9. Ask Price"]),"last_updated":rate["6. Last Refreshed"],"ingested_at":datetime.utcnow().isoformat()})
            print(f"✅ {fc}/{tc}: {rate['5. Exchange Rate']}")
        else:
            print(f"⚠️ {fc}/{tc}: rate limit")
        time.sleep(13)

    ts        = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"financial/raw_{ts}.json"
    client    = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob      = client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob.upload_blob(json.dumps({"stocks":stocks,"forex":forex,"run_at":datetime.utcnow().isoformat()}, indent=2), overwrite=True)
    print(f"🎉 Azure: {blob_name} | Stocks:{len(stocks)} Forex:{len(forex)}")
    context['ti'].xcom_push(key='blob_name', value=blob_name)

# ── Task 2: Load RAW ──────────────────────────────────
def load_to_snowflake(**context):
    blob_name = context['ti'].xcom_pull(key='blob_name', task_ids='ingest')
    client    = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    data      = json.loads(client.get_blob_client(container=CONTAINER_NAME, blob=blob_name).download_blob().readall())

    conn = snowflake.connector.connect(**SNOWFLAKE_CFG)
    cs   = conn.cursor()

    for r in data["stocks"]:
        cs.execute("INSERT INTO FINANCIAL_DW.RAW.STOCKS (SYMBOL,TRADE_DATE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,VOLUME,INGESTED_AT,SOURCE_FILE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (r["symbol"],r["date"],r["open"],r["high"],r["low"],r["close"],r["volume"],r["ingested_at"],blob_name))

    for r in data["forex"]:
        cs.execute("INSERT INTO FINANCIAL_DW.RAW.FOREX (FROM_CURRENCY,TO_CURRENCY,EXCHANGE_RATE,BID_PRICE,ASK_PRICE,LAST_UPDATED,INGESTED_AT,SOURCE_FILE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (r["from_currency"],r["to_currency"],r["exchange_rate"],r["bid_price"],r["ask_price"],r["last_updated"],r["ingested_at"],blob_name))

    cs.close(); conn.close()
    print(f"✅ Snowflake RAW: Stocks:{len(data['stocks'])} Forex:{len(data['forex'])}")

# ── Task 3: Transform ─────────────────────────────────
def transform_staging(**context):
    conn = snowflake.connector.connect(**SNOWFLAKE_CFG)
    cs   = conn.cursor()

    cs.execute("""
        CREATE OR REPLACE TABLE FINANCIAL_DW.STAGING.STOCKS_CLEAN AS
        SELECT *,
            ROUND(CLOSE_PRICE - OPEN_PRICE, 4) AS PRICE_CHANGE,
            ROUND((CLOSE_PRICE - OPEN_PRICE) / NULLIF(OPEN_PRICE,0) * 100, 2) AS CHANGE_PCT,
            ROUND(HIGH_PRICE - LOW_PRICE, 4) AS DAILY_RANGE,
            ROUND((HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) / 3, 4) AS TYPICAL_PRICE
        FROM FINANCIAL_DW.RAW.STOCKS
        WHERE CLOSE_PRICE > 0 AND VOLUME > 0
    """)

    cs.execute("""
        CREATE OR REPLACE TABLE FINANCIAL_DW.STAGING.FOREX_CLEAN AS
        SELECT *,
            CONCAT(FROM_CURRENCY,'/',TO_CURRENCY) AS PAIR,
            ROUND(ASK_PRICE - BID_PRICE, 6) AS SPREAD
        FROM FINANCIAL_DW.RAW.FOREX
        WHERE EXCHANGE_RATE > 0
    """)

    cs.close(); conn.close()
    print("✅ STAGING محدّثة")

# ── Task 4: Validate ──────────────────────────────────
def validate_data(**context):
    conn = snowflake.connector.connect(**SNOWFLAKE_CFG)
    cs   = conn.cursor()

    cs.execute("SELECT COUNT(*) FROM FINANCIAL_DW.RAW.STOCKS")
    stocks_count = cs.fetchone()[0]

    cs.execute("SELECT COUNT(*) FROM FINANCIAL_DW.RAW.FOREX")
    forex_count = cs.fetchone()[0]

    cs.execute("SELECT COUNT(*) FROM FINANCIAL_DW.STAGING.STOCKS_CLEAN")
    staging_count = cs.fetchone()[0]

    cs.close(); conn.close()

    print(f"📊 RAW Stocks: {stocks_count} | RAW Forex: {forex_count} | Staging: {staging_count}")

    if stocks_count == 0:
        raise ValueError("❌ RAW.STOCKS فاضية!")
    print("✅ Validation passed!")

# ── DAG ───────────────────────────────────────────────
default_args = {
    "owner":            "ibraheem",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL كامل: API → Azure → Snowflake",
    schedule_interval="0 18 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "finance", "azure", "snowflake"],
) as dag:

    t1 = PythonOperator(task_id="ingest",    python_callable=ingest_to_azure)
    t2 = PythonOperator(task_id="load_raw",  python_callable=load_to_snowflake)
    t3 = PythonOperator(task_id="transform", python_callable=transform_staging)
    t4 = PythonOperator(task_id="validate",  python_callable=validate_data)

    t1 >> t2 >> t3 >> t4