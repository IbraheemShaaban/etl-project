from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# الـ Hook الجديد الآمن للتعامل مع Azure Blob Storage
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

ALPHA_API_KEY  = "6JXBBVLASZJVR1F9"
CONTAINER_NAME = "financial-raw"
BASE_URL       = "https://www.alphavantage.co/query"

STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
FOREX  = [("EUR", "USD"), ("USD", "EGP"), ("GBP", "USD")]

def fetch_stock_data(**context):
    results = []
    for symbol in STOCKS:
        r = requests.get(BASE_URL, params={
            "function":   "TIME_SERIES_DAILY",
            "symbol":     symbol,
            "apikey":     ALPHA_API_KEY,
            "outputsize": "compact"
        })
        data = r.json()
        if "Time Series (Daily)" in data:
            ts   = data["Time Series (Daily)"]
            date = sorted(ts.keys())[-1]
            row  = ts[date]
            results.append({
                "symbol":      symbol,
                "date":        date,
                "open":        float(row["1. open"]),
                "high":        float(row["2. high"]),
                "low":         float(row["3. low"]),
                "close":       float(row["4. close"]),
                "volume":      int(row["5. volume"]),
                "ingested_at": datetime.utcnow().isoformat()
            })
            print(f"✅ {symbol}: ${row['4. close']}")
        else:
            print(f"❌ {symbol}: {data}")
    context['ti'].xcom_push(key='stock_data', value=results)

def fetch_forex_data(**context):
    results = []
    for fc, tc in FOREX:
        r = requests.get(BASE_URL, params={
            "function":      "CURRENCY_EXCHANGE_RATE",
            "from_currency": fc,
            "to_currency":   tc,
            "apikey":        ALPHA_API_KEY
        })
        data = r.json()
        if "Realtime Currency Exchange Rate" in data:
            rate = data["Realtime Currency Exchange Rate"]
            results.append({
                "from_currency": fc,
                "to_currency":   tc,
                "exchange_rate": float(rate["5. Exchange Rate"]),
                "bid_price":     float(rate["8. Bid Price"]),
                "ask_price":     float(rate["9. Ask Price"]),
                "last_updated":  rate["6. Last Refreshed"],
                "ingested_at":   datetime.utcnow().isoformat()
            })
            print(f"✅ {fc}/{tc}: {rate['5. Exchange Rate']}")
        else:
            print(f"❌ {fc}/{tc}: {data}")
    context['ti'].xcom_push(key='forex_data', value=results)

def upload_to_azure(**context):
    stocks = context['ti'].xcom_pull(key='stock_data', task_ids='fetch_stocks')
    forex  = context['ti'].xcom_pull(key='forex_data', task_ids='fetch_forex')

    payload   = {
        "stocks": stocks,
        "forex":  forex,
        "run_at": datetime.utcnow().isoformat()
    }
    ts        = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"financial/raw_{ts}.json"

    # ── الرفع الآمن إلى Azure باستخدام الـ Hook ──
    azure_hook = WasbHook(wasb_conn_id='azure_blob_connection')
    azure_hook.load_string(string_data=json.dumps(payload, indent=2), container_name=CONTAINER_NAME, blob_name=blob_name, overwrite=True)

    print(f"🎉 اترفع: {blob_name}")
    print(f"   Stocks: {len(stocks)} | Forex: {len(forex)}")

default_args = {
    "owner":       "ibraheem",
    "retries":     2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="financial_to_azure",
    default_args=default_args,
    description="جلب بيانات الأسهم والعملات ورفعها على Azure",
    schedule_interval="0 18 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "azure", "etl"],
) as dag:

    fetch_stocks = PythonOperator(
        task_id="fetch_stocks",
        python_callable=fetch_stock_data,
    )

    fetch_forex = PythonOperator(
        task_id="fetch_forex",
        python_callable=fetch_forex_data,
    )

    upload = PythonOperator(
        task_id="upload_to_azure",
        python_callable=upload_to_azure,
    )

    [fetch_stocks, fetch_forex] >> upload