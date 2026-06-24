import snowflake.connector
from datetime import datetime, timedelta
import random

# إعدادات الاتصال بقاعدة بيانات Snowflake الخاصة بك
conn_params = {
    "user": "HIMASHAABAN",
    "password": "hima@01200694266G",
    "account": "yhzjeyv-vc91298",
    "warehouse": "ETL_WH",
    "database": "FINANCIAL_DW",
    "schema": "RAW"  # سنبدأ بالتعامل مع الطبقة الخام RAW أولاً
}

def backfill_data():
    try:
        # 1. إنشاء الاتصال بـ Snowflake
        print("جاري الاتصال بقاعدة بيانات Snowflake...")
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        # تفعيل الـ Warehouse والـ Database
        cursor.execute("USE WAREHOUSE ETL_WH;")
        cursor.execute("USE DATABASE FINANCIAL_DW;")
        
        print("تم الاتصال بنجاح! 🚀")
        
        # 2. تنظيف جداول الـ RAW القديمة للبدء على نظافة
        print("جاري تنظيف وتصفير جداول البيانات الخام (RAW)...")
        cursor.execute("TRUNCATE TABLE RAW.STOCKS;")
        cursor.execute("TRUNCATE TABLE RAW.FOREX;")
        
        # 3. توليد وإدخال بيانات الأسهم التاريخية في RAW.STOCKS
        stocks = {
            "AAPL": {"base_price": 175.0, "vol_base": 50000000},
            "AMZN": {"base_price": 145.0, "vol_base": 40000000},
            "TSLA": {"base_price": 210.0, "vol_base": 80000000}
        }
        
        today = datetime.now()
        ingested_at = today.strftime('%Y-%m-%d %H:%M:%S')
        source_file = "historical_backfill_azure.json"
        
        print("جاري توليد وإدخال بيانات الأسهم التاريخية في RAW.STOCKS...")
        for symbol, info in stocks.items():
            current_price = info["base_price"]
            for d in range(30, -1, -1):
                trade_date = (today - timedelta(days=d)).strftime('%Y-%m-%d')
                
                # تخطي عطلات البورصة الأسبوعية
                date_obj = today - timedelta(days=d)
                if date_obj.weekday() in [5, 6]: 
                    continue
                
                pct_change = random.uniform(-0.025, 0.025)
                price_change = current_price * pct_change
                close_price = round(current_price + price_change, 2)
                open_price = round(current_price, 2)
                high_price = round(max(open_price, close_price) * random.uniform(1.0, 1.015), 2)
                low_price = round(min(open_price, close_price) * random.uniform(0.985, 1.0), 2)
                volume = int(info["vol_base"] * random.uniform(0.7, 1.4))
                
                # إدخال السجل في الجدول الخام
                insert_query = f"""
                INSERT INTO RAW.STOCKS (SYMBOL, TRADE_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, INGESTED_AT, SOURCE_FILE)
                VALUES ('{symbol}', '{trade_date}', {open_price}, {high_price}, {low_price}, {close_price}, {volume}, '{ingested_at}', '{source_file}');
                """
                cursor.execute(insert_query)
                current_price = close_price
                
        # 4. توليد وإدخال بيانات العملات التاريخية في RAW.FOREX
        print("جاري توليد وإدخال بيانات العملات التاريخية في RAW.FOREX...")
        forex_pairs = [
            {"from": "USD", "to": "EGP", "rate": 48.50},
            {"from": "EUR", "to": "USD", "rate": 1.08},
            {"from": "GBP", "to": "USD", "rate": 1.27}
        ]
        
        for pair in forex_pairs:
            current_rate = pair["rate"]
            for d in range(30, -1, -1):
                trade_date = (today - timedelta(days=d)).strftime('%Y-%m-%d %H:%M:%S')
                
                pct_change = random.uniform(-0.003, 0.003)
                current_rate = round(current_rate * (1 + pct_change), 4)
                bid_price = round(current_rate * 0.999, 4)
                ask_price = round(current_rate * 1.001, 4)
                
                insert_query = f"""
                INSERT INTO RAW.FOREX (FROM_CURRENCY, TO_CURRENCY, EXCHANGE_RATE, BID_PRICE, ASK_PRICE, LAST_UPDATED, INGESTED_AT, SOURCE_FILE)
                VALUES ('{pair["from"]}', '{pair["to"]}', {current_rate}, {bid_price}, {ask_price}, '{trade_date}', '{ingested_at}', '{source_file}');
                """
                cursor.execute(insert_query)

        # 5. تشغيل أوامر التحويل والـ ETL لتحديث الـ Staging تلقائياً
        print("جاري تشغيل خط أنابيب التحويل (ETL Pipelines) لتحديث طبقة الـ STAGING...")
        
        # تحديث STAGING.STOCKS_CLEAN
        cursor.execute("""
        CREATE OR REPLACE TABLE STAGING.STOCKS_CLEAN AS
        SELECT *,
            ROUND(CLOSE_PRICE - OPEN_PRICE, 4) AS PRICE_CHANGE,
            ROUND((CLOSE_PRICE - OPEN_PRICE) / NULLIF(OPEN_PRICE,0) * 100, 2) AS CHANGE_PCT,
            ROUND(HIGH_PRICE - LOW_PRICE, 4) AS DAILY_RANGE,
            ROUND((HIGH_PRICE + LOW_PRICE + CLOSE_PRICE) / 3, 4) AS TYPICAL_PRICE
        FROM RAW.STOCKS
        WHERE CLOSE_PRICE > 0 AND VOLUME > 0;
        """)
        
        # تحديث STAGING.FOREX_CLEAN
        cursor.execute("""
        CREATE OR REPLACE TABLE STAGING.FOREX_CLEAN AS
        SELECT *,
            CONCAT(FROM_CURRENCY,'/',TO_CURRENCY) AS PAIR,
            ROUND(ASK_PRICE - BID_PRICE, 6) AS SPREAD
        FROM RAW.FOREX
        WHERE EXCHANGE_RATE > 0;
        """)

        print("\nعملية التغذية التاريخية وإعادة تشغيل الـ ETL تمت بنجاح تام! 🎉")
        print("الآن جميع الـ Analytics Views تم تحديثها تلقائياً بالبيانات الجديدة.")
        print("افتح Tableau واعمل Refresh للبيانات لمشاهدة خطوط الأسعار الرائعة والكاملة! 📊🔥")
        
    except Exception as e:
        print(f"حدث خطأ أثناء تشغيل خط الأنابيب: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    backfill_data()