import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

KAFKA_CONSUMER_BROKER = os.getenv("KAFKA_CONSUMER_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", 1000))

# Kafka Consumer 설정
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_CONSUMER_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    max_poll_records=1000
)

# RDS 접속 정보
db_config = {
    'host': '***',
    'port': 5432,
    'dbname': 'dev',
    'user': '***',
    'password': '***'
}

insert_sql = """
    INSERT INTO raw_data.btc_raw_tick (
        trade_dt,
        trade_time,
        timestamp_ms,
        market,
        open_price,
        close_price,
        high_price,
        low_price,
        trade_price,
        trade_volume,
        trade_amount,
        bid_price_1,
        ask_price_1,
        total_bid_volume,
        total_ask_volume,
        stream_type
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (market, stream_type, timestamp_ms) DO NOTHING;
"""

print("=== Kafka Consumer 시작 ===")

def create_conn_cursor():
    while True:
        try:
            conn = psycopg2.connect(**db_config)
            conn.autocommit = False
            cur = conn.cursor()
            print("[INFO] DB 연결 성공")
            return conn, cur
        except psycopg2.OperationalError as e:
            print(f"[ERROR] DB 연결 실패: {e}")
            time.sleep(5)

# 초기 연결
conn, cur = create_conn_cursor()

batch = []
BATCH_SIZE = 3000
LAST_INSERT_TIME = time.time()
BATCH_INTERVAL = 10
POLL_TIMEOUT_MS = 1000
POLL_INTERVAL_SEC = 0.5  # 🔧 0.5초로 조정

try:
    while True:
        records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

        for tp, messages in records.items():
            for message in messages:
                try:
                    data = message.value

                    for key in ["candle_date_time_utc", "candle_date_time_kst"]:
                        if key in data:
                            dt_str = data[key]
                            if dt_str and ('T' not in dt_str) and len(dt_str) == 17:
                                dt_str = dt_str[:10] + 'T' + dt_str[10:]
                                data[key] = dt_str

                    market = data.get("code")
                    stream_type = data.get("type")
                    timestamp_ms = data.get("timestamp")

                    trade_dt = None
                    trade_time = None

                    utc_dt_str = data.get("candle_date_time_utc")
                    if utc_dt_str:
                        dt_obj = datetime.strptime(utc_dt_str, "%Y-%m-%dT%H:%M:%S")
                        trade_dt = dt_obj.date()
                        trade_time = dt_obj.time()

                    elif data.get("trade_date") and data.get("trade_time"):
                        trade_date_str = data["trade_date"].replace("-", "")
                        trade_time_str = data["trade_time"].replace(":", "")
                        combined_str = trade_date_str + trade_time_str

                        if len(combined_str) == 14:
                            try:
                                dt_obj = datetime.strptime(combined_str, "%Y%m%d%H%M%S")
                                trade_dt = dt_obj.date()
                                trade_time = dt_obj.time()
                            except ValueError as e:
                                print(f"[ERROR] datetime parsing failed: {e}")
                        else:
                            trade_dt = None
                            trade_time = None

                    elif timestamp_ms is not None:
                        dt_obj = datetime.utcfromtimestamp(timestamp_ms / 1000)
                        trade_dt = dt_obj.date()
                        trade_time = dt_obj.time()

                    open_price = close_price = high_price = low_price = None
                    trade_price = trade_volume = trade_amount = None
                    bid_price_1 = ask_price_1 = None
                    total_bid_volume = total_ask_volume = None

                    if stream_type == "ticker":
                        trade_price = data.get("trade_price")
                        trade_volume = data.get("acc_trade_volume_24h")
                        high_price = data.get("high_price")
                        low_price = data.get("low_price")
                        open_price = data.get("opening_price")
                        close_price = data.get("trade_price")

                    elif stream_type == "trade":
                        trade_price = data.get("trade_price")
                        trade_volume = data.get("trade_volume")
                        if trade_price is not None and trade_volume is not None:
                            trade_amount = trade_price * trade_volume

                    elif stream_type == "orderbook":
                        if data.get("orderbook_units"):
                            first_unit = data["orderbook_units"][0]
                            bid_price_1 = first_unit.get("bid_price")
                            ask_price_1 = first_unit.get("ask_price")
                            total_bid_volume = sum(
                                unit["bid_size"] for unit in data["orderbook_units"]
                            )
                            total_ask_volume = sum(
                                unit["ask_size"] for unit in data["orderbook_units"]
                            )

                    elif stream_type == "candle.1s":
                        open_price = data.get("opening_price")
                        close_price = data.get("trade_price")
                        high_price = data.get("high_price")
                        low_price = data.get("low_price")
                        trade_volume = data.get("candle_acc_trade_volume")
                        trade_amount = data.get("candle_acc_trade_price")
                        trade_price = data.get("trade_price")

                    row = (
                        trade_dt,
                        trade_time,
                        timestamp_ms,
                        market,
                        open_price,
                        close_price,
                        high_price,
                        low_price,
                        trade_price,
                        trade_volume,
                        trade_amount,
                        bid_price_1,
                        ask_price_1,
                        total_bid_volume,
                        total_ask_volume,
                        stream_type
                    )
                    batch.append(row)

                    if len(batch) >= BATCH_SIZE:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        print(f"[BATCH INSERT] {len(batch)} rows inserted.")
                        batch.clear()
                        LAST_INSERT_TIME = time.time()

                    elif time.time() - LAST_INSERT_TIME >= BATCH_INTERVAL and batch:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        print(f"[BATCH INSERT] {len(batch)} rows inserted (interval flush).")
                        batch.clear()
                        LAST_INSERT_TIME = time.time()

                except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                    print(f"[ERROR] DB 연결 문제: {e}")
                    conn, cur = create_conn_cursor()
                    continue

                except Exception as e:
                    print(f"[ERROR] 처리 중 오류 발생: {e}")
                    continue

        time.sleep(POLL_INTERVAL_SEC)  # 🔧 0.5초 대기

except KeyboardInterrupt:
    print("종료 신호 감지. 남은 배치 처리 중...")

if batch:
    cur.executemany(insert_sql, batch)
    conn.commit()
    print(f"[BATCH INSERT] {len(batch)} rows inserted on shutdown.")

cur.close()
conn.close()
