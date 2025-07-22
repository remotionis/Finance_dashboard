import os
import requests
import asyncio
import websockets
import json
import time
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_PRODUCER_BROKER = os.getenv("KAFKA_PRODUCER_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# ---------------------------
# Logging 설정
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

# ---------------------------
# producer 생성 함수
# ---------------------------
def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_PRODUCER_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        compression_type='lz4',
        linger_ms=50,
        batch_size=32768
    )
    return producer

# ---------------------------
# WebSocket 구독 실행 함수
# ---------------------------
async def run_ws():
    # ① 마켓 코드 가져오기
    resp = requests.get("https://api.upbit.com/v1/market/all")
    markets = resp.json()

    codes = [m['market'] for m in markets if m['market'].startswith('KRW-')]
    logging.info(f"구독할 종목 수: {len(codes)}")

    subscribe_msg = [
        {"ticket": "all-in-one"},
        {"type": "ticker", "codes": codes},
        {"type": "trade", "codes": codes},
        {"type": "orderbook", "codes": codes},
        {"type": "candle.1s", "codes": codes}
    ]

    producer = create_producer()
    topic = KAFKA_TOPIC
    url = "wss://api.upbit.com/websocket/v1"

    counter = {}
    last_print = time.time()

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**23
    ) as ws:
        await ws.send(json.dumps(subscribe_msg))
        logging.info("구독 메시지 전송 완료")

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            # candle_date_time 보정
            for key in ["candle_date_time_utc", "candle_date_time_kst"]:
                if key in data:
                    dt_str = data[key]
                    if dt_str and ('T' not in dt_str) and len(dt_str) == 17:
                        dt_str = dt_str[:10] + 'T' + dt_str[10:]
                        data[key] = dt_str

            # Kafka로 전송
            producer.send(topic, value=data)

            # counter 증가
            code = data.get("code")
            if code:
                counter[code] = counter.get(code, 0) + 1

            # 30초마다 Top5 출력
            if time.time() - last_print > 30:
                logging.info("==== 종목별 수신 건수 (Top 5) ====")
                top5 = sorted(counter.items(), key=lambda x: -x[1])[:5]
                for c, cnt in top5:
                    logging.info(f"{c}: {cnt}건")
                last_print = time.time()

# ---------------------------
# 무한 재접속 루프
# ---------------------------
async def upbit_stream_all_to_kafka():
    while True:
        try:
            await run_ws()
        except Exception as e:
            logging.error(f"[ERROR] {e}. 5초 후 재접속 시도...")
            time.sleep(5)

# ---------------------------
# 실행
# ---------------------------
if __name__ == "__main__":
    asyncio.run(upbit_stream_all_to_kafka())
