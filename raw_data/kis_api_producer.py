# -*- coding: utf-8 -*-
import kis_auth as kis
import asyncio
import aiohttp
import hashlib
import pandas as pd
import json
import time

from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from more_itertools import chunked

# 설정
KST = timezone(timedelta(hours=9))
MIN_FETCH_INTERVAL = timedelta(seconds=30)
MAX_REQUESTS_PER_SECOND = 10
recent_fetch_time = {}
sent_cache = {}

TRS = [
    {'tr_id': 'FHKST01010100', 'url': '/uapi/domestic-stock/v1/quotations/inquire-price'},
    {'tr_id': 'FHKST01010200', 'url': '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'}
]

BASE_URL = "https://openapi.koreainvestment.com:9443"

# KafkaProducer 재생성 함수
def create_kafka_producer():
    while True:
        try:
            print("[Kafka] KafkaProducer 연결 시도 중...")
            return KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                acks=1,
                linger_ms=20,
                batch_size=32768,
                compression_type='gzip',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable as e:
            print("[Kafka] 브로커 연결 실패. 5초 후 재시도:", e)
            time.sleep(5)

# 중복 전송 방지
def is_duplicate(data):
    key = json.dumps(data, sort_keys=True)
    key_hash = hashlib.md5(key.encode()).hexdigest()
    if key_hash in sent_cache:
        return True
    sent_cache[key_hash] = True
    return False

# Kafka 전송 함수
def send_to_kafka(data):
    global producer
    now = datetime.now(tz=KST)
    enriched = {
        "timestamp": int(time.time()),
        "fields": {
            "stock_dt": now.strftime("%Y-%m-%d"),
            "stock_time": now.strftime("%H:%M:%S"),
            **data
        }
    }
    if is_duplicate(enriched):
        return

    try:
        producer.send("stock_trade", enriched)
    except (KafkaError, NoBrokersAvailable) as e:
        print("[Kafka] 전송 실패. 프로듀서 재생성:", e)
        try:
            producer.close()
        except Exception:
            pass
        producer = create_kafka_producer()

# KRX 종목 목록 로딩 (예외 포함)
def load_allstock_KRX():
    try:
        krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        stk_data = pd.read_html(krx_url, encoding='cp949', header=0)[0]
        stk_data = stk_data[['회사명', '종목코드']]
        stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})
        return stk_data
    except Exception as e:
        print("[KRX] 종목 목록 로딩 실패:", e)
        return pd.DataFrame(columns=['Name', 'Code'])

# API 요청 (예외 포함)
async def fetch_one(session, tr, code):
    url = f"{kis.getTREnv().my_url}{tr['url']}"
    tr_id = tr['tr_id']
    if tr_id[0] in ('T', 'J', 'C') and kis.isPaperTrading():
        tr_id = 'V' + tr_id[1:]

    headers = kis._getBaseHeader()
    headers["tr_id"] = tr_id
    headers["custtype"] = "P"
    headers["tr_cont"] = "application/json"
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code
    }

    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status != 200:
                text = await response.text()
                print(f"[API] [{code}] {tr_id} 오류 | status={response.status} | {text}")
                return {"rt_cd": "-1", "msg1": "HTTP Error"}
            return await response.json()
    except Exception as e:
        print(f"[API] [{code}] {tr_id} 요청 실패:", e)
        return {"rt_cd": "-1", "msg1": str(e)}

# 종목 하나 수집
async def fetch_stock_data(session, name, code):
    now = datetime.now(tz=KST)
    if code in recent_fetch_time and now - recent_fetch_time[code] < MIN_FETCH_INTERVAL:
        return
    recent_fetch_time[code] = now

    full_data = {"stock_name": name, "stock_code": code}

    try:
        for tr in TRS:
            res = await fetch_one(session, tr, code)
            if res.get("rt_cd") != "0":
                print(f"[fetch] 실패: {code} | {tr['tr_id']} | {res.get('msg1')}")
                continue

            try:
                output = res.get("output", {})
                output1 = res.get("output1", {})
                output2 = res.get("output2", {})
                if output1 and output2:
                    output1.update(output2)
                    full_data.update(output1)
                elif output:
                    full_data.update(output)
            except Exception as e:
                print(f"[fetch] 응답 파싱 오류: {e}")

        send_to_kafka(full_data)

    except Exception as e:
        print(f"[fetch_stock_data] 처리 중 예외 발생: {e}")

# 메인 루프
async def main_loop():
    try:
        kis.auth()
    except Exception as e:
        print("[인증] 실패:", e)
        return

    allstock_df = load_allstock_KRX()
    if allstock_df.empty:
        print("[KRX] 유효한 종목 없음. 루프 종료.")
        return

    connector = aiohttp.TCPConnector(limit=100, keepalive_timeout=15)
    timeout = aiohttp.ClientTimeout(total=2.0)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            chunks = chunked(allstock_df.itertuples(index=False), MAX_REQUESTS_PER_SECOND)
            for i, chunk in enumerate(chunks):
                tasks = [fetch_stock_data(session, row.Name, row.Code) for row in chunk]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        print("[main_loop] 비동기 작업 중 예외 발생:", res)
                await asyncio.sleep(1)
                print(f"chunk {i*10}~{i*10+9} ended")

# 진입점
if __name__ == "__main__":
    while True:
        try:
            producer = create_kafka_producer()
            asyncio.run(main_loop())
        except KeyboardInterrupt:
            print("종료 요청 수신됨")
            break
        except Exception as e:
            print("[__main__] 전체 루프 예외 발생. 5초 후 재시작:", e)
            time.sleep(5)
        finally:
            try:
                producer.flush()
                producer.close()
            except Exception:
                pass

