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
from more_itertools import chunked

# Kafka 설정
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 설정
KST = timezone(timedelta(hours=9))
MIN_FETCH_INTERVAL = timedelta(seconds=30)
MAX_REQUESTS_PER_SECOND = 10

recent_fetch_time = {}  # 종목별 마지막 수집 시각
sent_cache = {}         # Kafka 전송 중복 방지

TRS = [
    {'tr_id': 'FHKST01010100', 'url': '/uapi/domestic-stock/v1/quotations/inquire-price'},
    {'tr_id': 'FHKST01010200', 'url': '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'}
]

BASE_URL = "https://openapi.koreainvestment.com:9443"

# KRX 종목 목록 로딩
def load_allstock_KRX():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, encoding='cp949', header=0)[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})
    #stk_data['Code'] = stk_data['Code'].apply(lambda x: f"{int(x):06d}")
    print("KRX 종목 로딩 완료:", len(stk_data))
    return stk_data

# 중복 전송 방지
def is_duplicate(data):
    key = json.dumps(data, sort_keys=True)
    key_hash = hashlib.md5(key.encode()).hexdigest()
    if key_hash in sent_cache:
        return True
    sent_cache[key_hash] = True
    return False

# Kafka 전송
def send_to_kafka(data):
    now = datetime.now(tz=KST)
    enriched = {
        "timestamp": int(time.time()),
        "fields": {
            "stock_dt": now.strftime("%Y-%m-%d"),
            "stock_time": now.strftime("%H:%M:%S"),
            **data
        }
    }
    if not is_duplicate(enriched):
        producer.send("stock_trade", enriched)
        print(f"Kafka 전송 완료: {data.get('stock_code')}")

# 비동기 API 요청 (동기 구조 참조 기반)
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
    st = time.time()
    try:
        async with session.get(url, headers=headers, params=params) as response:
            print(f"code: {code}, Gap: {time.time() - st}")
            if response.status != 200:
                text = await response.text()
                print(f"[{code}] {tr_id} 오류 | status={response.status} | {text}")
                return {"rt_cd": "-1", "msg1": "HTTP Error"}
            return await response.json()
    except Exception as e:
        print(f"[{code}] {tr_id} 요청 실패:", str(e))
        return {"rt_cd": "-1", "msg1": str(e)}

# 종목 1개 데이터 수집 및 Kafka 전송
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
                print(f"요청 실패: {code} | {tr['tr_id']} | {res.get('msg1')}")
                continue

            output = res.get("output", {})
            output1 = res.get("output1", {})
            output2 = res.get("output2", {})

            if output1 and output2:
                output1.update(output2)
                full_data.update(output1)
            elif output:
                full_data.update(output)

        send_to_kafka(full_data)

    except Exception as e:
        print(f"오류 발생 [{code}]: {e}")

# 메인 루프 (슬롯화 + 병렬 실행)
async def main_loop():
    kis.auth()
    allstock_df = load_allstock_KRX()

    connector = aiohttp.TCPConnector(limit=100, keepalive_timeout=15)
    timeout = aiohttp.ClientTimeout(total=2.0)
    chunks = chunked(allstock_df.itertuples(index=False), MAX_REQUESTS_PER_SECOND)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            for chunk in chunks:
                tasks = [fetch_stock_data(session, row.Name, row.Code) for row in chunk]
                await asyncio.gather(*tasks)
                await asyncio.sleep(1)  # 초당 20개 요청 제한

# 진입점
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("종료 요청 수신됨")
    except Exception as e:
        print(str(e))

