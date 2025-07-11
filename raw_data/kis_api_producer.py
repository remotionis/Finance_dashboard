
# -*- coding: utf-8 -*-
import kis_auth as kis

import time
import requests
import json
import sys

import pandas as pd

from datetime import datetime, timezone, timedelta
from pandas import DataFrame
from kafka import KafkaProducer


# Kafka 설정
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # 필요 시 수정
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
KST = timezone(timedelta(hours=9))

# 수집시점 추가 후 Kafka 전송
def stockspurchase(data):
    now = datetime.now(tz=KST)
    date_dict = {"stock_dt":now.strftime("%Y-%m-%d"), "stock_time":now.strftime("%H:%M:%S")}        
    date_dict.update(data)
    purchase_data = {
        "timestamp": int(time.time()),
        "fields": date_dict
    }
    producer.send("stock_trade", purchase_data)
    producer.flush()

# 종목명 + 단축코드
def load_allstock_KRX():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, encoding='cp949', header=0)[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})
    stk_data['Code'] = stk_data['Code'].apply(lambda input: '0' * (6 - len(str(input))) + str(input))
    print("stock list loaded: ", len(stk_data))
    return stk_data


def connect():
    try:
        allstock_df = load_allstock_KRX()
        kis.auth()
        url = 'https://openapi.koreainvestment.com:9443'
        content_type = "application/json"
        trs = [
            # 주식현재가 시세
            {'tr_id': 'FHKST01010100', 'url': '/uapi/domestic-stock/v1/quotations/inquire-price'},
            # 호가/예상체결
            {'tr_id': 'FHKST01010200', 'url': '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'}
        ]

        while True:
            # row: Name, Code
            for index, row in allstock_df.iterrows():
                print(row['Code'], " | ", row['Name'])
                full_data = {"stock_name": row['Name'], "stock_code": row['Code']}
                params = {
                    "FID_COND_MRKT_DIV_CODE": "J", # 시장 분류 코드 	J : 주식/ETF/ETN, W: ELW
                    "FID_INPUT_ISCD": row['Code']            # 	종목번호 (6자리) ETN의 경우, Q로 시작 (EX. Q500001)
                }
                start = time.time()
                for tr in trs:
                    lp_start = time.time()
                    res = kis._url_fetch(tr['url'], ptr_id=tr['tr_id'], tr_cont=content_type, params=params)
                    mid = time.time()
                    body = res.getBody()
                    print(f"{body.rt_cd} | {body.msg_cd} | {body.msg1}")
                    try:  # 주식현재가 호가/예상체결
                        body.output1.update(body.output2)
                        full_data.update(body.output1)
                    except:  # 주식현재가 시세
                        full_data.update(body.output)
                    lp_end = time.time()
                    #time.sleep()
                    print(f"Time gap: {mid - lp_start} | {lp_end - mid}) => Loop total: {lp_end - lp_start}")
                insertst = time.time()
                stockspurchase(full_data)
                inserted = time.time()
                print(f"Insert delay: {inserted - insertst}")
                print(f"Total gap: {inserted - start}")
                    
    except Exception as e:
        print('Exception Raised!')
        print(e)
    finally:
        exit()

def main():
    try:
        connect()
    except Exception as e:
        print('Exception Raised!')
        print(e)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("KeyboardInterrupt Exception 발생!")
        sys.exit(-100)
    except Exception:
        print("Exception 발생!")
        sys.exit(-200)
