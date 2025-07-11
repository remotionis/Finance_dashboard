
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 16:57:19 2023

@author: Administrator
"""
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

def send_to_kafka(topic, data_dict):
    producer.send(topic, value=data_dict)
    producer.flush()

# 수집시점 추가 후 Kafka 전송
def stockspurchase(data):
    now = datetime.now(tz=KST)
    date_dict = {"stock_dt":now.strftime("%Y-%m-%d"), "stock_time":now.strftime("%H:%M:%S")}        
    date_dict.update(data)
    purchase_data = {
        "timestamp": int(time.time()),
        "fields": date_dict
    }
    send_to_kafka("stock_trade", purchase_data)

# 종목명 + 단축코드
def load_allstock_KRX():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, encoding='cp949', header=0)[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})
    stk_data['Code'] = stk_data['Code'].apply(lambda input: '0' * (6 - len(str(input))) + str(input))
    return stk_data


#====|  [국내주식] 기본시세  |============================================================================================================================

##############################################################################################
# [국내주식] 기본시세 > 주식현재가 시세 or 주식현재가 호가/예상체결
##############################################################################################
def get_inquire(url, ptr_id, tr_cont, params, itm_no="", FK100="", NK100="", dataframe=None):
    res = kis._url_fetch(url, ptr_id, tr_cont, params)
    body = res.getBody()
    try:  # 주식현재가 호가/예상체결
        body.output1.update(body.output2)
        return body.output1
    except:  # 주식현재가 시세
        return res.getBody().output


##############################################################################################
# [국내주식] 업종/기타 > 국내휴장일조회
# 국내휴장일조회 API입니다.
# 영업일, 거래일, 개장일, 결제일 여부를 조회할 수 있습니다.
# 주문을 넣을 수 있는지 확인하고자 하실 경우 개장일여부(opnd_yn)을 사용하시면 됩니다.
##############################################################################################
##def get_quotations_ch_holiday(dt="", tr_cont="", FK100="", NK100="", dataframe=None):
##    url = '/uapi/domestic-stock/v1/quotations/chk-holiday'
##    tr_id = "CTCA0903R"  # 국내휴장일조회
##
##    params = {
##        "BASS_DT": dt, # 시장 분류 코드 	J : 주식/ETF/ETN, W: ELW
##        "CTX_AREA_FK": FK100,  # 공란 : 최초 조회시 이전 조회 Output CTX_AREA_FK100 값 : 다음페이지 조회시(2번째부터)
##        "CTX_AREA_NK": NK100  # 공란 : 최초 조회시 이전 조회 Output CTX_AREA_NK100 값 : 다음페이지 조회시(2번째부터)
##    }
##    res = kis._url_fetch(url, tr_id, tr_cont, params)
##
##    # print(res.getBody())  # 오류 원인 확인 필요시 사용
##    # Assuming 'output' is a dictionary that you want to convert to a DataFrame
##    current_data = pd.DataFrame(res.getBody().output)
##
##    dataframe = current_data
##
##    # 첫 번째 값만 선택하여 반환
##    first_value = current_data.iloc[0] if not current_data.empty else None
##
##    return first_value


def connect():
    try:
        allstock_df = load_allstock_KRX()
        kis.auth()
        url = 'https://openapi.koreainvestment.com:9443'
        content_type = "application/json"

        while True:
            # row: Name, Code
            for index, row in allstock_df.iterrows():
                full_data = {"stock_name": row['Name'], "stock_code": row['Code']}
                trs = [
                    # 주식현재가 시세
                    {'tr_id': 'FHKST01010100', 'url': '/uapi/domestic-stock/v1/quotations/inquire-price'},
                    {'tr_id': 'FHKST01010200', 'url': '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'}
                ]
                for tr in trs:
                    params = {
                        "FID_COND_MRKT_DIV_CODE": "J", # 시장 분류 코드 	J : 주식/ETF/ETN, W: ELW
                        "FID_INPUT_ISCD": row['Code']            # 	종목번호 (6자리) ETN의 경우, Q로 시작 (EX. Q500001)
                    }
                    full_data.update(get_inquire(tr['url'], ptr_id=tr['tr_id'], tr_cont=content_type, params=params))
                stockspurchase(full_data)
                time.sleep(0.05)
                    
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
