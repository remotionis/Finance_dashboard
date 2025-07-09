from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL 연결 정보
db_config = {
    'host': 'de6-team7-postgresql.cujzazvifwam.us-east-1.rds.amazonaws.com',
    'port': 5432,
    'dbname': 'dev',
    'user': 'stockuser',
    'password': 'stockpw'
}

# 국내주식체결처리 출력라이브러리
def stockspurchase(data):
    print("============================================")
    pValue = data['fields']
    antc_amount = pValue['antc_vol'] * pValue['antc_cnpr']
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # DB에 저장
        cursor.execute("INSERT INTO stock_raw_info VALUES " + \
                           pValue['stock_dt'],
                           pValue['stock_time'],
                           data['timestamp'],
                           pValue['stock_code'],
                           pValue['stock_name'],
                           pValue['stck_oprc'],
                           pValue['stck_prpr'],
                           pValue['stck_hgpr'],
                           pValue['stck_lwpr'],
                           pValue['stck_prpr'],
                           pValue['stck_sdpr'],
                           pValue['prdy_vrss'],
                           pValue['prdy_ctrt'],
                           pValue['antc_vol'],
                           antc_amount,
                           pValue['acml_vol'],
                           pValue['acml_tr_pbmn'],
                           pValue['bidp1'],
                           pValue['bidp_rsqn1'],
                           pValue['askp1'],
                           pValue['askp_rsqn1'],
                           pValue['total_bidp_rsqn'],
                           pValue['total_askp_rsqn'],
                           pValue['wghn_avrg_stck_prc'],
                       )
        conn.commit()
        
    except KeyboardInterrupt:
        print("종료 신호 수신, 정리 중...")
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        consumer.close()


consumer = KafkaConsumer(
    'stock_trade',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=True,
    group_id='stock_trade_group'
)



for msg in consumer:
    stockspurchase(msg.value)
