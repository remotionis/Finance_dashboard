from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL 연결 정보
db_config = {
    'host': 'de6-team7-postgresql.cujzazvifwam.us-east-1.rds.amazonaws.com',
    'port': 5432,
    'dbname': 'dev',
    'user': 'stockuser',
    'password': 'stockpw',
    'connect_timeout': 10
}


# 국내주식체결처리 출력라이브러리
def stockspurchase():
    # PostgreSQL 연결
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    for msg in consumer:
        try:
            data = msg.value
            pValue = data['fields']
            ts = str(data['timestamp'])
            antc_amount = str(int(pValue['antc_vol']) * int(pValue['antc_cnpr']))

            # DB에 저장
            cursor.execute("INSERT INTO raw_data.stock_raw_info VALUES ('" +
                "', '".join([
                    pValue['stock_dt'],
                    pValue['stock_time'],
                    ts,
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
                    pValue['wghn_avrg_stck_prc']
                    ]) + "')"
            )
            conn.commit()
        except Exception as e:
            print(e)
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

stockspurchase()

