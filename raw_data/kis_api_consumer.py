from kafka import KafkaConsumer
import json
import psycopg2
import time

# PostgreSQL 연결 정보
db_config = {
    'host': 'de6-team7-postgresql.cujzazvifwam.us-east-1.rds.amazonaws.com',
    'port': 5432,
    'dbname': 'dev',
    'user': 'stockuser',
    'password': 'stockpw',
    'connect_timeout': 10
}

# Kafka Consumer 생성 함수
def create_consumer():
    return KafkaConsumer(
        'stock_trade',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        group_id='stock_trade_group'
    )

# Kafka 메시지 소비 및 DB 저장 처리
def stockspurchase():
    insert_txt = """
    INSERT INTO raw_data.stock_raw_info 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    while True:
        consumer = create_consumer()
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()

            print("Consumer 및 DB 연결 완료. 메시지 대기 중...")
            for msg in consumer:
                try:
                    data = msg.value
                    pValue = data['fields']
                    ts = str(data['timestamp'])

                    antc_amount = str(int(pValue.get('antc_vol', 0)) * int(pValue.get('antc_cnpr', 0)))

                    cursor.execute(insert_txt, (
                        pValue.get('stock_dt'),
                        pValue.get('stock_time'),
                        ts,
                        pValue.get('stock_code'),
                        pValue.get('stock_name'),
                        pValue.get('stck_oprc'),
                        pValue.get('stck_prpr'),
                        pValue.get('stck_hgpr'),
                        pValue.get('stck_lwpr'),
                        pValue.get('stck_prpr'),
                        pValue.get('stck_sdpr'),
                        pValue.get('prdy_vrss'),
                        pValue.get('prdy_ctrt'),
                        pValue.get('antc_vol'),
                        antc_amount,
                        pValue.get('acml_vol'),
                        pValue.get('acml_tr_pbmn'),
                        pValue.get('bidp1'),
                        pValue.get('bidp_rsqn1'),
                        pValue.get('askp1'),
                        pValue.get('askp_rsqn1'),
                        pValue.get('total_bidp_rsqn'),
                        pValue.get('total_askp_rsqn'),
                        pValue.get('wghn_avrg_stck_prc')
                    ))

                    conn.commit()
                except Exception as msg_err:
                    print(f"Kafka 메시지 처리 중 오류 발생: {msg_err}")
        except Exception as outer_err:
            print(f"Kafka 또는 DB 연결 실패: {outer_err}")
        finally:
            try:
                if conn:
                    conn.close()
                    print("DB 연결 종료")
                consumer.close()
                print("Kafka Consumer 종료")
            except Exception as e:
                print(f"종료 중 오류: {e}")

        # 오류 후 재시도 지연
        time.sleep(5)

# 실행
if __name__ == "__main__":
    stockspurchase()

