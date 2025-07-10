import pandas as pd
import numpy as np
import boto3
from datetime import datetime, timedelta
import io

def load_data(target_date: str) -> pd.DataFrame:
    s3_client = boto3.client('s3')
    bucket_name = "de6-team7-bucket"  
    prefix = f"raw_btc/trade_dt={target_date}/"
    print(f"S3 경로에서 데이터를 불러옵니다: s3://{bucket_name}/{prefix}")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print(f"오류: 경로 '{prefix}'에 파일이 없습니다.")
            return pd.DataFrame()
    except Exception as e:
        print(f"S3 파일 목록을 가져오는 중 오류 발생: {e}")
        return pd.DataFrame()

    file_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    print(f"총 {len(file_keys)}개의 Parquet 파일을 발견했습니다.")

    df_list = []
    for key in file_keys:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
        buffer = io.BytesIO(s3_object['Body'].read())
        df_temp = pd.read_parquet(buffer)
        df_list.append(df_temp)

    if not df_list:
        print("데이터를 불러오지 못했습니다.")
        return pd.DataFrame()

    df_raw_combined = pd.concat(df_list, ignore_index=True)
    print(f"총 {len(df_raw_combined):,}개의 로우 데이터를 성공적으로 불러왔습니다.")
    
    return df_raw_combined

def data_processing(raw: pd.DataFrame):
    raw = raw.copy()
    raw['timestamp'] = pd.to_datetime(raw['timestamp_ms'], unit='ms')
    raw = raw.sort_values('timestamp')

    dfs = {root: raw[raw['stream_type'].str.startswith(root)].set_index(['timestamp', 'market'])
           for root in ['trade', 'orderbook', 'ticker', 'candle']}

    # --- trade: 1분 VWAP ---
    tr = dfs['trade']
    if not tr.empty:
        t = tr.reset_index()
        t['minute'] = t['timestamp'].dt.floor('min')
        vwap = (t.groupby(['market', 'minute'])
                  .agg({'trade_amount': 'sum', 'trade_volume': 'sum'})
                  .reset_index())
        vwap['vwap_1m'] = vwap['trade_amount'] / vwap['trade_volume']
        vwap = vwap[['market', 'minute', 'vwap_1m']]            
        tr = (t.merge(vwap, on=['market', 'minute'], how='left')
                .drop(columns='minute')
                .set_index(['timestamp', 'market']))
        dfs['trade'] = tr

    # --- orderbook: spread ---
    ob = dfs['orderbook']
    if not ob.empty:
        ob = ob.copy()
        ob['bid_ask_spread'] = ob['ask_price_1'] - ob['bid_price_1']
        dfs['orderbook'] = ob

    # --- ticker: 20‑분 MA ---
    tk = dfs['ticker']
    if not tk.empty:
        tk_r = tk.reset_index()
        ma = (tk_r.set_index('timestamp')
                .groupby('market')['close_price']
                .rolling('20min').mean()
                .reset_index()
                .rename(columns={'close_price': 'ma_20m'}))
        tk = tk_r.merge(ma, on=['market', 'timestamp'], how='left')
        dfs['ticker'] = tk.set_index(['timestamp', 'market'])

    return dfs


def ml_table(raw_df: pd.DataFrame) -> pd.DataFrame:
    dfs = data_processing(raw_df)

    # trade 집계 (OHLC + VWAP)
    tr = dfs['trade'].reset_index()
    minute_trade = (tr.groupby(['market', pd.Grouper(key='timestamp', freq='1min')])
                      .agg(open_price=('trade_price', 'first'),
                           high_price=('trade_price', 'max'),
                           low_price=('trade_price', 'min'),
                           close_price=('trade_price', 'last'),
                           cum_volume=('trade_volume', 'sum'),
                           cum_amount=('trade_amount', 'sum'),
                           vwap_1m=('vwap_1m', 'last'))
                      .reset_index())

    # orderbook 평균 spread
    ob = dfs['orderbook'].reset_index()
    minute_ob = (ob.groupby(['market', pd.Grouper(key='timestamp', freq='1min')])
                   .agg(spread_mean=('bid_ask_spread', 'mean'))
                   .reset_index())

    # ticker 
    # close_price(종가) : 해당 1분 동안 실제로 체결된 마지막 거래 가격
    # ticker_close(현재가) : 해당 1분 동안 확인된 마지막 현재가 정보
    tk = dfs['ticker']['close_price'].unstack().resample('1min').ffill().stack().to_frame('ticker_close').reset_index()

    df = minute_trade.merge(minute_ob, on=['market', 'timestamp'], how='left') \
                     .merge(tk, on=['market', 'timestamp'], how='left')

    # 20-period MA + 볼린저
    df = df.sort_values('timestamp')
    df['ma_20'] = df['close_price'].rolling(20, min_periods=1).mean()
    rolling_std = df['close_price'].rolling(20, min_periods=1).std()
    df['boll_upper'] = df['ma_20'] + 2 * rolling_std
    df['boll_lower'] = df['ma_20'] - 2 * rolling_std

    # 결측치 처리
    df['boll_upper'] = df['boll_upper'].fillna(df['ma_20'])
    df['boll_lower'] = df['boll_lower'].fillna(df['ma_20'])
    df['ticker_close'] = df['ticker_close'].fillna(df['close_price'])

    cols_fill = df.columns.drop('market')
    df[cols_fill] = df.groupby('market')[cols_fill].ffill()
    
    df.dropna(inplace=True)
    
    return df


# 테스트
df_raw_from_s3 = load_data('2025-07-07')

if not df_raw_from_s3.empty:
    ml_df = ml_table(df_raw_from_s3)
    print(ml_df.head())
    #ml_ready_df.info()
        