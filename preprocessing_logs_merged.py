import pandas as pd
import os 
import numpy as np
import json


data_path = os.path.join(os.getcwd(), 'data')
user_logs_path_20 = os.path.join(data_path, 'user_logs_encoded_merged_20.csv')
user_logs_df_20 = pd.read_csv(user_logs_path_20)

for i in range(20, 27):
    print(f'{i}번 파일 처리 중...')
    user_logs_path = os.path.join(data_path, f'user_logs_encoded_merged_{i}.csv')
    user_logs_df = pd.read_csv(user_logs_path)
    user_logs_df_20 = pd.concat([user_logs_df_20, user_logs_df], ignore_index=True)
    print(f'{i}번 파일 병합 완료')
# msno로 그룹화하고 요구사항에 맞게 처리
result = user_logs_df_20.groupby('msno').agg({
    'start_date': 'min',    # start_date 중 가장 작은 값
    'end_date': 'max',      # end_date 중 가장 큰 값
    'num_25': 'sum',        # 나머지 컬럼은 합계
    'num_50': 'sum',
    'num_75': 'sum',
    'num_985': 'sum',
    'num_100': 'sum',
    'total_secs': 'sum',
    'use_date': 'sum'
}).reset_index()

result.to_csv(os.path.join(data_path, 'user_logs_encoded_merged_all.csv'), index=False)