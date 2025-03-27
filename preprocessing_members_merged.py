import pandas as pd
import os 
import numpy as np
import json


data_path = os.path.join(os.getcwd(), 'data')
root_path = os.getcwd()
members_path = os.path.join(data_path, 'members_encoded.csv')
members_df = pd.read_csv(members_path)
members_df['msno'] = members_df['msno'].astype('int32') 

for i in range(20, 27):
    print(f'{i}번 파일 처리 중...')
    
    temp_df = members_df.copy()
    user_log_path = os.path.join(data_path, f'user_logs_encoded_{i}.csv')
    user_log_df = pd.read_csv(user_log_path)
    print(f'{i}번 파일 읽기 완료')
   
    temp_df = temp_df.merge(user_log_df, on='msno', how='left')
    print(f'{i}번 파일 병합 완료')
    
    temp_df = temp_df.fillna(0)
    print(f'{i}번 파일 결측치 0 채우기 완료')
    
    temp_df.to_csv(os.path.join(data_path, f'user_logs_encoded_merged_{i}.csv'), index=False)
    print(f'{i}번 파일 저장 완료')
    print(f'{i}번 파일 처리 완료')