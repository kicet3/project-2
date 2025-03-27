import pandas as pd
import os 
import numpy as np
import json
from sklearn.preprocessing import LabelEncoder 

data_path = os.path.join(os.getcwd(), 'data')
root_path = os.getcwd()
mapping_path = os.path.join(root_path, 'mapping.json')

with open(mapping_path, 'r') as labelData:
    mapping = json.load(labelData)
    
    # mapping 값 확인 (디버깅)

    for i in range(20, 27):
        print(f'{i}번 파일 처리 중...')
        user_log_path = os.path.join(data_path, f'user_logs_{i}.csv')
        
        # CSV 읽기, msno를 문자열로 로드
        user_log_df = pd.read_csv(user_log_path, 
                                  header=None, 
                                  names=['msno', 'date', 'num_25', 'num_50', 'num_75', 'num_985', 'num_100', 'num_unq', 'total_secs'],
                                  dtype={'msno': 'str', 'date': 'str', 'num_25': 'int32', 'num_50': 'int32', 
                                         'num_75': 'int32', 'num_985': 'int32', 'num_100': 'int32', 
                                         'num_unq': 'int32', 'total_secs': 'float32'})
        print(f'{i}번 파일 읽기 완료')
        
        # num_unq 컬럼 제거
        user_log_df = user_log_df.drop(columns=['num_unq'])
        print(f'{i}번 파일 num_unq 컬럼 제거 완료')
        
        # msno 매핑 및 int로 변환
        user_log_df['msno'] = user_log_df['msno'].map(mapping)
        print(f'{i}번 파일 인코딩 후 msno 타입:', user_log_df['msno'].dtype)
        
        # NaN 처리 및 int32로 변환
        if user_log_df['msno'].isna().any():
            print(f'{i}번 파일: 매핑되지 않은 msno 값 존재, NaN 수:', user_log_df['msno'].isna().sum())
            user_log_df.dropna(subset=['msno'], inplace=True)  # NaN 제거
        
        user_log_df['msno'] = user_log_df['msno'].astype('int32')
        print(f'{i}번 파일 인코딩 완료, msno 타입:', user_log_df['msno'].dtype)
        
        # 집계 함수 정의 (num_unq 제외)
        agg_funcs = {
            'date': ['min', 'max'],
            'num_25': 'sum',
            'num_50': 'sum',
            'num_75': 'sum',
            'num_985': 'sum',
            'num_100': 'sum',
            'total_secs': 'sum'
        }
        print(f'{i}번 파일 집계 중...')
        
        # groupby 및 집계
        result_df = user_log_df.groupby('msno').agg(agg_funcs)
        
        # use_date 추가
        use_date = user_log_df['msno'].value_counts().sort_index()
        result_df['use_date'] = use_date.reindex(result_df.index).astype('int32')
        print(f'{i}번 파일 집계 완료')
        
        # 컬럼 이름 정리
        print(f'{i}번 파일 컬럼 이름 정리 중...')
        result_df.columns = [
            'start_date' if col[1] == 'min' else 
            'end_date' if col[1] == 'max' else 
            col[0] if col[0] != 'use_date' else 'use_date'
            for col in result_df.columns
        ]
        result_df = result_df.reset_index()
        print(f'{i}번 파일 컬럼 이름 정리 완료')
        
        # 결과 저장
        print(f'{i}번 파일 저장 중...')
        result_df.to_csv(os.path.join(data_path, f'user_logs_encoded_{i}.csv'), index=False)
        print(f'{i}번 파일 저장 완료')
        print(f'{i}번 파일 처리 완료')