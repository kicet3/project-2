import pandas as pd
import os 
import numpy as np
import json
from sklearn.preprocessing import LabelEncoder 

path = os.path.join(os.getcwd(), 'data')
member_path = os.path.join(path, 'members_v3.csv')

member_df = pd.read_csv(member_path)

print('파일 읽기 완료')
encoder = LabelEncoder()

member_df['msno'] = encoder.fit_transform(member_df['msno'])+1

mapping = dict(zip(encoder.classes_, encoder.transform(encoder.classes_)))

with open('mapping.json', 'w') as f:
    json.dump(mapping, f,default=int)
