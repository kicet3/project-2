import pandas as pd
import os 
import numpy as np
import json
from sklearn.preprocessing import LabelEncoder 

data_path = os.path.join(os.getcwd(), 'data')
root_path = os.getcwd()
members_path = os.path.join(data_path, 'members_v3.csv')

mapping_path = os.path.join(root_path, 'mapping.json')

labelData = open(mapping_path, 'r')

mapping = json.load(labelData)

members_df = pd.read_csv(members_path)

members_df['msno'] = members_df['msno'].map(mapping)

members_df['msno'] = members_df['msno'].astype('int32')

members_df.to_csv(os.path.join(data_path, 'members_encoded.csv'), index=False)


