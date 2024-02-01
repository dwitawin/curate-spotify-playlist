'''
==========================================================
Milestone 3

Name: Dwita Alya Windani
Batch: FTDS-RMT-024

This program is used to upload spotify clean data to elasticsearch
==========================================================
'''

from elasticsearch import Elasticsearch
import pandas as pd

es = Elasticsearch("http://localhost:9200")

df = pd.read_csv('/Users/ita/github-classroom/FTDS-assignment-bay/p2-ftds024-rmt-m3-dwitawin/P2M3_dwita_data_clean.csv')

for i, r in df.iterrows():
    doc = r.to_json()
    res = es.index(index='data_clean', doc_type='doc', body=doc)
    print(res)
