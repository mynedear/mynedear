import pandas as pd
import requests

url = "http://localhost:9200/zeebe-record_process-instance*/_search"
data = {
    "size":10000
    }

response = requests.post(url , json=data)
print(response.json())

for body in response.json()['hits']['hits']:
    if body['_source']['value']['processInstanceKey'] == 2251799813708934:
        print(body)

