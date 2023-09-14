# import pandas as pd
# import requests

# url = "http://localhost:9200/zeebe-record_process-instance*/_search"
# data = {
#     "size":10000
#     }

# response = requests.post(url , json=data)
# print(response.json())

# for body in response.json()['hits']['hits']:
#     if body['_source']['value']['processInstanceKey'] == 2251799813708934:
#         print(body)

import pandas as pd
import requests
import json  # เพิ่มโมดูล json เพื่อใช้ในการจัดรูปแบบ JSON

url = "http://localhost:9200/zeebe-record_process-instance*/_search"
data = {
    "size": 10000
}

response = requests.post(url, json=data)
response_json = response.json()  # บันทึก JSON ไว้ในตัวแปร

# ใช้ json.dumps() เพื่อจัดรูปแบบ JSON ให้อ่านง่าย
formatted_json = json.dumps(response_json, indent=4)

# print(formatted_json)

# for body in response_json['hits']['hits']:
#     if body['_source']['value']['processInstanceKey'] == 2251799813708934:
#         print(body)

for body in response_json['hits']['hits']:
    if body['_source']['value']['processInstanceKey'] == 2251799813708934:
        # print(body)
        # print(json.dumps(body, indent=4))
        element_id = body['_source']['value']['elementId']
        print(f'elementId: {element_id}')

