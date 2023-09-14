import pandas as pd
import requests
# import json

url = "http://localhost:9200/zeebe-record_process-instance*/_search"
data = {
    "size": 10000
}

response = requests.post(url, json=data)
response_json = response.json()
# formatted_json = json.dumps(response_json, indent=4)

# print(formatted_json)

# for body in response_json['hits']['hits']:
#     if body['_source']['value']['processInstanceKey'] == 2251799813708934:
#         print(body)

elementId_List = []
intent_List = []
bpmnElementType_List = []
timestamp_List = []

for body in response_json['hits']['hits']:
    
    if body['_source']['value']['processInstanceKey'] == 2251799813708934:
        # print(body)
        # print(json.dumps(body, indent=4))
        elementId = body['_source']['value']['elementId']
        intent = body['_source']['intent']
        bpmnElementType = body['_source']['value']['bpmnElementType']
        timestamp = body['_source']['timestamp']
        # print(f'elementId: {elementId}')
        # print(f'intent: {intent}')
        # print(f'bpmnElementType: {bpmnElementType}')

        elementId_List.append(elementId)
        intent_List.append(intent)
        bpmnElementType_List.append(bpmnElementType)
        timestamp_List.append(timestamp)

data = {
    'elementId' : elementId_List,
    'intent' : intent_List,
    'bpmnElementType' : bpmnElementType_List,
    'timestamp' : timestamp_List
}

df = pd.DataFrame(data)

print(df)

