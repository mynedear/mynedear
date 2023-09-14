import pandas as pd
import requests
# import json

url = "http://localhost:9200/zeebe-record_process-instance*/_search"
data = {
    "size": 10000,
    "query": {
        "bool": {
            "filter":[
                {"terms": {"intent": ["ELEMENT_ACTIVATED","ELEMENT_COMPLETED"]}},
                {"match": {"value.processInstanceKey": 2251799813721617}}  
            ]
        }
    }
}

response = requests.post(url, json=data)
response_json = response.json()
# print(response_json)
# formatted_json = json.dumps(response_json, indent=4)
# print(formatted_json)

elementId_List = []
intent_List = []
bpmnElementType_List = []
timestamp_List = []
element_dict = {}

for body in response_json['hits']['hits']:
    
    elementId = body['_source']['value']['elementId']
    intent = body['_source']['intent']
    bpmnElementType = body['_source']['value']['bpmnElementType']
    timestamp = body['_source']['timestamp']

    if intent == 'ELEMENT_ACTIVATED':
        element_dict[elementId] = {
            'elementId': elementId,
            'intent': intent,
            'bpmnElementType': bpmnElementType,
            'timestamp': timestamp
        }
    elif intent == 'ELEMENT_COMPLETED' and elementId in element_dict:
        element_dict[elementId]['intent'] = intent
        element_dict[elementId]['bpmnElementType'] = bpmnElementType
        element_dict[elementId]['timestamp'] = timestamp
    
result_list = list(element_dict.values())
df = pd.DataFrame(result_list)
print(df)
