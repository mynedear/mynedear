import pandas as pd
import requests
# import json

url = "http://localhost:9200/zeebe-record_process-instance*/_search"
data = {
    "size": 10000,
    "query": {
        "bool": {
            "filter":[
                {"terms": {"intent": ["ELEMENT_ACTIVATED","ELEMENT_COMPLETED","ELEMENT_TERMINATED"]}},
                {"match": {"value.processInstanceKey": 	2251799813694049}}  
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
sequence_List = []
element_dict = {}

for body in response_json['hits']['hits']:
    
    elementId = body['_source']['value']['elementId']
    intent = body['_source']['intent']
    bpmnElementType = body['_source']['value']['bpmnElementType']
    sequence = body['_source']['sequence']

    if intent == 'ELEMENT_ACTIVATED':
        element_dict[elementId] = {
            'elementId': elementId,
            'intent': intent,
            'bpmnElementType': bpmnElementType,
            'sequence': sequence
        }
    elif intent == 'ELEMENT_COMPLETED':

        element_dict[elementId] = {
            'elementId': elementId,
            'intent': intent,
            'bpmnElementType': bpmnElementType,
            'sequence': sequence
        }

    
result_list = list(element_dict.values())
df = pd.DataFrame(result_list)
# print(df)
# print(result_list)
df_sorted = df.sort_values(by='sequence', ascending=True)
print(df_sorted)

# df_sorted.loc[df_sorted['elementId'] == 'download', 'intent'] = 'FAIL'
# print(df_sorted)

data2 = {
    'elementId': ['download'],
    'intent': ['CREATED'],
    'bpmnElementType': ['bpmnchromedriver'],
    'sequence': [2251799813685251]
}

df2 = pd.DataFrame(data2)
print(df2)

df_sorted.loc[df_sorted['elementId'].isin(df2['elementId']), 'intent'] = 'Fail'
print(df_sorted)



