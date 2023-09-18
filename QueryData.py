import pandas as pd
import requests
def Instance_History(Process_Instance_Key):

    url = "http://localhost:9200/zeebe-record_process-instance*/_search"
    data = {
        "size": 10000,
        "sort": [
                    {
                    "sequence" : {
                        "order": "asc"
                    }
                    }
                ],
        "query": {
            "bool": {
                "filter":[
                    {"terms": {"intent": ["ELEMENT_ACTIVATED","ELEMENT_COMPLETED","ELEMENT_TERMINATED"]}},
                    {"match": {"value.processInstanceKey": 	Process_Instance_Key}}  
                ]
            }
        }
    }
    url_incident = "http://localhost:9200/zeebe-record_incident_8.2.5*/_search"
    data_incident = {
        "size": 10000,
        "query": {
            "bool": {
                "filter":[
                    {"match": {"value.processInstanceKey": 	Process_Instance_Key}}  
                ]
            }
        }
    }

    response = requests.post(url, json=data)
    response_json = response.json()
    response_incident = requests.post(url_incident, json=data_incident)
    response_incident_json = response_incident.json()

    elementId_List = []
    intent_List = []
    bpmnElementType_List = []
    sequence_List = []
    element_dict = {}
    element_incident_dict = {}

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
        elif intent == 'ELEMENT_TERMINATED':

            element_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnElementType,
                'sequence': sequence
            }
        
        
    result_list = list(element_dict.values())
    df = pd.DataFrame(result_list)

    for body_incident in response_incident_json['hits']['hits']:
        
        elementId = body_incident['_source']['value']['elementId']
        intent = body_incident['_source']['intent']
        bpmnProcessId = body_incident['_source']['value']['bpmnProcessId']
        sequence = body_incident['_source']['sequence']

        element_incident_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnProcessId,
                'sequence': sequence
            }
    result_incident_list = list(element_incident_dict.values())
    df_incident = pd.DataFrame(result_incident_list)


    if 'elementId' in df_incident.columns:
        print("The 'elementId' column exists in df_incident.")
        df.loc[df['elementId'].isin(df_incident['elementId']), 'intent'] = 'Fail'
        
    else:
        print("The 'elementId' column does not exist in df_incident.")

    return df


x = Instance_History(Process_Instance_Key=2251799813698800) 
print(x)
