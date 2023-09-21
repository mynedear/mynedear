import pandas as pd
import requests

import datetime
import pytz
def convert_time(timestamp):

    timestamp_milliseconds = timestamp
    timestamp_seconds = timestamp_milliseconds / 1000

    utc_time = datetime.datetime.utcfromtimestamp(timestamp_seconds)
    thailand_timezone = pytz.timezone('Asia/Bangkok')
    thailand_time = utc_time.replace(tzinfo=pytz.utc).astimezone(thailand_timezone)
    human_readable_time = thailand_time.strftime('%Y-%m-%d %H:%M:%S %Z')

    return human_readable_time



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
                    {"terms": {"intent": ["ELEMENT_ACTIVATED","ELEMENT_COMPLETED","ELEMENT_TERMINATED","ELEMENT_ACTIVATING"]}},
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

    element_dict = {}
    element_incident_dict = {}

    for body in response_json['hits']['hits']:
        
        
        elementId = body['_source']['value']['elementId']
        intent = body['_source']['intent']
        # print(elementId+' : '+intent)

        if intent == 'ELEMENT_TERMINATED':
            intent = 'CANCELED'
        elif intent == 'ELEMENT_ACTIVATED':
            intent = 'Active'
        elif intent == 'ELEMENT_COMPLETED':
            intent = 'COMPLETED'
        elif intent == 'ELEMENT_ACTIVATING':
            intent = 'Activating'
        bpmnElementType = body['_source']['value']['bpmnElementType']
        sequence = body['_source']['sequence']
        end_date = convert_time(body['_source']['timestamp'])
        
        if intent == 'Active':
            element_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnElementType,
                'sequence': sequence,
                'endDate' : end_date,
            }
        elif intent == 'COMPLETED':

            element_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnElementType,
                'sequence': sequence,
                'endDate' : end_date,
            }
        elif intent == 'CANCELED':

            element_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnElementType,
                'sequence': sequence,
                'endDate' : end_date,
            }
        elif intent == 'Activating':

            element_dict[elementId] = {
                'elementId': elementId,
                'intent': intent,
                'bpmnElementType': bpmnElementType,
                'sequence': sequence,
                'endDate' : end_date,
            }
        
        
    result_list = list(element_dict.values())

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


    for element in result_list:
        elementId = element['elementId']
        # if any(item['elementId'] == elementId for item in result_incident_list):
        #     element['intent'] = 'FAILED'
        for item in result_incident_list:
            if item['elementId'] == elementId:
                # if element[elementId] == elementId:
                if element['intent'] == 'CREATED' and item['intent'] != 'RESOLVED':
                    element['intent'] = 'FAILED'


    return result_list


history = Instance_History(Process_Instance_Key=2251799813698910) 
# print(history)

import json
pretty_formatted_data = json.dumps(history, indent=4)

print(pretty_formatted_data)

# from fastapi import FastAPI
# import uvicorn
# app = FastAPI()
# from pydantic import BaseModel
# class InstanceHistory_class(BaseModel):

#     Processinstance: int
#     test: str

# @app.post("/InstanceHistory")
# async def root(payload:InstanceHistory_class):
#     history = Instance_History(Process_Instance_Key=payload.Processinstance)
#     print(payload.test)
#     return history
# if __name__ == "__main__":

#     uvicorn.run(app, host="0.0.0.0", port=8000)

