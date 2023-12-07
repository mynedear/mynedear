import requests
import grpc
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc
import json

# def FindelementInstanceKey(Process_InstanceKey):

#     url = "http://localhost:9200/zeebe-record_process-instance_*/_search"
#     data = {
#         "size": 1000,
#         "sort": [
#             {
#                 "sequence" : {
#                     "order" : "asc"
#                 }
#             }
#         ],
#         "query": {
#             "bool": {
#             "filter": [
#                 {"terms": {"intent": ["ELEMENT_ACTIVATING","ELEMENT_COMPLETED"]}},
#                 {"match": {"value.processInstanceKey": Process_InstanceKey}}
#                 # {"match": {"value.processInstanceKey": 2251799813708427}}
#                     ]
#             }
#         }
#     }

#     response = requests.post(url,json=data)
#     response_json = response.json()
#     elementInstanceKey_dict = {}

#     # print(response_json)

#     for value in response_json['hits']['hits']:
#         key = value['_source']['key']
#         intent = value['_source']['intent']
#         bpmnElementType = value['_source']['value']['bpmnElementType']

#         elementInstanceKey_dict = {
#             'elementInstanceKey' : key,
#             'intent' : intent,
#             'bpmnElementType' : bpmnElementType,
#         }
#         print(elementInstanceKey_dict)
#         # print(type(elementInstanceKey_dict))

#     # last_value = list(elementInstanceKey_dict)[-1]
#     # # print(last_value)
#     # print(elementInstanceKey_dict[last_value])

#     # last_value = list(elementInstanceKey_dict.items())

#         # if elementInstanceKey_dict:
#         #     lastest_value = elementInstanceKey_dict[-1]
#         #     print(lastest_value)

#         # item = list(elementInstanceKey_dict.items())
#         # if item:
#         #     lasest_key, lastest_data = item[-1]
#         #     print("Key:", lasest_key)
#         #     print("intent:", lastest_data)


#         # last = list(elementInstanceKey_dict.values())[-1]
#         # print(last)

#     # last_index = len(elementInstanceKey_dict) - 1
#     # for key, value in elementInstanceKey_dict[last_index].items():
#     #     print(f"{key}: {value}")
#             # print(key) #ตอนนี้กำลังทำให้แสดงค่าล่าสุด หลังจากนั้นจะทำif elseเชคว่าเป้นcompletedหรือเปล่า
#         #เชค active completedล่าสุดมาดูว่าคีย์ตรงกันมั้ย ถ้าไม่ใช่ให้ใช้คีย์ของactive ถ้าตรงกันก็บอกว่าแก้ไขตัวแปรไม่ได้

#         # # print(lastest_intent)
#         # for key in lastest_intent.items():
#         #     print(lastest_intent) #ไม่ได้ตามที่ต้องการ ผลลัพธ์ยังแปลกๆอยู่ 

#         # print(key)
#         # print(intent)

#         #ลองให้แสดงค่า active ล่าสุด คิดว่าแค่นี้คงพอเพราะต้องเป็นactive เท่านั้นถึงจะตั้งค่าตัวแปรได้
#         # if intent == 'ELEMENT_ACTIVATED':  #เหมือนจะต้องเปลี่ยนมาเชคคำว่า activatingแทนเพราะกรณี fail ล่าสุดคือ activating 
#         #     latest_intent = elementInstanceKey_dict
#         # print(intent)
#         # if intent == 'ELEMENT_ACTIVATING':   
#         #     latest_intent = elementInstanceKey_dict
#         #     # print(intent)

#     # if latest_intent:
#     #     # print(latest_intent)
#     #     # print(latest_intent['elementInstanceKey'])
#     #     # print(latest_intent)
#     #     elementKey = latest_intent['elementInstanceKey']
#     #     # print(elementKey)
#     # return elementKey

# test_list = [
#     'a','b','c'
# ]

# # print(len(test_list))

# activeelementKey = FindelementInstanceKey(Process_InstanceKey=2251799813708463)

#ทำฟังก์ชั่นที่เชคว่ากล่องไหนactiveจริงๆ 

def Set_Variable(ProcessInstanceKey,edit_variable):
    with grpc.insecure_channel("localhost:26500") as channel:
        # print('hello')
        stub = gateway_pb2_grpc.GatewayStub(channel)
        # print('2')
        
        grpc_start = stub.SetVariables(
            
            gateway_pb2.SetVariablesRequest(
                # elementInstanceKey = elementKey,
                elementInstanceKey = ProcessInstanceKey,
                # variables = json.dumps({"testcomplete":"success","name":"change"})
                variables = json.dumps(edit_variable),
                # local = False
            )
        )
        # print(grpc_start)
        return grpc_start

# Set_Variable(ProcessInstanceKey=2251799813708463,edit_variable = {"addvar":"testvar2"})
#ทำfast API

from fastapi import FastAPI
import uvicorn
app = FastAPI()
from pydantic import BaseModel
class SetVariable_class(BaseModel):

    Processinstance: int
    edit_variable: dict

@app.post("/setVariable")
async def root(payload:SetVariable_class):
    print(payload)

    SetVar = Set_Variable(ProcessInstanceKey=payload.Processinstance,edit_variable=payload.edit_variable)
    print(SetVar)
    return str(SetVar)
# @app.get("/")
# async def root():
#     # SetVar = Set_Variable(Process_Instance_Key=payload.Processinstance,)
#     print("Hello mind")
#     return "this is me"
if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
