import json
import boto3
import random

def lambda_handler(event, context):
    # TODO implement
    print(event)
    possible_intents = event.get("interpretations")
   
    for intent_json in possible_intents:
        intent=intent_json.get("intent")
        intent_name=intent.get("name")
        if intent_name=="DiningSuggestionsIntent":
            break
    dinning_data=intent.get("slots")

    parameters={"location":None,"cuisine":None,"people":None,'date': None, 'email': None, 'time':None}
    session_attributes=event.get("sessionState").get("sessionAttributes")
    if len(session_attributes)==0:
        session_attributes=parameters
    for key in session_attributes:
        key_data=dinning_data.get(key)
        if session_attributes[key] is None and key_data is not None:
            session_attributes[key]=key_data.get("value").get("interpretedValue")
            session_attributes[key]=session_attributes[key]
    
    #check if users cuisine type in our database, if not, needs to ask again
    cuisine_types=['mediterranean', 'italian', 'indian', 'chinese', 'mexican', 'french']
    if session_attributes["cuisine"] and session_attributes["cuisine"].lower().strip() not in cuisine_types:
        session_attributes["cuisine"]=None

    if session_attributes["people"] and int(session_attributes["people"])<1: #check if there is at least 1 person
        session_attributes["people"]=None
        


    #{'name': 'DiningSuggestionsIntent', 'slots': {'date': None, 'email': None, 'cuisine': None, 'location': None, 'people': None}, 'state': 'ReadyForFulfillment', 'confirmationState': 'None'}
    print(session_attributes)
    if session_attributes["location"] is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "location"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    
    elif session_attributes["cuisine"]  is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "cuisine"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    elif session_attributes["people"]  is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "people"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    elif session_attributes["date"]  is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "date"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    elif session_attributes["time"]  is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "time"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    elif session_attributes["email"]  is None:
        return {
            "sessionState" : {
                "sessionAttributes":session_attributes,
                "dialogAction" : {
                    "type" : "ElicitSlot",
                    "slotToElicit" : "email"
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "confirmationState": "None",
                    "slots" : dinning_data,
                    'state': 'InProgress'
                },
                
            }
        }
    else:

        sqs = boto3.client('sqs')
        queue_url = "https://sqs.us-east-1.amazonaws.com/386726448370/Q1"
        #parameters={"location":None,"cuisine":None,"people":None,'date': None, 'email': None, 'time':None}

        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=10,
            MessageAttributes={
                'Location': {
                    'DataType': 'String',
                    'StringValue': session_attributes["location"]
                },
                'Cuisine': {
                    'DataType': 'String',
                    'StringValue': session_attributes["cuisine"]
                },
                'People': {
                    'DataType': 'Number',
                    'StringValue': "{}".format(session_attributes["people"])
                },
                'Time': {
                    'DataType': 'String',
                    'StringValue': session_attributes["time"]
                },
                'Date': {
                    'DataType': 'String',
                    'StringValue': session_attributes["date"]
                },
                'Email': {
                    'DataType': 'String',
                    'StringValue': "{}".format( session_attributes["email"])
                }
            },
            MessageBody=(
                'Parameters from user'
            )
        )


        return {
            "sessionState" : {
                "sessionAttributes":parameters,
                "dialogAction" : {
                    "type": "Close",
                },
                "intent" : {
                    "name" : "DiningSuggestionsIntent",
                    "state": "Fulfilled",
                },
                "messages" : {
                    "contentType": "PlainText",
                    "content" : "Thank you! I'll email you my recommendatinos shortly"
                }
            }
        }

    



   
    