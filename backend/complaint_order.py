import json
import boto3
import uuid
import ast

import os
from google.cloud import pubsub_v1
from google.auth import jwt
from lambda_response_builder import *
from authentication import *

dyn_client = boto3.client('dynamodb')
PUB_SUB_TOPIC_NAME = ''
def getOrderFromDatabase(orderId):
    TABLE_NAME = 'Order'
    order = dyn_client.scan(TableName=TABLE_NAME,FilterExpression='orderId = :orderId',
        ExpressionAttributeValues={
            ':orderId': {'S': orderId}
        })

    return order

def validateComplaintOrderSlots(slots):
    if not slots['userName']:
        print("In empty username")
        return{
        'isValid':False,
        'violatedSlot':'userName'
        }

    userName = str(slots['userName']['value']['originalValue'])
    authentication_result = authenticateUser(userName)
    print(authentication_result)
    if(authentication_result['userFound']==True):
        if(authentication_result['user']['status']=='LoggedOut'):
            return {
                'isValid':False,
                'violatedSlot':'userName',
                'message': 'Please login to complaint.'
            }
    else:
        return {
                'isValid':False,
                'violatedSlot':'userName',
                'message': 'User not found. Please enter valid Username.'
            }


    if not slots['orderId']:
        print("Inside Empty OrderId")
        return {
        'isValid': False,
        'violatedSlot': 'orderId'
        }


    orderId = str(slots['orderId']['value']['originalValue'])
    order = getOrderFromDatabase(orderId)

    if(order['Count']==0):
        return {
            'isValid':False,
            'violatedSlot':'orderId',
            'message': 'Please enter valid order id.'
        }
    else:
        print("In order complaint")
        print(order['Items'])
        if(order['Items'][0]['userName']['S'] != userName):
            return {
            'isValid':False,
            'violatedSlot':'orderId',
            'message': 'Please enter odrer id of your order.'
        }

    return {'isValid':True}

def setupPubSubPublisher():
    service_account_info = {}

    audience = ""

    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    # The same for the publisher, except that the "audience" claim needs to be adjusted
    publisher_audience = ""
    credentials_pub = credentials.with_claims(audience=publisher_audience)

    publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
    return publisher

def getMessageForOrderComplaint(intent_request):
    TABLE_NAME = 'Order'
    intent = intent_request['sessionState']['intent']
    intent_name = intent['name']

    orderId = intent['slots']['orderId']['value']['originalValue']
    userName = intent['slots']['userName']['value']['originalValue']

    orders = dyn_client.scan(TableName=TABLE_NAME,FilterExpression='orderId = :orderId and userName = :uname',
    ExpressionAttributeValues={
        ':orderId': {'S': orderId},
        ':uname':{'S':userName}
    })
    print(orders)
    print(orderId)
    print(userName)
    restaurantId = ""

    if(orders['Count']>0):
            for order in orders['Items']:
                print(order)
                restaurantId = order['restaurantId']['S']
                restaurantOwner = order ['restaurantOwner']['S']
    restaurantId = 'vaishwipatel@gmail.com'
    message = {"orderId":orderId,"userName":userName,"restaurantId":restaurantId,"restaurantOwner":restaurantOwner}
    return message

def complaintOrder(intent_request):
    intent = intent_request['sessionState']['intent']
    intent_name = intent['name']
    slots = intent['slots']

    validation_result = validateComplaintOrderSlots(slots)

    if intent_request['invocationSource'] == 'DialogCodeHook':
        return DialogCodeHookResponse(intent_name,slots,validation_result)

    if intent_request['invocationSource'] == 'FulfillmentCodeHook':

        publisher = setupPubSubPublisher()
        message = getMessageForOrderComplaint(intent_request)
        restaurantOwner = message['restaurantOwner']

        topic_name = PUB_SUB_TOPIC_NAME

        message = json.dumps(message)

        print(message)

        message = bytes(message, 'utf-8')

        future = publisher.publish(topic_name,message, spam='eggs')
        future.result()

        siteId = os.getenv("siteId",default = "https://localhost:3000")
        siteId += "/chat?email"+restaurantOwner
        chatBotmessage = "Shortly restaurant owner will chat with you. \n You can wait for them by clicking this link: "+siteId
        return close(intent_name,"Fulfilled",chatBotmessage)

    return null

