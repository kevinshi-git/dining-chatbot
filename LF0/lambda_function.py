import boto3

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):

    # msg_from_user = event['messages'][0]

    # change this to the message that user submits on 
    # your website using the 'event' variable
    print(event)
    print(context)
    msg_from_user = event.get("messages")[0].get("unstructured").get("text")

    print(f"Message from frontend: {msg_from_user}")

    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='YGHOSS9EIB', # MODIFY HERE
            botAliasId='X9YI1IGMVV', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    print("response: ", response)
    msg_from_lex = response.get('messages', [])

    if msg_from_lex:
        
        print(f"Message from Chatbot: {msg_from_lex[0]['content']}")
        print(response)

        resp = {
            'statusCode': 200,
            'messages': [ {'type': "unstructured", 'unstructured': {'text': msg_from_lex[0]['content']}  } ]

        }
        

        # modify resp to send back the next question Lex would ask from the user
        
        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket
    else:
        print("i dun goofed")
        resp = {
            'statusCode': 200,
            'messages': [ {'type': "unstructured", 'unstructured': {'text': "something went wrong AHHHHHH!"}  } ]

        }
    return resp
