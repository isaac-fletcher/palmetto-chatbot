import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Update with appropriate values
FLOWALIAS=""
FLOWID=""
NODE="FlowInputNode"
OUTPUTNAME="document"

def lambda_handler(event, context):
    """
    This function orchestrates a Bedrock Flow with text input from an APIGateway endpoint.

    event - Event dict that contains parameters sent upon invocation.
    context - Context in which the function is called.
    """

    # Retrieve text from the request
    message = event.get("text") 

    client = boto3.client('bedrock-agent-runtime')

    # Take the message and do any processing on it needed
    # Probably best to do json to minimize data parsing

    # Invoke the flow
    flow = client.invoke_flow(
            flowAliasIdentifier=FLOWALIAS,
            flowIdentifier=FLOWID,
            inputs=[
                {
                    'content': {
                        'document': message
                    },
                    'nodeName': NODE,
                    'nodeOutputName': OUTPUTNAME
                },
            ],
    )

    # Response from the model
    flow_stream = flow['responseStream']
    flow_exception = None
    flow_output = ""

    for event in flow_stream:
        # Grab key from event and check if it's an exception
        key = next(iter(event.keys()))
        
        if "Exception" in key:
            exception = item
            break
        
        # If not an exception, check if it is the flow output
        if "flowOutputEvent" in key:
            output = event['flowOutputEvent']['content']['document']

    # Error handling for if the flow fails
    if flow_exception:
        logger.error("Flow failed to execute: %s", flow_exception)
        raise Exception(flow_exception)

    # This response will be sent back to Mattermost
    response = {
        "statusCode": 200,
        "body": {
            "response_type": "comment",
            "text": output
        }
    }

    # log response
    logger.info("Response: %s", response)
    return response

