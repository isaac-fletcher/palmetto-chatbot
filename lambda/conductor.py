import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FLOWALIAS=""
FLOWID=""
INPUTNODE=""

def lambda_handler(event, context):
    """
    This function orchestrates a Bedrock Flow with text input from an APIGateway endpoint.

    event - Event dict that contains parameters sent upon invocation.
    context - Context in which the function is called.
    """

    # retrieve text from the request
    message = event.get("text") 

    client = boto3.client('bedrock-agent-runtime')

    # take the message and do any processing on it needed
    # probably best to do json to minimize data parsing

    # invoke the flow
    flow = client.invoke_flow(
            enableTrace=True,
            flowAliasIdentifier=FLOWALIAS,
            flowIdentifier=FLOWID,
            inputs=[
                {
                    'content': {
                        'document': message
                    },
                    # can specify specific inputs/outputs
                    # nodeInputName / nodeOutputName
                    nodeName=INPUTNODE
                },
            ],
    )

    # response from the model
    output = flow['responseStream']['flowOutputEvent']['content']['document']

    # add error handling if the flow fails

    # this response will be sent back to Mattermost
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
