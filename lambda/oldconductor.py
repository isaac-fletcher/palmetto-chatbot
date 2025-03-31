import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FLOWALIAS="ZDHQGAEG6R" #M67TH4VVD2
FLOWID="8EEUFJG3LF"
NODE="FlowInputNode"
OUTPUTNAME="document"

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
        flowAliasIdentifier=FLOWALIAS,
        flowIdentifier=FLOWID,
        inputs=[
            {
                'content': {
                    'document': message
                },
                # can specify specific inputs/outputs
                # nodeInputName / nodeOutputName
                'nodeName': NODE,
                'nodeOutputName': OUTPUTNAME
            },
        ],
    )

    # response from the model
    flow_stream = flow['responseStream']
    flow_exception = None
    flow_output = ""

    for event in flow_stream:
        # Grab key from event and check if it's an exception
        key = next(iter(event.keys()))

        if "Exception" in key:
            exception = item
            break

        if "flowOutputEvent" in key:
            output = event['flowOutputEvent']['content']['document']

    # add error handling if the flow fails
    if flow_exception:
        logger.error("Flow failed to execute: %s", flow_exception)
        raise Exception(flow_exception)

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