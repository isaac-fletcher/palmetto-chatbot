import json
import boto3

lambda_client = boto3.client('lambda')

# The question you want to ask
question = "How can I share storage with members of my team?"

# Create the event payload
event = {
    "text": question
}

# Invoke the Lambda function
# Replace 'conductor' with your actual Lambda function name
response = lambda_client.invoke(
    FunctionName='conductor',
    InvocationType='RequestResponse',
    Payload=json.dumps(event)
)

# Get the response
response_payload = json.loads(response['Payload'].read())
print(response_payload)
