import boto3
import json
import uuid

def create_inference_profile():
    # Create a Bedrock client
    bedrock = boto3.client('bedrock', region_name='us-east-1')
    # Generate a unique client request token
    request_token = str(uuid.uuid4())

    # Define the request body
    request_body = {
        "clientRequestToken": request_token,
        "description": "Claude 3.5 Sonnet inference profile for Bedrock Flows",
        "inferenceProfileName": "claude3-sonnet-profile",
        "modelSource": {
            "copyFrom": "arn:aws:bedrock:us-east-1:605134456935:inference-profile/us.anthropic.claude-3-5-sonnet-20241022-v2:0"
        },
        "tags": [
            {
                "key": "Model",
                "value": "Claude3-Sonnet"
            },
            {
                "key": "Environment",
                "value": "Production"
            }
        ]
    }

    try:
        # Make the API call to create the inference profile
        response = bedrock.create_inference_profile(**request_body)
        print(f"Successfully created inference profile: {response}")
        return response
    except Exception as e:
        print(f"Error creating inference profile: {str(e)}")
        raise

if __name__ == "__main__":
    create_inference_profile()
