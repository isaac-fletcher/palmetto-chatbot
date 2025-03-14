import boto3
import json

def list_flows(max_results=10):
    """
    List all Bedrock Agent flows in the account

    Parameters:
    max_results (int): Maximum number of flows to return per page
    """
    bedrock_agent = boto3.client('bedrock-agent', region_name='us-east-1')

    try:
        # Initial request
        response = bedrock_agent.list_flows(
            maxResults=max_results
        )

        # Print flows from first page
        print("\nExisting Flows:")
        for flow in response.get('flowSummaries', []):
            print(f"\nFlow Name: {flow.get('name')}")
            print(f"Flow ID: {flow.get('flowId')}")
            print(f"Flow ARN: {flow.get('flowArn')}")
            print(f"Status: {flow.get('status')}")
            print(f"Created At: {flow.get('createdAt')}")

        # Handle pagination if there are more results
        while 'nextToken' in response:
            response = bedrock_agent.list_flows(
                maxResults=max_results,
                nextToken=response['nextToken']
            )

            # Print flows from subsequent pages
            for flow in response.get('flowSummaries', []):
                print(f"\nFlow Name: {flow.get('name')}")
                print(f"Flow ID: {flow.get('flowId')}")
                print(f"Flow ARN: {flow.get('flowArn')}")
                print(f"Status: {flow.get('status')}")
                print(f"Created At: {flow.get('createdAt')}")

        return response

    except bedrock_agent.exceptions.ValidationException as ve:
        print(f"\nValidation Error: {str(ve)}")
        raise
    except bedrock_agent.exceptions.AccessDeniedException as ae:
        print(f"\nAccess Denied: {str(ae)}")
        raise
    except bedrock_agent.exceptions.ThrottlingException as te:
        print(f"\nThrottling Error: {str(te)}")
        raise
    except Exception as e:
        print(f"\nUnexpected Error: {str(e)}")
        raise

def format_flow_details(flow):
    """
    Format flow details for better readability
    """
    return {
        "name": flow.get('name'),
        "flowId": flow.get('flowId'),
        "flowArn": flow.get('flowArn'),
        "status": flow.get('status'),
        "createdAt": flow.get('createdAt').strftime("%Y-%m-%d %H:%M:%S") if flow.get('createdAt') else None,
        "description": flow.get('description')
    }

if __name__ == "__main__":
    # List flows with default max_results of 10
    list_flows()

    # Optionally, list flows with different max_results
    # list_flows(max_results=5)
