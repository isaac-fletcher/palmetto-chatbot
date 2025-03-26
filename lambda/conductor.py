import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Flow configurations for category determination
CATEGORY_FLOW_ALIAS = "ZDHQGAEG6R"
CATEGORY_FLOW_ID = "8EEUFJG3LF"

# Flow configurations for specific categories
FLOW_CONFIGS = {
    "PALMETTO_HARDWARE": {
        "alias": "1HFN0HTY08",
        "id": "EIXHO1T17X"
    },
    "EXCEEDING_STORAGE": {
        "alias": "storage_flow_alias",
        "id": "EBERAJQZHB"
    },
    "CONVERTING_JOBS": {
        "alias": "converting_jobs_alias",
        "id": "D1ZFVVUCJU"
    },
    "RESOURCE_USAGE": {
        "alias": "resource_usage_alias",
        "id": "H2DOIK6ECY"
    },
    "DATA_FILE_TRANSFER": {
        "alias": "transfer_flow_alias",
        "id": "BGF67MLU9Q"
    },
    "PACKAGES": {
        "alias": "packages_flow_alias",
        "id": "KY7ARA4NUN"
    },
    "MONITORING": {
        "alias": "monitoring_flow_alias",
        "id": "XP6ZO3TKQI"
    },
    "JOB_RUN_WALL_TIME": {
        "alias": "wall_time_flow_alias",
        "id": "6TNTPGPXTH"
    },
    "DATA_BACKUP": {
        "alias": "backup_flow_alias",
        "id": "PK1SQHG343"
    },
    "FILE_STORAGE": {
        "alias": "file_storage_alias",
        "id": "RP66HM3P1E"
    },
    "DATA_SHARING": {
        "alias": "Q2O6UYKPMU",
        "id": "JVD6Y8S4FA"
    },
    "CONNECTION_REQUESTS": {
        "alias": "G05DMBM3U0",
        "id": "STV2J4BOW0"
    }
}

NODE = "FlowInputNode"
OUTPUTNAME = "document"

def invoke_flow(client, flow_id, flow_alias, message):
    """Helper function to invoke a flow and process its response"""
    try:
        logger.info(f"Invoking flow {flow_id} with alias {flow_alias}")
        flow = client.invoke_flow(
            flowAliasIdentifier=flow_alias,
            flowIdentifier=flow_id,
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

        # Process the flow response
        flow_output = ""
        for event in flow['responseStream']:
            key = next(iter(event.keys()))

            if "Exception" in key:
                raise Exception(event[key])

            if "flowOutputEvent" in key:
                flow_output += event['flowOutputEvent']['content']['document']

        logger.info(f"Flow {flow_id} completed successfully")
        return flow_output.strip()

    except Exception as e:
        logger.error(f"Flow {flow_id} failed to execute: {str(e)}")
        raise e

def lambda_handler(event, context):
    """
    This function determines the category of the question and routes it to the appropriate flow.
    Supported categories: PALMETTO_HARDWARE, EXCEEDING_STORAGE, DATA_FILE_TRANSFER, PACKAGES
    """

    # retrieve text from the request
    original_message = event.get("text")
    if not original_message:
        logger.error("No text provided in the request")
        # statusCode: 400,
        return {
            "response_type": "comment",
            "text": "No text provided in the request"
        }

    client = boto3.client('bedrock-agent-runtime')

    try:
        # First, determine the category
        logger.info("Determining question category")
        category = invoke_flow(
            client,
            CATEGORY_FLOW_ID,
            CATEGORY_FLOW_ALIAS,
            original_message
        )
        category = category.strip()
        logger.info(f"Category determined: {category}")

        # Check if we handle this category
        if category in FLOW_CONFIGS:
            logger.info(f"Routing question to {category} flow")
            flow_config = FLOW_CONFIGS[category]
            final_response = invoke_flow(
                client,
                flow_config["id"],
                flow_config["alias"],
                original_message  # Send the original question
            )
        else:
            logger.info(f"Unhandled category: {category}")
            final_response = (f"This question is categorized as: {category}. "
                            "Currently, I can only handle questions about hardware, "
                            "storage, file transfers, and packages.")

        # Return the response
        # statusCode: 200,
        response = {
            "response_type": "comment",
            "text": final_response,
            "category": category,
            "handled": category in FLOW_CONFIGS
        }

        logger.info(f"Returning response for category {category}")
        return response

    except Exception as e:
        error_message = f"Error processing request: {str(e)}"
        logger.error(error_message)
        # statusCode: 500
        return {
            "response_type": "comment",
            "text": error_message
        }

