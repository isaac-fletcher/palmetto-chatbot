import json
import boto3
import logging
import os

S3_BUCKET_NAME = ""
ROOT_PATH = "/tmp/python/"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    # gather dependencies
    os.system(f"pip install -t {ROOT_PATH} scrapy")
    
    s3 = boto3.client('s3')

    # upload all dependencies to bucket
    # can be further optimized to zip up files
    for path, subdirs, files in os.walk(ROOT_PATH):
        directory = path.replace(ROOT_PATH,"")
        for file in files:
            s3.upload_file(os.path.join(path, file), S3_BUCKET_NAME, "layer/python/"+directory+'/'+file)

    response = {
        'statusCode': 200,
        'body': json.dumps('Dependencies successfully gathered!')
    }

    logger.info("Response: %s", response)

    return response
