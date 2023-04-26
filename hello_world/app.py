import zipfile
import urllib.parse
from io import BytesIO
from mimetypes import guess_type
import boto3
import logging
import pandas as pd
from csv import reader
import time




logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3',region_name = 'ap-southeast-1')

def lambda_handler(event, context):

    bucketTarget = "billing-ula"

    logger.info('===========Started=============')
    bucket = event['Records'][0]['s3']['bucket']['name']
    # zip_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    zip_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    # logger.info(str(zip_key))
    logger.info(str(zip_key))
    logger.info('============Finish '+ bucket +' ============')

    try:
        # Get the zipfile from S3
        obj = s3.get_object(Bucket=bucket, Key=zip_key)
        z = zipfile.ZipFile(BytesIO(obj['Body'].read()))
        # Extract and upload each file in the zipfile
        for filename in z.namelist():
            content_type = guess_type(filename, strict=False)[0]
            fileobj=z.open(filename)
            # timestr = time.strftime("%Y%m%d-%H%M%S")
            s3.upload_fileobj(
                Fileobj=fileobj,
                Bucket= bucketTarget,
                Key=filename,
                ExtraArgs={'ContentType': content_type}
            ) 
        
            response = s3.get_object(Bucket=bucketTarget, Key=filename)
            data = response['Body'].read().decode('utf-8')

            # df = pd.read_csv(data, 
            #  # Read CSV file from response object
            # df=pd.DataFrame( list(reader(data)),

            #                  dtype={"user_Department": "string",
            #                         "user_Environment": "string",
            #                         "user_Owner": "string",
            #                         "user_Name": "string",
            #                         "user_Project": "string",
            #                         "user_name": "string",
            #                         "user_resource-name": "string",
            #                         "user_service": "string",
            #                         "user_team": "string",
            #                         "user_tribe": "string"})
            #  # Remove Prefix or Suffix from Column Names
            # df.columns = df.columns.str.replace('user_', '')
            # # Save modified data frame to S3
            # csv_buffer = BytesIO()
            # df.to_csv(csv_buffer, index=False)
            # s3.put_object(Bucket=bucket, Key='filename', Body=csv_buffer.getvalue())



    except Exception as e:
        print('Error getting object {zip_key} from bucket {bucket}.')
        raise e
