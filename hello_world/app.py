import zipfile
import urllib.parse
from io import BytesIO
from mimetypes import guess_type
import boto3
import logging
import pandas as pd
from csv import reader
import os
# import time




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
            tempFilePath = '/mnt/'
            existing_columns = []
            z.extract(filename, tempFilePath)
            
            content_type = guess_type(filename, strict=False)[0]

            # csv_stream = pd.read_csv(tempFilePath + '/' + filename, sep=",", low_memory=False)
            # # print(csv_stream.columns)
            # duplicate_counter = 1
            # for i in csv_stream.columns:
            #     if (i in existing_columns):
            #         print('duplicate')
            #         existing_columns.append(i.lower().replace(' ', '_') + '_dup' + str(duplicate_counter))
            #         duplicate_counter = duplicate_counter + 1
            #         continue
            #     existing_columns.append(i.lower().replace(' ', '_'))

            # csv_stream.columns = existing_columns

            # for i in existing_columns:
            #     if (i.startswith('user')):
            #         csv_stream[i] = csv_stream[i].astype('str')
            

            # fileobj=z.open(filename)
            with open(tempFilePath , 'rb') as data:
                s3.upload_fileobj(
                    Fileobj=data,
                    Bucket= bucketTarget,
                    Key=filename,
                    ExtraArgs={'ContentType': content_type}
                )
            os.remove('/mnt/csv/' + filename)
            response = s3.get_object(Bucket=bucketTarget, Key=filename)

            # data = response['Body'].read().decode('utf-8')
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
