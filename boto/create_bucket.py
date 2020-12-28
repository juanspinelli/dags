import logging
import boto3
from botocore.exceptions import ClientError

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    endpoint_url='http://192.168.49.2:31566',
)

region = session.region_name

def create_bucket(bucket_name, s3_client=s3_client, region=region):
    try:
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
        print('Bucket creado exitosamente')
    except:
        print('El bucket ya existe')