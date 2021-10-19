import datetime
import json
import random
import boto3
import time
import uuid

STREAM_NAME = "ecomm_profile_customer_stream"

def generate(stream_name, kinesis_client):
    customer_id = uuid.uuid1().hex
    while True:
        data = "{\"customer_id\":" + customer_id + "\"}"
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=customer_id)
        time.sleep(2)


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis',region_name='us-west-2'))