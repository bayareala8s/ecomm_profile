import datetime
import json
import random
import boto3
import time
import uuid

STREAM_NAME = "ecomm_profile_customer_stream"


def get_data():

    customerid = uuid.uuid4()

    return {
        "_id": {
            "$oid": "6165d525ac6b9a27e0dd1842"
        },
        'event_time': datetime.datetime.now().isoformat(),
        "customer_id": customerid.hex,
        "customer_fname": "testuser_fname",
        "customer_lname": "testuser_lname",
        "customer_email": "testuser_email@bayareala8s.com",
        "customer_password": "testuser_password",
        "customer_street": "testuser_street",
        "customer_city": "testuser_city",
        "customer_state": "testuser_state",
        "customer_zip": 11111
    }

def generate(stream_name, kinesis_client):
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=customerid.hex)
        time.sleep(2)


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis',region_name='us-west-2'))