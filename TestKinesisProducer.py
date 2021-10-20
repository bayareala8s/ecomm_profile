import datetime
import json
import random
import boto3
import time
import uuid
import random
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(filename='CustomerKinesisStream.log', level=logging.DEBUG)

class CustomerKinesisStream:
    """Encapsulates a Kinesis stream."""

    def __init__(self, stream_name, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = stream_name
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name,
                Data=json.dumps(data),
                PartitionKey=partition_key)
            logger.info("Put record in stream %s.", self.name)
        except ClientError:
            logger.exception("Couldn't put record in stream %s.", self.name)
            raise
        else:
            return response

if __name__ == '__main__':
    stream_name = "ecomm_profile_customer_stream"
    customerStream = CustomerKinesisStream(stream_name,boto3.client('kinesis',region_name='us-west-2'))
    f = open("customer.json", "r")
    data = f.read()
    logger.info("Sending JSON data:")
    logger.info(data)
    customerStream.put_record(data,"166530461675373702791268246655298776009")


###
# aws kinesis describe-stream-summary --stream-name ecomm_profile_customer_stream
# aws kinesis get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --stream-name ecomm_profile_customer_stream
# aws kinesis put-record --stream-name ecomm_profile_customer_stream --partition-key 20a9af5e313111eca432064f1ffdbad3 --data '{\"customer_id\": \"87136990505216409385591600002097770239\"}'
# sudo aws kinesis get-records --shard-iterator "AAAAAAAAAAF/MokcYtCeNRXd/a0jn0PcCp0tMluoT/uOZdgL6bmXlRfoMvxrBcpdgLdfjPky/lPHb94FjdtlMW3iLQvEs+H/A4CvxnnOx/nuseD7nHXWTNCZ24sFwqc5WXT0qou5FDJ61F3p5YdmyG8recQOxe9/hOv4e/1kuQ4clyHESa1cLH4PN0/zJ0qKz7G6ngegMyykrkWr/GWvpL+WoiJK6Cuz8UAm743NhNOTk8DWcryGhxFiSWHFPHXfvwZUuUltVC4=" --region "us-west-2"



