from flask import Flask, jsonify, request
from flask_restful import reqparse, abort, Api, Resource
from bson import json_util
import pymongo, json
import sys
import logging
from botocore.exceptions import ClientError
import boto3
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)
logging.basicConfig(filename='Customer.log', level=logging.DEBUG)

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('')

class Customer(Resource):

    def __init__(self,documentdb_client,database,collection,stream_name, kinesis_client):

        """
        :param documentdb_client: A DocumentDB client.
        :param database: DocumentDB database.
        :param collection: DocumentDB collection
        """
        self.documentdb_client = documentdb_client
        self.database = database
        self.collection = collection

        """
        :param kinesis_client: A Boto3 Kinesis client.
        :param stream_name: Kinesis stream name
        """
        self.kinesis_client = kinesis_client
        self.name = stream_name
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record_documentdb(self, data):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the collection.
        :param data: The data to put in the collection.
        """
        try:
            customer_object_id = self.collection.insert_one(data)
            logger.info("Put record in collection %s.", self.collection)
            return customer_object_id
        except Exception:
            logger.exception("Couldn't put record in collection %s.", self.collection)

    def put_record_kinesisstream(self, data, partition_key):
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

    def get(self, customer_email):

        if customer_email is None:
            abort(400, message="Customer email is empty")

        customer = Customer.col.find_one({"customer_email":customer_email})

        if customer is None:
            abort(404, message="Customer {} doesn't exist".format(customer_email))

        logger.info("Found customer in db: " + customer)

        # insert customer event to collection
        partition_key = json.loads(customer)["customer_id"]

        event_id = id = uuid.uuid1().int
        event_type = "ADD"

        # Getting the current date and time
        event_timestamp = datetime.now()

        event = {}
        event["event_id"] = event_id
        event["event_type"] = event_type
        event["event_timestamp"] = event_timestamp
        event["customer"] = customer

        self.put_record_kinesisstream(event, partition_key)

        return json.loads(json_util.dumps(customer))


    def put(self, customer_email):

        if customer_email is None:
            abort(400, message="Customer email is empty")

        customer = Customer.col.find_one({"customer_email":customer_email})

        if customer is None:
            abort(404, message="Customer {} doesn't exist".format(customer_email))

        print(customer)

        # Get posted data by the user
        postedData = request.get_json()
        if postedData is None:
            abort(400, message="Customer is empty")
        print(postedData)


        updateResult= Customer.col.update_one({"customer_email":customer_email}, {"$set":postedData})
        if updateResult.modified_count == 1:
            updated_customer = Customer.col.find_one({"customer_email": customer_email})

            if updated_customer is None:
                abort(404, message="Customer {} doesn't exist".format(customer_email))

            print(updated_customer)
        else:
            abort(400, message="DB error not able to update record")
        return json.loads(json_util.dumps(updated_customer))

    def delete(self, customer_email):
        if customer_email is None:
            abort(400, message="Customer email is empty")

        deleteResult = Customer.col.delete_one({"customer_email":customer_email})
        if deleteResult is not None:
            print("Customer deleted")

        return ""


class Register(Resource):
    def __init__(self,documentdb_client,database,collection,stream_name, kinesis_client):

        """
        :param documentdb_client: A DocumentDB client.
        :param database: DocumentDB database.
        :param collection: DocumentDB collection
        """
        self.documentdb_client = documentdb_client
        self.database = database
        self.collection = collection

        """
        :param kinesis_client: A Boto3 Kinesis client.
        :param stream_name: Kinesis stream name
        """
        self.kinesis_client = kinesis_client
        self.name = stream_name
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record_documentdb(self, data):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the collection.
        :param data: The data to put in the collection.
        """
        try:
            customer_object_id = self.collection.insert_one(data)
            logger.info("Put record in collection %s.", self.collection)
            return customer_object_id
        except Exception:
            logger.exception("Couldn't put record in collection %s.", self.collection)

    def put_record_kinesisstream(self, data, partition_key):
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

    def post(self):
        #Get posted data by the user
        postedData = request.get_json()
        if postedData is None:
            abort(400, message="Customer is empty")
        logger.info("Payload of customer registration: " + postedData)

        # insert customer document to collection
        customer_object_id = self.put_record_documentdb(postedData)
        if customer_object_id is not None:
            logger.info("Registered customer: " + customer_object_id.inserted_id)
        else:
            abort(400, message="DB error not able to insert record")

        # insert customer event to collection
        partition_key = postedData["customer_id"]
        self.put_record_kinesisstream(postedData,partition_key)

        return json.loads(json_util.dumps(postedData))

    def get(self):
        return json.loads(json_util.dumps(Customer.col.find()))




##
## Setup the Api resource routing here
##
api.add_resource(Customer, '/api/v1/customer/<customer_email>')
api.add_resource(Register, '/api/v1/customer/register')
if __name__ == '__main__':

    # DocumentDB configs
    documentdb_client = pymongo.MongoClient('mongodb://ecommprofile:password@ecomm-profile-documentdb-cluster.cluster-c7myxlzkr5l7.us-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    # collection name
    database = documentdb_client.ecomm_profile
    # document name
    collection = database.customer

    stream_name = "ecomm_profile_customer_stream"
    customerDocument = Register(documentdb_client, database, collection,stream_name, boto3.client('kinesis', region_name='us-west-2'))
    app.run()