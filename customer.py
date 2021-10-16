from flask import Flask, jsonify, request
from flask_restful import reqparse, abort, Api, Resource
from bson import json_util
import pymongo, json
import sys

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('')

class Customer(Resource):
    client = None
    db = None
    col = None

    def __init__(self):
        ##Create a MongoDB client, open a connection to Amazon DocumentDB as a replica set and specify the read preference as secondary preferred
        ##client = pymongo.MongoClient('mongodb://ecommprofile:<insertYourPassword>@ecommprofile.cluster-c7myxlzkr5l7.us-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
        Customer.client = pymongo.MongoClient('mongodb://localhost:27017')

        ##Specify the database to be used
        Customer.db = Customer.client.ecomm_profile

        ##Specify the collection to be used
        Customer.col = Customer.db.customer


    def get(self, customer_email):

        if customer_email is None:
            abort(400, message="Customer email is empty")

        customer = Customer.col.find_one({"customer_email":customer_email})

        if customer is None:
            abort(404, message="Customer {} doesn't exist".format(customer_email))

        print(customer)

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
    def __init__(self):
        ##Create a MongoDB client, open a connection to Amazon DocumentDB as a replica set and specify the read preference as secondary preferred
        ##client = pymongo.MongoClient('mongodb://ecommprofile:<insertYourPassword>@ecommprofile.cluster-c7myxlzkr5l7.us-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
        Customer.client = pymongo.MongoClient('mongodb://localhost:27017')

        ##Specify the database to be used
        Customer.db = Customer.client.ecomm_profile

        ##Specify the collection to be used
        Customer.col = Customer.db.customer

    def post(self):
        #Get posted data by the user
        postedData = request.get_json()
        if postedData is None:
            abort(400, message="Customer is empty")
        print(postedData)
        customer_object_id = Customer.col.insert_one(postedData)
        if customer_object_id is not None:
            print(customer_object_id.inserted_id)
        else:
            abort(400, message="DB error not able to insert record")
        return json.loads(json_util.dumps(postedData))

    def get(self):
        return json.loads(json_util.dumps(Customer.col.find()))


##
## Setup the Api resource routing here
##
api.add_resource(Customer, '/api/v1/customer/<customer_email>')
api.add_resource(Register, '/api/v1/customer/register')


if __name__ == '__main__':
    app.run(debug=True)