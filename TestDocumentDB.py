from bson import json_util
import pymongo, json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='TestDocumentDB.log', level=logging.DEBUG)

class TestDocument:
    """Encapsulates a DocumentDB """

    def __init__(self,documentdb_client,database,collection):
        """
        :param documentdb_client: A DocumentDB client.
        :param database: DocumentDB database.
        :param collection: DocumentDB collection
        """
        self.documentdb_client = documentdb_client
        self.database = database
        self.collection = collection

    def put_record(self, data):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the collection.
        :param data: The data to put in the collection.
        """
        try:
            customer_object_id = self.collection.insert_one(data)
            logger.info("Put record in collection %s.", self.collection)
        except Exception:
            logger.exception("Couldn't put record in collection %s.", self.collection)

if __name__ == '__main__':

    documentdb_client = pymongo.MongoClient('mongodb://ecommprofile:password@ecomm-profile-documentdb-cluster.cluster-c7myxlzkr5l7.us-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    database =  documentdb_client.ecomm_profile
    collection = database.customer

    customerDocument = TestDocument(documentdb_client,database,collection)

    f = open("customerDocument.json", "r")
    data = f.read()
    logger.info("Sending JSON data:")
    logger.info(data)
    customerDocument.put_record(json.loads(data))


# mongo --ssl --host ecomm-profile-documentdb-cluster.cluster-c7myxlzkr5l7.us-west-2.docdb.amazonaws.com:27017 --sslCAFile rds-combined-ca-bundle.pem --username ecommprofile --password password

# echo -e "[mongodb-org-3.6] \nname=MongoDB Repository\nbaseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/3.6/x86_64/\ngpgcheck=1 \nenabled=1 \ngpgkey=https://www.mongodb.org/static/pgp/server-3.6.asc" | sudo tee /etc/yum.repos.d/mongodb-org-3.6.repo

# sudo yum install -y mongodb-org-shell

# use ecomm_profile
# show dbs
# db.createCollection("customer")
