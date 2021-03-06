from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

class Ping(Resource):
    def get(self):
        return {'Ping': 'ok'}

api.add_resource(Ping, '/')

if __name__ == '__main__':
    app.run()