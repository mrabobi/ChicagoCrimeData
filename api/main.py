import json

import flask
from flask import jsonify, abort

import backend

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "Access denied"


@app.route('/api/getAllData', methods=['GET'])
def get_products():
    message = backend.getAllData()
    return jsonify(message)


@app.route('/api/year/<int:year>', methods=['GET'])
def get_product_by_id(year):
    message = backend.getDatByYear(year)
    if message:
        return jsonify(json.loads(message))
    return abort(404, description="Resource not found")


@app.route('/api/<string:category>/<string:value>', methods=['GET'])
def get_product(category, value):
    message = backend.getData(category, value)
    if message:
        return jsonify(json.loads(message))
    return abort(404, description="Resource not found")


app.run()
