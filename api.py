import sys
import os
import logging
import logging.config

from datetime import datetime

import requests
import yaml

import atexit
from flask import Flask, jsonify, request, Response
from flask_cors import CORS

from vehicletracker.helpers.config import load_config
from vehicletracker.helpers.events import EventQueue

if not os.path.exists('./logs'):
    os.makedirs('./logs')

config = load_config()
logging.config.dictConfig(config['logging'])

app = Flask(__name__)
CORS(app)

event_queue = EventQueue(domain = 'api')
event_queue.start()
atexit.register(event_queue.stop)

@app.route('/trainer/jobs')
def list_trainer_jobs():
    result = event_queue.call_service(
        service_name = 'list_trainer_jobs',
        service_data = None,
        timeout = 1)
    return jsonify(result)

@app.route('/link/models')
def list_link_models():
    result = event_queue.call_service(
        service_name = 'link_models',
        service_data = None,
        timeout = 5)
    return jsonify(result)

@app.route('/link/travel_time')
def link_travel_time_special_days():
    result, content_type = event_queue.call_service('link_travel_time', {
        'linkRef': request.args.get('link_ref'),
        'fromTime': request.args.get('from_time'),
        'toTime': request.args.get('to_time')
    }, timeout = int(request.args.get('timeout', 30)), parse_json = False)
    return Response(result, mimetype = content_type)

@app.route('/link/travel_time/n_preceding_normal_days')
def link_travel_time_n_preceding_normal_days():
    result, content_type = event_queue.call_service('link_travel_time_n_preceding_normal_days', {
        'linkRef': request.args.get('link_ref'),
        'time': request.args.get('time'),
        'n': request.args.get('n')
    }, timeout = int(request.args.get('timeout', 30)), parse_json = False)
    return Response(result, mimetype = content_type)

@app.route('/link/travel_time/special_days')
def link_travel_time_special_days():
    result, content_type = event_queue.call_service('link_travel_time_special_days', {
        'linkRef': request.args.get('link_ref'),
        'fromTime': request.args.get('from_time'),
        'toTime': request.args.get('to_time')
    }, timeout = int(request.args.get('timeout', 30)), parse_json = False)
    return Response(result, mimetype = content_type)

@app.route('/link/predict')
def link_predict():
    result = event_queue.call_service(
        service_name = 'link_predict',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

@app.route('/link/train', methods=['POST'])
def link_train():
    result = event_queue.call_service(
        service_name = 'link_model_schedule_train',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
