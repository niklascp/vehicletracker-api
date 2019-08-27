import atexit
from flask import Flask, jsonify, request

from vehicletracker.data.events import EventQueue

app = Flask(__name__)
event_queue = EventQueue(domain = 'api')

@app.before_first_request
def startup():
    event_queue.start()
    atexit.register(event_queue.stop)

@app.route('/trainer/jobs')
def list_trainer_jobs():
    result = event_queue.call_service(
        service_name = 'list_trainer_jobs',
        service_data = None)
    return jsonify(result)

@app.route('/link/models')
def list_link_models():
    result = event_queue.call_service(
        service_name = 'list_link_models',
        service_data = None)
    return jsonify(result)

@app.route('/link/predict')
def link_predict():
    result = event_queue.call_service(
        service_name = 'predict_link',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

@app.route('/link/train')
def link_train():
    result = event_queue.call_service(
        service_name = 'schedule_train_link_model',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
