import atexit
from flask import Flask, jsonify, request

from vehicletracker.azure.events import EventHubsQueue

app = Flask(__name__)
event_queue = EventHubsQueue(consumer_group = '$default')

@app.before_first_request
def startup():
    event_queue.start()
    atexit.register(event_queue.stop)

@app.route('/link.predict')
def link_predict():
    result = event_queue.call_service(
        service_name = 'link.predict',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

@app.route('/link.train')
def link_predict():
    result = event_queue.call_service(
        service_name = 'link.train',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('model', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
