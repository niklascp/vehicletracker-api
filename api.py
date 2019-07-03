import atexit
from flask import Flask, jsonify, request

from vehicletracker.azure.events import EventHubsQueue

app = Flask(__name__)
event_queue = EventHubsQueue(consumer_group = '$default')

@app.route('/link.predict')
def index():
    result = event_queue.call_service(
        service_name = 'link.predict',
        service_data = {
            'linkRef': request.args.get('link_ref'),
            'model': request.args.get('link_ref', default = 'svr'),
            'time': request.args.get('time')
    })
    return jsonify(result)

if __name__ == '__main__':
    event_queue.start()
    atexit.register(event_queue.stop)
    app.run(debug=True)
