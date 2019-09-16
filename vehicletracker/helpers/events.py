import os
import logging
import socket
import threading

import time
import uuid

import pika
import json

from typing import Any, Callable

#REQUESTS_EXCHANGE_NAME = 'vehicletracker-requests'
EVENTS_EXCHANGE_NAME = 'vehicletracker-events'

_LOGGER = logging.getLogger(__name__)

class EventQueue():    

    def __init__(self, domain):
        self.domain_queue_name = domain + '-domain'
        self.node_queue_name = domain + '-node-' + socket.gethostname()

        credentials = pika.PlainCredentials(
            username = os.environ['RABBITMQ_USERNAME'],
            password = os.environ['RABBITMQ_PASSWORD'])

        self.parameters = pika.ConnectionParameters(
            host = os.getenv('RABBITMQ_ADDRESS', 'localhost'),
            port = os.getenv('RABBITMQ_PORT', 5672),
            virtual_host = os.getenv('RABBITMQ_VHOST', '/'),
            credentials = credentials)
        
        self.thread = None
        self.cancel_event = None        

        self.consume_connection = None
        self.consume_channel = None

        self.connection = None
        self.channel = None

        self.wait_events = {}
        self.results = {}
        self.listeners = {}
        self.listeners_all = {}

    def bind_internal(self, event_type, domain = True):
        if self.consume_channel:
            self.consume_channel.queue_bind(
                exchange = EVENTS_EXCHANGE_NAME,
                queue = self.domain_queue_name if domain else self.node_queue_name,
                routing_key = '#' if event_type == '*' else event_type)

    def consume_internal(self):

        while not self.cancel_event.is_set():
            try:
                _LOGGER.info('domain connecting to RabbitMQ ...')

                # Declare connections and channel
                self.consume_connection = pika.BlockingConnection(self.parameters)
                self.consume_channel = self.consume_connection.channel()

                # Declare exchange / queue for events
                self.consume_channel.exchange_declare(exchange = EVENTS_EXCHANGE_NAME, exchange_type = 'topic')
                self.consume_channel.queue_declare(queue = self.domain_queue_name, auto_delete = True)
                
                # Declare queue for service callbacks        
                self.consume_channel.queue_declare(queue = self.node_queue_name, exclusive = True)
        
                # Hook up consumers
                self.consume_channel.basic_consume(queue = self.domain_queue_name, on_message_callback = self.domain_event_internal, auto_ack = True)
                self.consume_channel.basic_consume(queue = self.node_queue_name, on_message_callback = self.node_event_internal, auto_ack = True)

                for event_type in self.listeners_all.keys():
                    self.bind_internal(event_type, False)

                for event_type in self.listeners.keys():
                    self.bind_internal(event_type, True)

                self.consume_channel.start_consuming()
            
            except pika.exceptions.ConnectionClosedByBroker:
                # Recovery from server-initiated connection closure, including
                # when the node is stopped cleanly
                _LOGGER.info("server-initiated connection closure, retrying...")
                continue
            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError:
                _LOGGER.exception("caught a channel error, stopping...")
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                if self.cancel_event.is_set():
                    break
                _LOGGER.exception("connection was closed, retrying...")
                continue

    def node_event_internal(self, ch, method, properties, body):
        # Test if this is a reply
        if properties.correlation_id in self.wait_events:	
            _LOGGER.debug(f"got reply (correlation_id: {properties.correlation_id})")	
            wait_event = self.wait_events.pop(properties.correlation_id, None)            
            self.results[properties.correlation_id] = (body, properties.content_type)
            wait_event.set()
            return
        else:
            self.handle_listeners_internal(
                body, properties.content_type, properties.reply_to, properties.correlation_id, self.listeners_all)

    def domain_event_internal(self, ch, method, properties, body):
        self.handle_listeners_internal(
            body, properties.content_type, properties.reply_to, properties.correlation_id, self.listeners)

    def ensure_connection(self): 
        if self.connection is None:
            _LOGGER.info('node connecting to RabbitMQ ...')
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()

    def publish_internal_with_retry(self, exchange, routing_key, body, content_type, reply_to = None, correlation_id = None, retry_count = 3):
        retry = 0
        while retry < retry_count:
            try:
                self.ensure_connection()
                self.channel.basic_publish(
                    exchange = exchange,
                    routing_key = routing_key,
                    body = body,
                    properties = pika.BasicProperties (
                        content_type = content_type,
                        reply_to = reply_to,
                        correlation_id = correlation_id
                    )
                )
                return
            except Exception:
                retry = retry + 1
                _LOGGER.warn(f"failed to publish_event (reply_to: {reply_to}, correlation_id: {correlation_id}, retry {retry})")
                self.connection = None

        _LOGGER.error(f"all attempts failed to publish_event (reply_to: {reply_to}, correlation_id: {correlation_id})")

    def publish_event(self, event, reply_to = None, correlation_id = None):
        self.publish_internal_with_retry(
            exchange = EVENTS_EXCHANGE_NAME,
            routing_key = event['eventType'],
            body = json.dumps(event),
            content_type = 'application/json',
            reply_to = reply_to,
            correlation_id = correlation_id
        )

    def publish_reply(self, data, to, correlation_id, content_type = None):
        if content_type is None:
            content_type = 'application/json'
        self.publish_internal_with_retry(
            exchange = '',
            routing_key = to,
            body = json.dumps(data) if content_type == 'application/json' else data,
            content_type = content_type,
            correlation_id = correlation_id
        )

    def handle_listeners_internal(self, body, content_type, reply_to, correlation_id, listeners):
        try:
            if content_type == 'application/json':
                event = json.loads(body)
                event_type = event['eventType']

                # TODO: This is not buitifull, but works... 
                target_list = listeners.get(event_type, [])
                target_list.extend(listeners.get('*', []))

                _LOGGER.debug(f"handling {event_type} with {len(target_list)} targets (reply_to: {reply_to}, correlation_id: {correlation_id})")

                for target in target_list:
                    # TODO: Wrap targets in exception logging and execute async.
                    try:
                        #task_runner.async_add_job(target, *args)
                        result = target(event)

                        if isinstance(result, tuple):
                            result_data, content_type = result
                        else:
                            result_data, content_type = result, None

                        if result_data and properties.reply_to:
                            self.publish_reply(
                                data = result_data,
                                content_type = content_type,
                                to = reply_to,
                                correlation_id = correlation_id)

                    except Exception as e:
                        _LOGGER.exception('error in executing listerner')
                        self.publish_reply(
                            data = {
                                'error': str(e)
                            },
                            to = reply_to,
                            correlation_id = correlation_id)
                        self.publish_event(
                            event = {
                                'eventType': 'error',
                                'error': str(e)
                            },
                            reply_to = reply_to,
                            correlation_id = correlation_id
                        )

        except Exception:
            _LOGGER.exception('Error in event_callback.')

    def listen_domain(self, event_type: str, target: Callable[..., Any]) -> Callable[[], None]:
        return self.listen(event_type, target, domain = True)

    def listen_node(self, event_type: str, target: Callable[..., Any]) -> Callable[[], None]:
        return self.listen(event_type, target, domain = False)

    def listen(self, event_type: str, target: Callable[..., Any], domain : bool = True) -> Callable[[], None]:
        _LOGGER.info(f"listening for {event_type} ({'domain' if domain else 'node'})")
        listeners = self.listeners if domain else self.listeners_all
        if event_type not in listeners:
            listeners[event_type] = []
            self.bind_internal(event_type, domain)

        def remove_listener() -> None:
            _LOGGER.info(f"removing listener for {event_type} ({'domain' if domain else 'node'})")
            try:
                listeners[event_type].remove(target)
            except (KeyError, ValueError):
                # KeyError is key target listener did not exist
                # ValueError if listener did not exist within event_type
                _LOGGER.warning("Unable to remove unknown target %s", target)

        self.listeners[event_type].append(target)

        return remove_listener

    def register_service(self, service_name : str, target : Callable[..., Any]) -> Callable[[], None]:
        return self.listen_domain(f'service.{service_name}', lambda x: target(x.get('serviceData')))

    def call_service(self, service_name, service_data, timeout = 300, parse_json = True):
        correlation_id = str(uuid.uuid4())
        _LOGGER.debug(f"call service '{service_name}' (correlation_id: {correlation_id}, timeout: {timeout}).")
        self.wait_events[correlation_id] = threading.Event()
        self.publish_event(
            event = {
                'eventType': f'service.{service_name}',
                'serviceData': service_data
            },
            reply_to = self.node_queue_name,
            correlation_id = correlation_id
        )
        try:
            if self.wait_events[correlation_id].wait(timeout = timeout):
                body, content_type = self.results.pop(correlation_id)
                if content_type == 'application/json' and parse_json:
                    return json.loads(body)
                else:
                    return body, content_type
            else:
                _LOGGER.warn(f"call to '{service_name}' timed out.")
                return { 'error': 'timeout' }
        except:
            _LOGGER.exception(f"call service '{service_name}' failed.")
            return { 'error': 'failed' }
        finally:
            self.wait_events.pop(correlation_id, None)

    def start(self):
        if self.thread:
            _LOGGER.warning(f"cannot start event processing, since it has already been started.")
            return

        _LOGGER.info("event processing is starting ...")
        self.cancel_event = threading.Event()
        self.thread = threading.Thread(target=self.consume_internal)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        _LOGGER.info("event processing is stopping ...")
        
        if self.cancel_event:
            self.cancel_event.set()

        if self.consume_connection:
            try:
                self.consume_channel.stop_consuming()
                self.consume_connection.close()
            except pika.exceptions.AMQPConnectionError:
                pass

        if self.connection:
            try:
                self.channel.stop_consuming()
                self.connection.close()
            except pika.exceptions.AMQPConnectionError:
                pass

        if self.thread:
            self.thread.join(timeout=5) 

        self.cancel_event = None
        self.thread = None
