"""Message queue components module"""
import threading
import pika

class Connector(object):
    """Connector to RMQ broker (Blocking)"""
    def __init__(self, settings):
        self.settings = settings

    def connect(self):
        """Creates a connection to message queue broker from settings"""
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.settings.host,
                credentials=pika.credentials.PlainCredentials(
                    username=self.settings.credentials.username,
                    password=self.settings.credentials.password)))
        return conn

class GenericPublisher(threading.Thread):
    """Generic threaded publisher"""
    def __init__(self, exchange, connector, reconnect=True):
        super(GenericPublisher, self).__init__()

        self._exchange = exchange
        self._connector = connector
        self._reconnect = reconnect
        self._running = True
        self._channel = None

    def _connect(self):
        """Connects to the message broker"""
        connection = self._connector.connect()
        self._channel = connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange,
            type='fanout')

    def running(self):
        """Indicates whether the publisher should be running.
        May be overriden.
        """
        return self._running

    def stop(self):
        """Stops the publisher.
        May be overriden.
        """
        self._running = False

    def work(self):
        """Publisher implementation
        Must be implemented by user.
        """
        raise RuntimeError("GenericPublisher 'work' must be implemented")

    def publish(self, data):
        """Publishes data to the exchange"""
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key='',
            body=data)

    def run(self):
        while self.running():
            try:
                self._connect()
                self.work()
            except pika.exceptions.ConnectionClosed:
                if not self._reconnect:
                    raise

class GenericSubscriber(threading.Thread):
    """Generic threaded subscriber"""
    def __init__(self, exchange, connector, reconnect=True):
        super(GenericSubscriber, self).__init__()
        self._exchange = exchange
        self._connector = connector
        self._running = True
        self._reconnect = reconnect
        self._channel = None

    def _connect(self):
        connection = self._connector.connect()
        self._channel = connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange,
            type='fanout')

    def running(self):
        """Indicates whether the publisher should be running.
        May be overriden.
        """
        return self._running

    def stop(self):
        """Stops the publisher.
        May be overriden.
        """
        self._running = False
        if self._channel:
            self._channel.stop_consuming()

    def received(self, message):
        """User defined handler called when a message is received"""
        raise RuntimeError("GenericSubscriber 'received' must be implemented")

    def run(self):
        while self.running():
            try:
                self._connect()
                queue_name = self._channel.queue_declare(
                    exclusive=True).method.queue
                self._channel.basic_consume(
                    self._received,
                    queue=queue_name,
                    no_ack=True)
                self._channel.queue_bind(
                    exchange=self._exchange,
                    queue=queue_name)
                self._channel.start_consuming()
            except pika.exceptions.ConnectionClosed:
                if not self._reconnect:
                    raise

    def _received(self, channel, method, properties, body):
        """Called when a new message is received"""
        self.received(body)
