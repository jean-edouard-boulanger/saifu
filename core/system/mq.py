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


class _GenericMQAgent(threading.Thread):
    def __init__(self, connector, reconnect=True):
        super(_GenericMQAgent, self).__init__()
        self._connector = connector
        self._reconnect = reconnect
        self._channel = None
        self._running = True

    def _get_channel(self):
        """Returns the connection channel"""
        return self._channel

    def _initialize(self):
        """Post initialization done after the connection."""
        pass

    def _dispatch(self):
        """Dispatch the agent work"""
        pass

    def _post_stop(self):
        """Additional stop operations"""
        pass

    def _connect(self):
        """Connects the agent to the message queue broker"""
        connection = self._connector.connect()
        self._channel = connection.channel()

    def running(self):
        """Indicates whether the agent should be running."""
        return self._running

    def run(self):
        """Starts the agent thread"""
        while self.running():
            try:
                # Connects to the message queue broker
                self._connect()

                # Post connection initialization (Create queues or exchanges)
                self._initialize()

                # Dispatch the work.
                self._dispatch()
            except pika.exceptions.ConnectionClosed:
                if not self._reconnect:
                    raise

    def stop(self):
        """Stops the agent.
        May be overriden.
        """
        self._running = False
        self._post_stop()


class GenericPublisher(_GenericMQAgent):
    """Generic threaded publisher"""
    def __init__(self, exchange, connector, reconnect=True):
        super(GenericPublisher, self).__init__(connector, reconnect)
        self._exchange = exchange

    def _initialize(self):
        """Publisher agent initialization (internal)"""
        self._get_channel().exchange_declare(
            exchange=self._exchange,
            type='fanout')

    def _dispatch(self):
        """Dispatch to user implementation"""
        self.work()

    def work(self):
        """Publisher implementation
        Must be implemented by user.
        """
        raise RuntimeError("work is not implemented")

    def publish(self, data):
        """Publishes data to the exchange"""
        self._get_channel().basic_publish(
            exchange=self._exchange,
            routing_key='',
            body=data)


class GenericSubscriber(_GenericMQAgent):
    """Generic threaded subscriber
    A subscriber subscribes to an exchange and receives broadcasted updates
    """
    def __init__(self, exchange, connector, reconnect=True):
        super(GenericSubscriber, self).__init__(connector, reconnect)
        self._exchange = exchange

    def _initialize(self):
        """Publisher agent initialization (internal)"""
        self._get_channel().exchange_declare(
            exchange=self._exchange,
            type='fanout')
        queue_name = self._channel.queue_declare(exclusive=True).method.queue
        self._channel.basic_consume(self._received, queue=queue_name, no_ack=True)
        self._channel.queue_bind(exchange=self._exchange, queue=queue_name)

    def _dispatch(self):
        """Starts to consumme incomming updates"""
        self._get_channel().start_consuming()

    def _post_stop(self):
        """Stop consumming after stop is called"""
        if self._get_channel() is not None:
            self._get_channel().stop_consuming()

    def _received(self, channel, method, properties, body):
        """Called when a new message is received"""
        self.received(body)

    def received(self, message):
        """User defined handler called when a message is received"""
        raise RuntimeError("received is not implemented")


class GenericDispatcher(_GenericMQAgent):
    """Generic threaded work dispatcher
    A dispatcher dispatches work on a work queue. An item is picked up by a
    single worker agent
    """
    def __init__(self, queue, connector, reconnect=True):
        super(GenericDispatcher, self).__init__(connector, reconnect)
        self._queue = queue

    def _initialize(self):
        """Publisher agent initialization (internal)"""
        self._get_channel().exchange_declare(exchange="Direct-X", exchange_type="direct")

    def _dispatch(self):
        """Calls into work implementation"""
        self.work()

    def dispatch(self, job):
        """Dispatch a job to the queue"""
        self._get_channel().basic_publish(
            exchange="Direct-X",
            routing_key="Key1",
            body=job)

    def work(self):
        """Publisher implementation
        Must be implemented by user.
        """
        raise RuntimeError("work is not implemented")

class GenericWorker(_GenericMQAgent):
    """Generic threaded worker
    A worker picks up work from a work queue and execute it
    """
    def __init__(self, queue, connector, reconnect=True):
        super(GenericWorker, self).__init__(connector, reconnect)
        self._queue = queue

    def _handle(self, ch, method, properties, body):
        self.handle(body)

    def _initialize(self):
        """Publisher agent initialization (internal)"""
        self._get_channel().exchange_declare(exchange="Direct-X", exchange_type="direct")
        self._get_channel().queue_declare(queue=self._queue, durable=True)
        self._get_channel().queue_bind(exchange="Direct-X",queue=self._queue, routing_key="Key1")
        self._get_channel().basic_consume(self._handle, queue=self._queue, no_ack=True)

    def _post_stop(self):
        """Stop consumming after stop is called"""
        if self._get_channel() is not None:
            self._get_channel().stop_consuming()

    def _dispatch(self):
        self._get_channel().start_consuming()

    def handle(self, job, ack):
        """Handles the job. ack must be called (with no argument) after the
        job is executer.
        """
        raise RuntimeError("handle is not implemented")
