from rabbitMQ.rabbitMQ_connection import RabbitMQConnection


class Sender:
    def __init__(self, params='localhost'):
        """
        constructor
        :param params: rabbitMQ connection params.
         :type params: str/pika.params
        """
        self.params = params
        self._connection = None

    def connect(self):
        """
        connect to rabbitMQ
        """
        self._connection = RabbitMQConnection(self.params)
        self._connection.connect()

    def create_queue(self, queue):
        """declare a simple queue and try to reconnect 5 times if connection closed.
         :param queue_name: The queue name.
         :type queue_name: str
         """
        try:
            self._connection.get_channel().queue_declare(queue=queue)
        except:
            print('queue error')

    def send_message(self, exchange='', routing_key='', body=''):
        """sends the body message to the given exchange and routing_key(queue name)
        :param exchange: The exchange name.
        :type exchange: str
        :param routing_key: The queue name.
        :type routing_key: str
        :param body: message body.
        :type body: str
        """
        try:
            self._connection.get_channel().basic_publish(exchange=exchange,
                               routing_key=routing_key,
                               body=body)
        except:
            print('message sending error')

    def close_sender(self):
        """
        disconnect from rabbitMQ
        """
        self._connection.disconnect()
