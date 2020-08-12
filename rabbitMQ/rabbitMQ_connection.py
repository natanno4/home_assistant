import pika


class RabbitMQConnection:
    
    def __init__(self, params='localhost'):
        """
        constructor
        :param params: rabbitMQ connection params.
         :type params: str/pika.params
        """
        self.params = params

    def connect(self):
        """
        connect to rabbitMQ and create chanel
        :param queue_nte chaame: The queue name.
         :type queue_name: str
        """
        try:
            self.__connection = pika.BlockingConnection(pika.ConnectionParameters(self.params))
            self.__channel = self.__connection.channel()
        except pika.exceptions.AMQPConnectionError:
            print('rabbitMQ connection error')

    def get_channel(self):
        """
        get the open channel.
        """
        return self.__channel

    def disconnect(self):
        """
        disconnect from rabbitMq.
        """
        if not self.__connection.is_closed:
            self.__connection.close()

    def check_if_connection_open(self):
        """
        check if connection still open.
        """
        return not self.__connection.is_closed()
