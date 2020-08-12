import pika
from rabbitMQ.rabbitMQ_connection import RabbitMQConnection
from abc import ABC, abstractmethod


class Receiver(ABC):
    def __init__(self, params='localhost'):
        """
        constructor
        :param params: rabbitMQ connection params.
         :type params: str/pika.params
        """
        self.reconnection_counter = 0
        self.params = params
        self._connection = None

    def connect(self):
        """
        connect to rabbitMQ
        """
        self._connection = RabbitMQConnection(self.params)
        self._connection.connect()

    def create_queue(self, queue_name):
        """declare a simple queue and try to reconnect 5 times if connection closed.
        :param queue_name: The queue name.
        :type queue_name: str
        """
        try:
            self._connection.get_channel().queue_declare(queue=queue_name)
        except pika.exceptions.ConnectionClosed:
            self.reconnection_counter += 1
            if self.reconnection_counter > 5:
                return
            self._connection.c
            self.create_queue(queue_name)
            self.reconnection_counter = 0

    def receive_messages(self, queue_name='', auto_ack=True):
        """starting a simple messages consumption, receive_callback function will handle the messages.
           try to reconnect 5 times if connection closed.
        :param queue_name: The queue name.
        :type queue_name: str
        :param auto_ack: Manual message acknowledgments.
        :type auto_ack: boolean
        """
        try:
            self._connection.get_channel().basic_consume(
                queue=queue_name, on_message_callback=self._receive_callback, auto_ack=auto_ack)
            print("start consuming")
            self._connection.get_channel().start_consuming()
        except pika.exceptions.ConnectionClosed:
            self.reconnection_counter += 1
            if self.reconnection_counter > 5:
                return
            self._connection.c
            self.create_queue(queue_name)
            self.receive_messages(queue_name, auto_ack)
            self.reconnection_counter = 0

    @abstractmethod
    def _receive_callback(self,ch, method, properties, body):
        """handles the message, as an abstract method.
        :param ch: The chanel that has been used.
        :type ch: chanel
        :param method: method frame - details.
        :type method: pika.spec.BasicProperties.
        :param properties: header frame.
        :type properties: pika.spec.BasicProperties
        :param body: message body.
        :type properties: bytearray

        """
        pass

    def close_receiver(self):
        """
        disconnect from rabbitMQ
        """
        self._connection.disconnect()

