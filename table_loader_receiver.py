import pika
from rabbitMQ.receiver import Receiver
from rabbitMQ.sender import Sender
from db.db_handler import DbHandler
import json
import time
from utils.file_data_extractors.csv_file_data_extractor import CsvFileDataExtractor
from utils.file_data_extractors.json_file_data_extractor import JsonFileDataExtractor


class TableLoaderReceiver(Receiver, Sender):

    QUEUE1 = 'file_and_table_name'
    QUEUE2 = 'update_plot'

    def __init__(self, conn = 'localhost'):
        """
        constructor
        :param conn: rabbitMQ connection params.
         :type conn: str/pika.params
        """
        super().__init__(conn)

    def _receive_callback(self,ch, method, properties, body):
        """handles the message, gets the file path,type and table name, inserts the
           data from the file into the data base table and updates(send message)
           the plot display if the insertion succeeded.
        :param ch: The chanel that has been used.
        :type ch: chanel
        :param method: method frame - details.
        :type method: pika.spec.BasicProperties.
        :param properties: header frame.
        :type properties: pika.spec.BasicProperties
        :param body: message body.
        :type properties: bytearray

        """
        m = json.loads(body)
        if body:
            extractor = None
            if m['file_type'] == 'CSV':  # check the file type.
                extractor = CsvFileDataExtractor()
            else:
                extractor = JsonFileDataExtractor()
            data = extractor.data_extractor(m['file_path'])  # get data from the file.
            if data:
                if self.__insert_data_to_tabel(m['table_name'], data): # try to insert data to data base
                    self.create_queue(self.QUEUE2)
                    self.send_message('', self.QUEUE2, json.dumps({'table_name' : m['table_name']}))  # send message
                    time.sleep(3)  # wait 3 second before continue to next message.
            else:
                print('failed getting data from file')
        else:
            print('error with given message')

    def __insert_data_to_tabel(self, table_name, data):
        """
        insert the data from file to the given table in the data base . returns True if insertion succeeded, else False.
        :param table_name: data base table name.
         :type table_name: str
         :param data: filed names and params for query.
         :type data: list
        """
        db = DbHandler('dbinvoices.db')
        if not db.connect():
            return
        query = 'insert into ' + table_name + '(' + ','.join(data[0]) + ') VALUES (' \
                + ','.join(['?'] * len(data[0])) + ')'  # create query
        params = data[1:] # params for the query
        return db.insert(query, params)


def main():
    receiver = TableLoaderReceiver()
    receiver.connect()
    receiver.create_queue(receiver.QUEUE1)
    receiver.receive_messages(receiver.QUEUE1)


if __name__ == '__main__':
    main()
