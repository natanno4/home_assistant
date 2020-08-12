from rabbitMQ.sender import Sender
import json
from pathlib import Path
import time


class FileAndTableNameSender(Sender):
    QUEUE = 'file_and_table_name'

    def __init__(self, conn='localhost'):
       """
       constructor
       :param conn: rabbitMQ connection params.
       :type conn: str/pika.params
       """
       super().__init__(conn)


def main():
    # file list
    files = ['invoices_2009.json', 'invoices_2010.json','invoices_2011.json', 'invoices_2012.csv', 'invoices_2013.csv']
    sender = FileAndTableNameSender()
    sender.connect()
    sender.create_queue(sender.QUEUE)
    for file in files:
        # create message for each file
        message = {'file_path': file, 'table_name': 'invoices', 'file_type': Path(file).suffix.upper()[1:]}
        sender.send_message("",sender.QUEUE, json.dumps(message))
        time.sleep(2)  # wait 2 seconds between messages.
    sender.close_sender()


if __name__ == '__main__':
    main()
