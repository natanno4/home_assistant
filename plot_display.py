from rabbitMQ.receiver import Receiver
from db.db_handler import DbHandler
import pandas as pd
import matplotlib.pyplot as plt
import json
import matplotlib.dates as mdates
import matplotlib.ticker as tkr
import datetime as dt


class PlotDisplay(Receiver):
    QUEUE = 'update_plot'

    def __init__(self, conn='localhost'):
        """
        constructor
        :param conn: rabbitMQ connection params.
         :type conn: str/pika.params
        """
        super().__init__(conn)
        self.__first_time = True
        self.__table_name = None
        self.__plots = []
        self.__fig = None

    def _receive_callback(self, ch, method, properties, body):
        """handles the message. gets the data base table name from the message, gets information
           from the table and updates/creates the plots with the new information.
           the plots:
           Total sales per month for each year.
           The number of active customers in each month.
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
        self.__table_name = m["table_name"]
        if self.__first_time:  # create the plots if first message.
            self.__create_plots()
        self.__update_plot()
        self.__first_time = False

    def __get_monthly_sales_summary(self):
        """
         gets Total sales per month for each year form given table. return the data or None if failed.
         """
        db = DbHandler('dbinvoices.db')
        if not db.connect():
            return None
        query = 'select strftime("%Y",InvoiceDate) as y, strftime("%m",InvoiceDate) as m, sum(total) as t from ' +\
                self.__table_name + ' group by strftime("%Y",InvoiceDate), strftime("%m",InvoiceDate)'
        res = db.select(query, None)
        return res

    def __get_number_of_monthly_customers(self):
        """
         gets The number of active customers in each month. return the data or None if failed.
         """
        db = DbHandler('dbinvoices.db')
        if not db.connect():
            return None
        query = 'select strftime("%Y",InvoiceDate) as y, strftime("%m",InvoiceDate) as m, count(CustomerId) as c from ' + self.__table_name \
                + ' group by strftime("%Y",InvoiceDate), strftime("%m",InvoiceDate)'
        res = db.select(query, None)
        return res

    def __create_plots(self):
        """
         create plots with no values.
         """
        self.__fig = plt.figure()
        ax1 = self.__fig.add_subplot(111)
        ax2 = ax1.twinx()
        self.__plots.append(ax1)
        self.__plots.append(ax2)
        plt.ion()  # d'ont block with plot.show
        plt.show()


    def __update_plot(self):
        """
         extract data from data base and update the plots with the data.
         """
        sales, customers = self.__get_plot_info()  # get the info from data base.
        x = [i[1] + '/' + i[0] for i in sales]  # create monthly x axis
        df = pd.DataFrame(sales)
        ax1 = self.__plots[0]  # first plot for total
        ax2 = self.__plots[1]  # second plot for customers
        ax1.clear()
        ax1.plot(x, df[2].tolist())
        ax1.set_ylabel('sales summary', color='b')  # set first plot labels and color
        for tick in ax1.get_xticklabels():  # rotate dates to 45.
            tick.set_rotation(45)
        # skip by 3 labels.
        every_nth = 3
        l = len(ax1.xaxis.get_ticklabels())  # get number of labels shown
        for n, label in enumerate(ax1.xaxis.get_ticklabels()):
            if n % every_nth != 0:
                if n == l - 1:  # show last
                    continue;
                label.set_visible(False)

        ax1.tick_params(axis='x', which='major', labelsize=5)  # set dates smaller font size
        ax2.clear()
        df = pd.DataFrame(customers)
        ax2.plot(x, df[2].tolist(), 'r-')
        ax2.set_ylabel('customers', color='r') # set second plot labels and color
        for tl in ax2.get_yticklabels():
            tl.set_color('r')
        plt.draw()
        plt.pause(0.001)


    def __get_plot_info(self):
        """
           gets the data from the data base and returns it. return None if failed
           """
        sales = self.__get_monthly_sales_summary()
        customers = self.__get_number_of_monthly_customers()
        if not sales and not customers:
            return None, None
        return sales, customers

def main():
    plot = PlotDisplay()
    plot.connect()
    plot.create_queue(plot.QUEUE)
    plot.receive_messages(plot.QUEUE)

if __name__ == '__main__':
    main()
