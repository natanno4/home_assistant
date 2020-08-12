import sqlite3


class DbHandler:
    def __init__(self, db_path):
        """
        constructor
        :param db_path: db file path to connect to.
         :type db_path: str
        """
        self.__path=db_path
        self.__conn = None
        self.__cursor = None

    def get_path(self):
        """
        returns the data base connection path.
        """
        return self.__path

    def connect(self):
        """
        connect to the data base path. and create cursor.returns True if ok, else False.
        """
        try:
            self.__conn = sqlite3.connect(self.__path)
            self.__cursor = self.__conn.cursor()
            print("connected to database")
            return True
        except:
            print('connection error')
            return False

    def disconnect(self):
        """
        disconnect from the data base.
        """
        if self.__conn:
            self.__conn.close()
            print("disconnected from database")

    def select(self, query, params):
        """
        runs an select query with the given query and params. returns the result or None if Failed.
         disconnects from the connection after query.
        :param query: select query.
        :type query: str
        :param query: select query.
        :type query: str
        """
        try:
            if params:
                self.__cursor.execute(query, params)
            else:
                self.__cursor.execute(query)
            rs = self.__cursor.fetchall()
            self.disconnect()
            return rs
        except:
            print('select failed')
            return None

    def insert(self, query, params):
        """
        runs an insert query that can handle multiple values with the given query and params.
         returns the True or False if Failed.
         disconnects from the connection after query.
        :param query: insert query.
        :type query: str
        :param query: select query.
        :type query: str
        """
        try:
            self.__cursor.executemany(query, params)
            self.__conn.commit()
            self.disconnect()
            return True
        except:
            print('insertion failed')
            return False


