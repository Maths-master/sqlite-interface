import os
import sqlite3 as lite


class DatabaseConnection(object):
    def __init__(self):
        self.logger = None
        self.db_path = os.path.abspath(os.path.dirname(__file__) + '/../../../Betfair_Database/betfairdb.db')
        self.con = lite.connect(self.db_path)

    def execute_data_access(self, exec_string = None, replace_string = None):
        if exec_string:
            try:
                with self.con:
                    self.con.row_factory = lite.Row

                    cur = self.con.cursor()
                    cur.execute(exec_string, replace_string)
                    data = cur.fetchall()

                    # if nothing then return nothing
                    if not data:
                        return None

                    if type(data) == list:
                        # return even if empty list and check in the logic
                        return data
                    else:
                        raise Exception(str(data) + str(exec_string) + str(replace_string))
            except lite.Error as er:
                print('er: ' + str(er))

    def execute_data_access_fetchone(self, exec_string=None, replace_string=None):
        if exec_string:
            try:
                with self.con:
                    self.con.row_factory = lite.Row
                    cur = self.con.cursor()
                    cur.execute(exec_string, replace_string)
                    row = cur.fetchone()

                    # check if anything is returned
                    if not row:
                        return None

                    if str(type(row)) == "<class 'sqlite3.Row'>":
                        # return even if empty list and check in the logic
                        return row
                    else:
                        raise Exception(str(row) + str(exec_string) + str(replace_string))
            except lite.Error as er:
                print('er: ' + str(er))

    def execute_data_management(self, exec_string = None, replace_string = None):
        #exec_string = "insert into market (market_id, name) values( ?, ?)"
        #replace_string = (1,'TEST') or ('ID3', 'TSCO', 16) /needs to correspond to same number of '?' as exec _string
        if exec_string:
            try:
                with self.con:
                    cur = self.con.cursor()
                    cur.execute(exec_string, replace_string)
            except lite.IntegrityError as intEr:
                print("Integrity constraint error" + str(intEr))
            except lite.Error as er:
                print('er: ' + str(er))

