from sqlite.database_interface import DatabaseConnection


class TableDALC(object):
    def __init__(self):
        self.logger = None
        self.DBInterface = None

    def build_select_string_dynamic(self, table_name, select_col_name, where_col_val_pairs, fetchall=True):
        #select_string = "SELECT market_id, name FROM market WHERE market_id=?"

        self.DBInterface = DatabaseConnection()

        # set the SELECT part of the string
        select_str = 'select '
        from_str = ' from ' + table_name
        replace_list = ()
        inc = 0
        length = len(select_col_name)
        for col_val in select_col_name:
            print(col_val)
            inc += 1
            if inc == length:  # final value
                select_str += col_val
            else:
                select_str += col_val + ', '

        select_str += from_str

        # set the WHERE part of the string if exists
        if where_col_val_pairs:
            where_str = ' where '
            inc = 0
            length = len(where_col_val_pairs)
            for where_val in where_col_val_pairs:
                inc += 1
                replace_list += (where_val[1],)  # append to tuple for replacing ?
                if inc == length:  # final value
                    where_str += where_val[0] + ' = ? '
                else:
                    where_str += where_val[0] + ' = ? and '
            select_str += where_str

        print(select_str)
        print(replace_list)
        if fetchall:
            resp = self.DBInterface.execute_data_access(select_str, replace_list)
        else:
            resp = self.DBInterface.execute_data_access_fetchone(select_str, replace_list)
        return resp

    def market_select(self, market_id=None):
        #exec_string = "SELECT market_id, name FROM market WHERE market_id=?"
        select_col_name = ['market_id', 'name', 'market_start_time']

        if market_id:
            where_col_val_pairs = [('market_id', market_id)]
        else:
            where_col_val_pairs = []
        resp = self.build_select_string_dynamic('market', select_col_name, where_col_val_pairs)

        return resp

    def runner_all_cols(self, runner_id=None):
        # exec_string = "SELECT * FROM initial_data WHERE runner_id=?"
        select_col_name = ['*']
        where_col_val_pairs = []

        if runner_id:
            where_col_val_pairs += [('runner_id', runner_id)]

        resp = self.build_select_string_dynamic('runner', select_col_name, where_col_val_pairs)

        return resp

    def initial_data_all_cols(self, runner_id=None):
        # exec_string = "SELECT * FROM initial_data WHERE runner_id=?"
        select_col_name = ['*']
        where_col_val_pairs = []

        if runner_id:
            where_col_val_pairs += [('runner_id', runner_id)]

        resp = self.build_select_string_dynamic('initial_data', select_col_name, where_col_val_pairs)

        return resp

    def unmatched_data_all_cols(self, runner_id=None, bet_type_id=None):
        # exec_string = "SELECT * FROM unmatched_data WHERE runner_id=? and bet_type_id = ?"
        select_col_name = ['*']
        where_col_val_pairs = []

        if runner_id:
            where_col_val_pairs += [('runner_id', runner_id)]
        if bet_type_id:
            where_col_val_pairs += [('bet_type_id', bet_type_id)]

        resp = self.build_select_string_dynamic('unmatched_data', select_col_name, where_col_val_pairs)

        return resp

    def matched_data_all_cols(self, bet_id=None):
        # exec_string = "SELECT * FROM matched_data WHERE bet_id=?"
        select_col_name = ['*']
        where_col_val_pairs = []

        if bet_id:
            where_col_val_pairs += [('bet_id', bet_id)]

        resp = self.build_select_string_dynamic('matched_data', select_col_name, where_col_val_pairs)

        return resp

    def sum_size_matched_data(self, runner_id, bet_type_id):
        #exec_string = "SELECT sum(size_matched) as sum_size_matched FROM matched_data WHERE runner_id=? and bet_type_id=? "
        select_col_name = ['sum(size_matched) as sum_size_matched']
        where_col_val_pairs = []

        if runner_id:
            where_col_val_pairs += [('runner_id', runner_id)]
        if bet_type_id:
            where_col_val_pairs += [('bet_type_id', bet_type_id)]

        resp = self.build_select_string_dynamic('matched_data', select_col_name, where_col_val_pairs, fetchall=False)
        if resp:
            if resp['sum_size_matched']:
                return resp['sum_size_matched']
            else:
                return 0
        else:
            return 0

        return resp