from sqlite.database_interface import DatabaseConnection


class TableDML(object):
    def __init__(self):
        self.logger = None
        self.DBInterface = None

    def build_insert_string_dynamic(self, table_name, column_value_pairs):
        #exec_string = "insert into market(market_id, name) values(?,?)"
        #replace_string = (1.68768758, 'fish')
        self.DBInterface = DatabaseConnection()
        exec_str = 'insert into ' + table_name + ' ('
        end_str = 'values('
        replace_string = ()
        inc = 0
        length = len(column_value_pairs)
        for col_val in column_value_pairs:
            inc += 1
            replace_string += (col_val[1],)
            if inc == length: #final value
                exec_str += col_val[0] + ') '
                end_str += '?)'
            else:
                exec_str += col_val[0] + ', '
                end_str += '?, '
        exec_str += end_str
        #print(exec_str)
        #print(replace_string)
        self.DBInterface.execute_data_management(exec_str, replace_string)

    def build_simple_update_string_dynamic(self, table_name, update_col_value_pairs, where_col_val_pairs):
        #exec_string = "update market set name = ?, blah = ? where market_id = ?"
        #replace_list = ('fish', 'bob', 1.287982798)
        self.DBInterface = DatabaseConnection()

        # set the update part of the string
        exec_str = 'update ' + table_name + ' set '
        replace_string = ()
        inc = 0
        length = len(update_col_value_pairs)
        for col_val in update_col_value_pairs:
            inc += 1
            replace_string += (col_val[1],) #append to tuple for replacing ?
            if inc == length: #final value
                exec_str += col_val[0] + ' = ? '
            else:
                exec_str += col_val[0] + ' = ?, '

        # set the where part of the string
        where_str = 'where '
        inc = 0
        length = len(where_col_val_pairs)
        for where_val in where_col_val_pairs:
            inc += 1
            replace_string += (where_val[1],) #append to tuple for replacing ?
            if inc == length:  # final value
                where_str += where_val[0] + ' = ? '
            else:
                where_str += where_val[0] + ' = ? and '

        exec_str += where_str
        #print(exec_str)
        #print(replace_string)
        self.DBInterface.execute_data_management(exec_str, replace_string)

    # market
    def market_insert(self, market_id, name, market_start_time):
        column_value_pairs = [('market_id', market_id),
                              ('name', name),
                              ('market_start_time', market_start_time)]
        #self.ring_dynamic(self, table_name='market', column_value_pairs=column_value_pairs) #self only used when running from python
        self.build_insert_string_dynamic('market', column_value_pairs) #self only used when running from python

    # runner
    def runner_insert(self, runner_id, market_id, selection_id):
        column_value_pairs = [('runner_id', runner_id),
                              ('market_id', market_id),
                              ('selection_id', selection_id)]
        self.build_insert_string_dynamic('runner', column_value_pairs)

    # initial_data
    def initial_data_insert(self, runner_id, back_det, lay_det,  margin_to_even,
                            back_price, lay_price, size):
        column_value_pairs = [('runner_id', runner_id),
                              ('back_det', back_det),
                              ('lay_det', lay_det),
                              ('margin_to_even', margin_to_even),
                              ('back_price', back_price),
                              ('lay_price', lay_price),
                              ('size', size)]
        self.build_insert_string_dynamic('initial_data', column_value_pairs)

    def initial_data_update(self, runner_id, back_det, lay_det, margin_to_even,
                            back_price, lay_price, size):
        update_col_value_pairs = [('back_det', back_det),
                                  ('lay_det', lay_det),
                                  ('margin_to_even', margin_to_even),
                                  ('back_price', back_price),
                                  ('lay_price', lay_price),
                                  ('size', size)]

        where_col_val_pairs = [('runner_id', runner_id)]

        self.build_simple_update_string_dynamic('initial_data', update_col_value_pairs, where_col_val_pairs)

    def initial_data_update_back(self, runner_id, back_det, margin_to_even,
                                 back_price, size):
        update_col_value_pairs = [('back_det', back_det),
                                  ('margin_to_even', margin_to_even),
                                  ('back_price', back_price),
                                  ('size', size)]

        where_col_val_pairs = [('runner_id', runner_id)]

        self.build_simple_update_string_dynamic('initial_data', update_col_value_pairs, where_col_val_pairs)

    def initial_data_update_lay(self, runner_id, lay_det, margin_to_even,
                                lay_price, size):
        update_col_value_pairs = [('lay_det', lay_det),
                                  ('margin_to_even', margin_to_even),
                                  ('lay_price', lay_price),
                                  ('size', size)]

        where_col_val_pairs = [('runner_id', runner_id)]

        self.build_simple_update_string_dynamic('initial_data', update_col_value_pairs, where_col_val_pairs)

    # unmatched_data
    def unmatched_data_insert(self, bet_id, bet_type_id, runner_id, back_det, lay_det, margin_to_even, price, size_unmatched, is_cancelled):
        column_value_pairs = [('bet_id', bet_id),
                              ('bet_type_id', bet_type_id),
                              ('runner_id', runner_id),
                              ('back_det', back_det),
                              ('lay_det', lay_det),
                              ('margin_to_even', margin_to_even),
                              ('price', price),
                              ('size_unmatched', size_unmatched),
                              ('is_cancelled', is_cancelled)]
        self.build_insert_string_dynamic('unmatched_data', column_value_pairs)

    def unmatched_data_update(self, runner_id, bet_type_id, bet_id, back_det, lay_det, margin_to_even, price, is_cancelled):
        update_col_value_pairs = [('bet_id', bet_id),
                                  ('back_det', back_det),
                                  ('lay_det', lay_det),
                                  ('margin_to_even', margin_to_even),
                                  ('price', price),
                                  ('is_cancelled', is_cancelled)]

        where_col_val_pairs = [('runner_id', runner_id),
                               ('bet_type_id', bet_type_id)]

        self.build_simple_update_string_dynamic('unmatched_data', update_col_value_pairs, where_col_val_pairs)

    def unmatched_data_update_size(self, bet_id, size_unmatched):
        update_col_value_pairs = [('size_unmatched', size_unmatched)]

        where_col_val_pairs = [('bet_id', bet_id)]

        self.build_simple_update_string_dynamic('unmatched_data', update_col_value_pairs, where_col_val_pairs)

    # matched_data
    def matched_data_insert(self, bet_id, bet_type_id, runner_id, back_det, lay_det, margin_to_even, price, size_matched):
        column_value_pairs = [('bet_id', bet_id),
                              ('bet_type_id', bet_type_id),
                              ('runner_id', runner_id),
                              ('back_det', back_det),
                              ('lay_det', lay_det),
                              ('margin_to_even', margin_to_even),
                              ('price', price),
                              ('size_matched', size_matched)]
        self.build_insert_string_dynamic('matched_data', column_value_pairs)

    def matched_data_update_size(self, bet_id, size_matched):
        update_col_value_pairs = [('size_matched', size_matched)]

        where_col_val_pairs = [('bet_id', bet_id)]

        self.build_simple_update_string_dynamic('matched_data', update_col_value_pairs, where_col_val_pairs)




