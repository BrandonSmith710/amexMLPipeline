from functions.input_functions import (yes_no_input, table_input,
                                       collect_query_params,
                                       conjoin_filter)
from functions.db_functions import (get_conn, get_curs, disconnect,
                                    filter_table)


conn = get_conn()
curs = get_curs(conn = conn)


if __name__ == '__main__':

    querying = True
    while querying:
        print('Enter exit to abandon statistics')
        table = table_input()
        if table:
            params = collect_query_params(table = table)
            if params:
                params = conjoin_filter(params)
                new = filter_table(conn = conn, curs = curs, table = table, fields = params)
                print(new[:30])
            print('Filter another table?')
            querying = yes_no_input()
        else:
            querying = False

    disconnect(conn = conn, curs = curs)