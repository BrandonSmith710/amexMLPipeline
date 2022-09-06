from functions.db_functions import add_customer, delete_customer, get_conn, get_curs, disconnect
from functions.db_functions import customers_columns, customers_train_columns
from functions.input_functions import *

conn = get_conn()
curs = get_curs(conn = conn)


if __name__ == '__main__':

    updating = True
    while updating:
        print('Enter exit to abandon update')
        table = table_input()
        if table == 'customers':
            cols = customers_columns
        elif table == 'customers_train':
            cols = customers_train_columns
        else:
            break
        aod = add_or_delete()
        if aod == 'exit':
            print('Add or delete another customer?')
            updating = yes_no_input()
            continue
        elif aod:
            data = collect_input(table = table)
            if data:
                print(add_customer(conn = conn, curs = curs, table = table,
                                   cols = cols, data = data))
            else:
                print('Add or delete another customer?')
                updating = yes_no_input()
                continue
        else:
            id1 = input('Enter customer ID: ').upper()
            if 'EXIT' in id1:
                print('Add or delete another customer?')
                updating = yes_no_input()
                continue
            try:
                id = cid_check(id1)
            except Exception as e:
                print('Invalid cid entry')
                continue
            name = validate_cid(curs = curs, table = table, id = id)
            if name:
                print(delete_customer(conn = conn, curs = curs, table = table,
                                      cid = id))
            else:
                print(f'Could not find cid {id1} in table {table}')
        print('Add or delete another customer?')
        updating = yes_no_input()
disconnect(conn = conn, curs = curs)
