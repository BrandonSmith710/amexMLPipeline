from functions.db_functions import get_conn, get_curs, disconnect, update_info
from functions.input_functions import *

conn = get_conn()
curs = get_curs(conn = conn)


if __name__ == '__main__':
    
    updating = True
    while updating:
        cid = None
        adding_field = False
        while not adding_field:
            table = table_input()
            print('Enter exit to abandon update')
            id = input('Enter customer ID: ').upper()
            if 'EXIT' in id:
                break
            customer_name = validate_cid(curs = curs, table = table,
                                         id = id)
            if customer_name:
                print(f'Found cid {id}: {customer_name}')
                cid = id
                adding_field = True
            else:
                print(f'Error finding cid "{id}" in table "{table}"')
        fields = field_inputs(adding_field = adding_field, table = table)
        if fields:
            print(update_info(conn = conn, curs = curs, table = table,
                              id = cid, fields = fields))
        else:
            print('Exited from update')
        print('Update another customer?')
        updating = yes_no_input()
    disconnect(conn = conn, curs = curs)


        
