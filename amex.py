from functions.db_functions import (get_conn, get_curs,
                                    disconnect, validate_cid)
from functions.ml_functions import predict_default
from functions.input_functions import table_input, yes_no_input
import pickle


conn = get_conn()
curs = get_curs(conn = conn)
model = pickle.load(open('model_xgb.h5', 'rb'))


if __name__ == '__main__':

    print('Enter exit to abandon prediction')
    predicting = True
    table = table_input()
    if not table:
        predicting = False
    while predicting:
        id = input('Enter customer ID: ').upper()
        if 'EXIT' in id:
            print('Exiting...')
            break
        name = validate_cid(curs = curs, table = table, id = id)
        if name:
            print(predict_default(curs = curs, table = table,
                                  id = id, model = model))
            print('Predict another customer?')
            predicting = yes_no_input()
            if predicting:
                print(f'Is this customer in {table}?')
                answer = yes_no_input()
                if not answer:
                    if table == 'customers_train':
                        table = 'customers'
                    else:
                        table = 'customers_train'
        else:
            print('Invalid customer ID')
    disconnect(conn = conn, curs = curs)
