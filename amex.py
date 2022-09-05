import pickle
from functions.db_functions import get_conn, get_curs, disconnect
from functions.ml_functions import predict_default
from functions.input_functions import table_input, validate_cid, yes_no_input

conn = get_conn()
curs = get_curs(conn = conn)
model = pickle.load(open('model_xgb.h5', 'rb'))


if __name__ == '__main__':

    predicting = True
    table = table_input()
    while predicting:
        print('Enter exit to abandon prediction')
        id = input('Enter customer ID: ').upper()
        if 'EXIT' in id:
            print('Exiting...')
            predicting = False
            break
        name = validate_cid(curs = curs, table = table, id = id)
        if name:
            print(predict_default(curs = curs, id = id, model = model))
            print('Predict another customer?')
            predicting = yes_no_input()
        else:
            print('Invalid customer ID')
    disconnect(conn = conn, curs = curs)
