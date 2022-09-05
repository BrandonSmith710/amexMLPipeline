import pandas as pd
import numpy as np
from functions.db_functions import get_conn, get_curs, disconnect, load_train, load_test
from functions.ml_functions import transform_train, transform_test

conn = get_conn()
curs = get_curs(conn = conn)
train = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')


if __name__ == '__main__':

    transform_train(train = train)
    transform_test(test = test)
    print(load_train(conn = conn, curs = curs, train = train))
    print(load_test(conn = conn, curs = curs, test = test))
    disconnect(conn = conn, curs = curs)
