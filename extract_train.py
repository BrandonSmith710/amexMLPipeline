from functions.db_functions import get_conn, get_curs, disconnect
from functions.ml_functions import train_xgb

conn = get_conn()
curs = get_curs(conn = conn)


if __name__ == '__main__':
    
    print(train_xgb(curs = curs))
    disconnect(conn = conn, curs = curs)
