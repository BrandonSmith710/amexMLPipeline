import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import AsIs
from os import getenv
import pandas as pd


def get_conn():
    return psycopg2.connect(host = getenv('HOST'),
                            password = getenv('PASSWORD'),
                            user = getenv('USER'),
                            database = getenv('DATABASE'))


def get_curs(conn):
    return conn.cursor()


def disconnect(conn, curs):
    curs.close()
    conn.close()


def load_train(conn, curs, train) -> str:
    '''create customers_train table and/or insert data into customers_train
       parameters: conn - psycopg2 connection
                   curs - psycopg2 cursor
                   train - pandas dataframe
    '''

    curs.execute(
        '''CREATE TABLE IF NOT EXISTS customers_train (cid VARCHAR PRIMARY KEY,
        name VARCHAR, age int, gender VARCHAR, owns_car VARCHAR, owns_house
        VARCHAR, num_children int, yearly_income float, num_days_employed int,
        occupation VARCHAR, num_family_members int, migrant_worker int,
        yearly_payments float, credit_limit float, percent_credit_limit_used
        int, credit_score int, prev_defaults int, default_in_last_six_months
        int, credit_card_default int);''')
    execute_values(curs,
        '''INSERT INTO customers_train (cid, name, age, gender, owns_car,
        owns_house, num_children, yearly_income, num_days_employed, occupation,
        num_family_members, migrant_worker, yearly_payments, credit_limit,
        percent_credit_limit_used, credit_score, prev_defaults,
        default_in_last_six_months, credit_card_default) VALUES %s;''',
        train.values)
    conn.commit()
    return 'customers_train successfully inserted'


def load_test(conn, curs, test) -> str:
    '''create customers table and/or insert testing data into customers
       parameters: conn - psycopg2 connection
                   curs - psycopg2 cursor
                   test - pandas dataframe
    '''

    curs.execute(
        '''CREATE TABLE IF NOT EXISTS customers (cid VARCHAR PRIMARY KEY,
        name VARCHAR, age int, gender VARCHAR, owns_car VARCHAR, owns_house
        VARCHAR, num_children int, yearly_income float, num_days_employed int,
        occupation VARCHAR, num_family_members int, migrant_worker int,
        yearly_payments float, credit_limit float, percent_credit_limit_used
        int, credit_score int, prev_defaults int, default_in_last_six_months
        int);''')
    execute_values(curs,
        '''INSERT INTO customers (cid, name, age, gender, owns_car,
        owns_house, num_children, yearly_income, num_days_employed, occupation,
        num_family_members, migrant_worker, yearly_payments, credit_limit,
        percent_credit_limit_used, credit_score, prev_defaults,
        default_in_last_six_months) VALUES %s;''', test.values)
    conn.commit()
    return 'customers successfully inserted'


def validate_cid(curs, table, id) -> str or bool:
    '''verify the id exists in table's primary key index
    parameters: curs - psycopg2 cursor
                table - customers or customers_train
                id - uppercase alphanumeric cid
    returns: cid if valid, else False
    '''

    try:
        curs.execute(
            sql.SQL("SELECT name FROM {table} WHERE cid = %s;").format(
                table = sql.Identifier(table)), (id,)
            )
        name = curs.fetchall()[0]
        return name
    except:
        return False


def update_info(conn, curs, table, id, fields) -> str:
    '''update database record matching id to the values specified in fields
       parameters: conn - psycopg2 connection
                   curs - psycopg2 cursor
                   table - table containing record primary key of id
                   id - cid belonging to primary key index of table
                   fields (list of tuples ex. (field, new_value))
    '''

    col_names, new_vals = [], []
    for field in fields:
        a, b = field
        if type(b) == str:
            new_vals.append(f"'{b}'")
        else:
            new_vals.append(b)
        col_names.append(a)
    e = ', '.join(f'{a} = {b}' for a, b in zip(col_names, new_vals))
    query_1 = sql.SQL('SELECT * FROM {table} WHERE cid = %s;').format(
        table = sql.Identifier(table)
    )
    query_2 = sql.SQL('UPDATE {table} SET %s WHERE cid = %s;').format(
        table = sql.Identifier(table)
    )
    curs.execute(query_1, (id,))
    a = list(curs.fetchall()[0])
    curs.execute(query_2, (AsIs(e), id))
    conn.commit()
    curs.execute(query_1, (id,))
    b = list(curs.fetchall()[0])
    print(str(a) + '\n\n' + str(b))
    return f'Customer {id} updated'


def add_customer(conn, curs, table, cols, data) -> str:
    '''insert a record into table
    parameters: conn - psycopg2 connection
                curs - psycopg2 cursor
                table - customers or customers_train
                cols - dict with column name keys or
                       list of column names for table
                data - list of data for insertion
    '''

    data = ', '.join(map(str, data))
    query_1 = sql.SQL('INSERT INTO {} (%s) VALUES (%s);').format(
        sql.Identifier(table)
    )
    curs.execute(query_1, (AsIs(', '.join(cols)), AsIs(data)))
    conn.commit()
    return 'Successfully added customer'


def delete_customer(conn, curs, table, cid) -> str:
    '''delete a record from table
    parameters: conn - psycopg2 connection
                curs - psycopg2 cursor
                table - customers or customers_train
                cid - cid found in primary key index of table
    '''

    query_1 = sql.SQL('DELETE FROM {} WHERE cid = %s;').format(
        sql.Identifier(table)
    )
    curs.execute(query_1, (cid,))
    conn.commit()
    return 'Successfully deleted customer'


def filter_table(curs, table, fields) -> list:
    
    '''
    query_1 filters the table according to input, and asks 
    asks for the three highest salaries per gender per occupation
    '''

    query_1 = sql.SQL('''with one as
                            (SELECT
                            occupation
                            , gender
                            , yearly_income
                            , rank() over (PARTITION BY occupation, gender
                                           ORDER BY yearly_income DESC) as rank
                            FROM {} WHERE %s
                            GROUP BY occupation, gender, yearly_income)
                        SELECT * FROM one
                        WHERE rank <= 3;''').format(sql.Identifier(table))
    curs.execute(query_1, (AsIs(fields),))
    cols = curs.description
    return cols, curs.fetchall()