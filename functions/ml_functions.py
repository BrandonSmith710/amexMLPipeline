import pickle
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import GridSearchCV
from category_encoders import OrdinalEncoder

def transform_train(train):
    #transform training dataframe before insertion into table customers_train

    train.loc[train['occupation_type'] == 'Unknown', 'occupation_type'] = 'Other'
    train.loc[train['owns_car'].isnull(), 'owns_car'] = 'N'
    train.loc[train['credit_score'].isnull(), 'credit_score'] = 500.0
    train.loc[train['no_of_children'].isnull(), 'no_of_children'] = 0.0
    train.loc[train['no_of_days_employed'].isnull(), 'no_of_days_employed'] = 0.0
    train.loc[train['total_family_members'].isnull(), 'total_family_members'] = 0.0
    train.loc[train['migrant_worker'].isnull(), 'migrant_worker'] = 0.0
    train.loc[train['yearly_debt_payments'].isnull(), 'yearly_debt_payments'] = 0.0
    train['credit_score'] = train['credit_score'].astype(int)
    train['no_of_children'] = train['no_of_children'].astype(int)
    train['no_of_days_employed'] = train['no_of_days_employed'].astype(int)
    train['total_family_members'] = train['total_family_members'].astype(int)
    train['migrant_worker'] = train['migrant_worker'].astype(int)
    # necessary for psycopg2 to handle values of type numpy.int64
    train = pd.DataFrame(data = train.itertuples(index = False),
                        columns = train.columns)

def transform_test(test):
    # transform testing dataframe before insertion into table customers

    test.loc[test['occupation_type'] == 'Unknown', 'occupation_type'] = 'Other'
    test.loc[test['owns_car'].isnull(), 'owns_car'] = 'N'
    test.loc[test['credit_score'].isnull(), 'credit_score'] = 500.0
    test.loc[test['no_of_children'].isnull(), 'no_of_children'] = 0.0
    test.loc[test['no_of_days_employed'].isnull(), 'no_of_days_employed'] = 0.0
    test.loc[test['total_family_members'].isnull(), 'total_family_members'] = 0.0
    test.loc[test['migrant_worker'].isnull(), 'migrant_worker'] = 0.0
    test.loc[test['yearly_debt_payments'].isnull(), 'yearly_debt_payments'] = 0.0
    test['credit_score'] = test['credit_score'].astype(int)
    test['no_of_children'] = test['no_of_children'].astype(int)
    test['no_of_days_employed'] = test['no_of_days_employed'].astype(int)
    test['total_family_members'] = test['total_family_members'].astype(int)
    test['migrant_worker'] = test['migrant_worker'].astype(int)
    test = pd.DataFrame(data = test.itertuples(index = False),
                        columns = test.columns)

def train_xgb(curs) -> str:
    # retrieve data for each record in database to train & serialize xgb classifier

    curs.execute('''SELECT age
                    , owns_car
                    , owns_house
                    , num_children
                    , yearly_income
                    , occupation
                    , credit_card_default
                    FROM customers_train;''')

    train = pd.DataFrame(data = curs.fetchall(),
                         columns = ['age', 'owns_car', 'owns_house',
                                   'no_of_children', 'net_yearly_income',
                                   'occupation_type', 'credit_card_default'])
    converter = lambda x: 1 if x == 'Y' else 0
    train['owns_house'] = train['owns_house'].apply(converter)
    train['owns_car'] = train['owns_car'].apply(converter)
    X, y = train.drop('credit_card_default', axis = 1), train['credit_card_default']
    xgb = XGBClassifier(random_state = 42, n_jobs = -1)
    ord_enc = OrdinalEncoder(cols = ['occupation_type'])
    pipe_xgb = make_pipeline(ord_enc, xgb)
    params = {'xgbclassifier__max_depth': range(15, 21),
            'xgbclassifier__booster': ['gbtree', 'gblinear', 'dart'],
            'xgbclassifier__n_estimators': range(60, 100, 10)}
    xgbgs = GridSearchCV(estimator = pipe_xgb, param_grid = params, cv = 5)
    xgbgs.fit(X, y)
    pipe_xgb = xgbgs.best_estimator_
    pipe_xgb.fit(X, y)
    with open('model_xgb.h5', 'wb') as f:
        pickle.dump(pipe_xgb, f)
        f.close()
    return f'Model accuracy: {pipe_xgb.score(X, y)}'


def predict_default(curs, id, model) -> str:
    '''retrieve record corresponding to id and predict credit card default
       parameters: curs - psycopg2 cursor
                   id - verified cid for existing database record
                   model - trained classification model
    '''

    curs.execute('''SELECT age
                    , owns_car
                    , owns_house
                    , num_children
                    , yearly_income
                    , occupation
                    FROM customers
                    WHERE cid = %s;''', (id,))
    row = curs.fetchall()
    row = [[0 if x == 'N' else 1 if x == 'Y' else x for x in row[0]]]
    s = pd.DataFrame(data = row,
                     columns = ['age', 'owns_car', 'owns_house', 'no_of_children',
                               'net_yearly_income', 'occupation_type'])
    pred = model.predict(s)[0]
    outcomes = ['Not a risk for credit card default', 'Risk for credit card default']
    return f'Customer {id}: {outcomes[pred]}'