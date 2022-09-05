This application offers several Python scripts which execute ETL & CRUD processes relating to an American Express credit card owner dataset.

transform_load.py will clean and transform csv files and insert into their respective tables

extract_train.py will query data from all records and then train an XGBClassifier model using the query results

update_customer_info.py allows a user to alter records of any table using primary key lookup

amex.py allow a user to enter an existing primary key for the customers table, and predict if the record associated with the key is a risk for credit card default



To install, simply clone the repository and change directories into it.
Then, run:

pipenv install pandas numpy xgboost sklearn category-encoders psycopg2-binary

Then run:

pipenv shell

Finally, add a .env file to the amexMLPipeline folder, and inside specify the host, user, password, and database.