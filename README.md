This application offers several Python scripts which execute warehousing, ETL & CRUD processes relating to an American Express credit card owner dataset.

transform_load.py will clean and transform csv files and insert into their respective tables

extract_train.py will query data from all records and then train an XGBClassifier model using the query results

amex.py allows a user to enter an existing primary key for a chosen table, and predict if the record associated with that key is a risk for credit card default

update_customer_info.py allows a user to alter records of any table using primary key lookup

add_or_delete.py gives the user the ability to insert records into or delete records from any table

customer_stats.py permits a user to view statistics of entire tables, or of tables which have been filtered according to user input

To install, simply clone the repository and change directories into it.
Then, run:

pipenv install pandas numpy xgboost sklearn category-encoders psycopg2-binary

Then run:

pipenv shell

Finally, add a .env file to the amexMLPipeline folder, and inside specify the host, user, password, and database.