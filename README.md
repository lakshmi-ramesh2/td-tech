README

query.py

This script provides a command line wrapper to execute a query on a Treasure Data database table and downloads the results. 

Database Info:

Database Name: airport_data
Table Name: sfo_pass_raw

This database table contains passenger data that have deplaned/enplaned at SFO International Airport.

Prerequisites:

TD toolbelt
Python 2.7 or newer
Python bindings - td-client 

To Run:

python query.py <db_name> <table_name>

Eg: python query.py airport_data sfo_pass_raw

The database and table names are mandatory arguments that need to be passed.

Optional Parameters:

-f : can be used to specify results download format - csv or tabular (default)
-e : can be used to specify engine - hive or presto (default)
-c : can be used to specify a column list, by default all columns will be returned
-m : a minimum timestamp can be specified
-M : a maximum timestamp can be specified
-l : a limit on the number of rows returned can be specified. By default, it will return all rows

Eg: python query.py -f csv -e hive -c year,month,time -m 1420099200 -M 1433142000 -l 5 airport_data sfo_pass_raw

Queries Executed:

See Query.txt

For results of each query, see Results_#.txt (e.g.: Results_1.txt contains results of Query 1)


