import os
import tdclient
apikey = "7072/89472d81d6e3e172ab0eadd0cf89829ea572abca"


db_name = "airport_data"
table_name = "sfo_pass_raw"
col_list = "activity_type_code,operating_airline,geo_region,month,year,passenger_count"
min_time = "1420099200"
max_time = "1433142000"
engine = "hive"
format = "tabular"
limit = "100"

query = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + "," + max_time + ") limit " + limit
print query

with tdclient.Client(apikey) as client:
    job = client.query(db_name, query,type=engine)
    # sleep until job's finish
    job.wait()
    for row in job.result():
        print(row)
