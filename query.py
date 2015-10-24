import os
import sys, getopt
import tdclient

#Validate options
def validate_options(engine,format):
	if engine not in ("presto", "hive"):
		print "Please specify correct engine value"
		sys.exit(2)
	if format not in ("tabular", "csv"):
		print "Please specify correct format value"
		sys.exit(2)

#Build query
def build_query(col_list,table_name,min_time,max_time,limit):
	query = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + "," + max_time + ")"
	if limit is not None:
		query = query + " limit " + limit
	return query

#Execute query
def execute_query(apikey,db_name,query,engine,format):
		with tdclient.Client(apikey) as client:
    			job = client.query(db_name,query,type=engine)
    			# sleep until job's finish
    			job.wait()
			f = open('results.txt', 'w')
    			for row in job.result():
				if(format == 'csv'):  			
      					f.write(','.join(map(str, row)))
				else:
					f.write('\t'.join(map(str, row)))
				f.write('\n')


def main():

	apikey = "7072/89472d81d6e3e172ab0eadd0cf89829ea572abca"
	db_name = ''
	table_name = ''
	col_list = '*'
	min_time = None
	max_time = None
	engine = "presto"
	format = "tabular"
	limit = None

	try:
        	myoptions, args = getopt.getopt(sys.argv[1:],"f:e:c:m:M:l:")
	except getopt.GetoptError as err:
		print "Please specify a valid option: -f, -e, -c, -m, -M, -l"
        	sys.exit(2)

	db_name = args[0]
	table_name = args[1]

	for o, a in myoptions:
    		if o == '-f':
        		format = a
    		elif o == '-e':
        		engine = a
    		elif o == '-c':
        		col_list = a
    		elif o == '-m':
        		min_time = a
    		elif o == '-M':
        		max_time = a
    		elif o == '-l':
        		limit = a

	validate_options(engine,format)

	query = build_query(col_list,table_name,min_time,max_time,limit)
	print query
	execute_query(apikey,db_name,query,engine,format)

if __name__ == "__main__":
    main()
