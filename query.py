import os
import sys, getopt
import tdclient
import requests

#Validate arguments - DB name and Table Name
def validate_args(args):
        args_len = len(args)
        if args_len != 2:
                print "Please provide both DB name and Table Name"
                sys.exit(2)

#Validate options - min_time, max_time, engine and format
def validate_options(min_time,max_time,engine,format):
	#Check if min_time is not NULL and does not contain numbers
	if min_time is not None and not min_time.isdigit():
		print "Please specify a valid timestamp for min_time"
		sys.exit(2)
	#Check if max_time is not NULL and does not contain numbers
	if max_time is not None and not max_time.isdigit():
        	print "Please specify a valid timestamp for max_time"
        	sys.exit(2)
	#Check if min_time and max_time are not NULL and max_time is greater than min_time
	if min_time is not None and max_time is not None and int(min_time) > int(max_time):
		print "Min time must be smaller than Max time"
		sys.exit(2)
	#Check if engine is either presto or hive
	if engine not in ("presto", "hive"):
		print "Please specify correct engine value"
		sys.exit(2)
	#Check if format is either tabular or csv
	if format not in ("tabular", "csv"):
		print "Please specify correct format value"
		sys.exit(2)

#Build query
def build_query(col_list,table_name,min_time,max_time,limit):
	query = "SELECT " + col_list + " FROM " + table_name
	#If min_time or max_time is None, pass NULL in query
	if min_time is not None or max_time is not None:
		if min_time is None:
			min_time = "NULL"
		if max_time is None:
			max_time = "NULL"
 		query = query + " where TD_TIME_RANGE(time," + min_time + "," + max_time + ")"
	#Add limit to query if limit is not NULL
	if limit is not None:
		query = query + " limit " + limit
	return query

def execute_query(apikey,db_name,query,engine,format):
	try:
		with tdclient.Client(apikey) as client:
    			job = client.query(db_name,query,type=engine)
    			# sleep until job's finish
    			job.wait()
			f = open('results.txt', 'w')
			#Counter to check number of records in result set
			size = 0
    			for row in job.result():
				if(format == 'csv'):  			
      					f.write(','.join(map(str, row)))
				else:
					f.write('\t'.join(map(str, row)))
				f.write('\n')
				size = size + 1
			#print "Size is",size
			#Check for empty result set
			if size == 0:
				print "No records in result set"

	except:
		print "Error executing query on TD"
		sys.exit(1)

def main():
	#Define variables and default values
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

	#Call validate arguments
	validate_args(args)

	#Assign DB name and Table name from arguments
	db_name = args[0]
	table_name = args[1]

	#Get options
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

	#Call to validate options
	validate_options(min_time,max_time,engine,format)

	#Call to build the query
	query = build_query(col_list,table_name,min_time,max_time,limit)
	#print query
	#Call to execute the query
	execute_query(apikey,db_name,query,engine,format)

if __name__ == "__main__":
    main()

