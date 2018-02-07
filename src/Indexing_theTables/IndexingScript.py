import datetime, time
import psycopg2 as pg
import sys
import csv

def run_sql_file(filename, connection):
    start = time.time()
    file = open(filename, 'r')
    sql = " ".join(file.readlines())
    #print(sql)
    print "Start executing: " + filename + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")) + "\n" 
    cursor = connection.cursor()
    cursor.execute(sql)    
    connection.commit()
    #print(sql)
    end = time.time()
    print "Time elapsed to run the query:"
    print str((end - start)*1000) + ' ms'

def main():
    f = open('/home/pal/WorkingNew/abhi/src/Indexing/DBName.csv')
    csv_f = csv.reader(f)
    for row in csv_f:
        DBName=''.join(row)
        print(DBName)
        dbstr="dbname = '"+ DBName +"' user = 'abhi' host = 'localhost' port ='5435' password = '**********' "
        print(dbstr)
        conn = pg.connect(dbstr)
        #conn='test'
		#Chnage the sql path for each table.
        run_sql_file("/src/Indexing/sql/posts/Tags_post.sql", conn)    
        conn.close()
if __name__ == "__main__":
    main()