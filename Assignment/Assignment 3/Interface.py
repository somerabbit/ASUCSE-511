#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")
    


def loadRatings(ratingstablename,ratingsfilepath,openconnection):
    cur=openconnection.cursor()
    
    
    cur.execute("DROP TABLE IF EXISTS Ratings CASCADE;")
    cur.execute( "CREATE TABLE Ratings (UserID int,symbol1 char ,MovieID int,symbol2 char,Rating float,symbol3 char,Timestamp bigint,PRIMARY KEY (UserID, MovieID));")

    with open(ratingsfilepath,'r') as data:
       cur.copy_from(data, ratingstablename,sep=':')

    cur.execute("ALTER TABLE Ratings DROP COLUMN symbol1, DROP COLUMN symbol2, DROP COLUMN symbol3, DROP COLUMN Timestamp; ")

    openconnection.commit()



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    
    with openconnection.cursor() as cur:
        min=0
        max=5
        number_range=(max-min)/numberofpartitions
    
        for n in range(numberofpartitions):  
            max=min+number_range
            
            #load range data from main data
            if n==0:
                cur.execute("SELECT* FROM {} WHERE {}.rating>={} AND {}.rating<={} ".format(ratingstablename,ratingstablename,min,ratingstablename,max))
            else:
                cur.execute("SELECT* FROM {} WHERE {}.rating>{} AND {}.rating<={} ".format(ratingstablename,ratingstablename,min,ratingstablename,max))
            min=max

            # save range data
            rows=cur.fetchall()
        
        
            #create range table
            
            cur.execute("DROP TABLE IF EXISTS range_part{} CASCADE;".format(n))
            cur.execute("CREATE TABLE range_part{} (UserID int,MovieID int,Rating float,PRIMARY KEY (UserID, MovieID))".format(n))
           
            #store range data to range table 
            for data in rows:
                cur.execute("INSERT INTO range_part{} (UserID ,MovieID,Rating) VALUES({} ,{} ,{}) ".format(n,data[0],data[1],data[2]))
    openconnection.commit()         # work for python 3 
    

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):  

    with openconnection.cursor() as cur:

        #create table 
        for n in range(numberofpartitions):
            
            cur.execute("DROP TABLE IF EXISTS rrobin_part{} CASCADE;".format(n))
            cur.execute("CREATE TABLE rrobin_part{} (UserID int,MovieID int,Rating float,PRIMARY KEY (UserID, MovieID))".format(n))
        
        # read main data
        cur.execute("SELECT* FROM ratings")
        rows=cur.fetchall()
        
            
        row_count=0
        for data in rows:
            n=row_count%numberofpartitions
            cur.execute("INSERT INTO rrobin_part{} (UserID ,MovieID,Rating) VALUES({} ,{} ,{}) ".format(n,data[0],data[1],data[2]))
            row_count+=1


    openconnection.commit()  # work for python 3

        

    
    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        # find current partition numb
        cur.execute("SELECT COUNT(table_name) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'rrobin_part%' ")
        row=cur.fetchall()
        total_n=row[0][0]  # total partition number
        
        data=total_n*[0]
        #store index and value into a array
        for a in range (total_n):
            
            cur.execute("SELECT COUNT(*) FROM rrobin_part{} ".format(a))
            row=cur.fetchall()
            data_n=row[0][0]  # data number of each partition table
            data[a]=data_n
       

        #find the partition table with min data number
        min_value=min(data)
        min_index=data.index(min_value)

        #insert data
        cur.execute("INSERT INTO rrobin_part{} (UserID ,MovieID,Rating) VALUES({} ,{} ,{}) ".format(min_index,userid,itemid,rating))

      
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        
        #find current partition number
        cur.execute("SELECT COUNT(table_name) from INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'range_part%' ")
        row=cur.fetchall()
        total_n=row[0][0]  # total partition number

        size_range=5/total_n
        
        # specific partion number
        if rating==0:
            n=0
        else:
            n=int(math.ceil(rating/size_range))-1
        
        #insert data
        cur.execute("INSERT INTO range_part{} (UserID ,MovieID,Rating) VALUES({} ,{} ,{}) ".format(n,userid,itemid,rating))
 
    openconnection.commit()  # work for python 3
   

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))   #######

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except (psycopg2.DatabaseError, e):
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    except (IOError, e):
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    finally:
        if cursor:
            cursor.close()


#createDB(dbname='dds_assignment')




ratingsfilepath='test_data.txt'
ratingstablename='ratings'





#userid=2
#itemid=994
#rating=4.5

#numberofpartitions=5




loadRatings(ratingstablename,ratingsfilepath,getOpenConnection(user='postgres', password='typeyourpassword', dbname='dds_assignment'))










#rangePartition(ratingstablename, numberofpartitions, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#roundRobinPartition(ratingstablename, numberofpartitions, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#roundrobininsert(ratingstablename, userid, itemid, rating, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#rangeinsert(ratingstablename, userid, itemid, rating, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))

