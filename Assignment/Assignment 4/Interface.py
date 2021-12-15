#!/usr/bin/python2.7
#
# Assignment2 Interface
#
import Assignment1
import psycopg2
import os
import sys
DATABASE_NAME = 'dds_assignment'
RANGE_TABLE_PREFIX = 'RangeRatingsPart'
RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()


    #--------range partition table-----------------------
    #locate range partition table number from RangeRatingsMetadata with ratingMinValue and ratingMaxValue
    cur.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating BETWEEN %s AND %s OR  MaxRating BETWEEN %s AND %s" %(ratingMinValue,ratingMaxValue,ratingMinValue,ratingMaxValue))
    rows_n=cur.fetchall()
      
    data=[]
        # read data from selected range partition table
    for PartitionNum in rows_n:
        range_partion_table_name=RANGE_TABLE_PREFIX+str(PartitionNum[0])
        cur.execute("SELECT * FROM %s WHERE rating >= %s AND rating<= %s " %(range_partion_table_name,ratingMinValue, ratingMaxValue))
        rows_partition=cur.fetchall()

        for n in rows_partition:
            data.append([range_partion_table_name]+list(n))


    #--------robin partition table-----------------------
    #find total robin partition table number 
    cur.execute("SELECT COUNT(table_name) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'roundrobinratingspart%' ")
    row=cur.fetchall()
    rows_n=row[0][0]
    
    
    # read data from all robin partition table
    for PartitionNum in range(rows_n):
        robin_partion_table_name=RROBIN_TABLE_PREFIX+str(PartitionNum)
        cur.execute("SELECT * FROM %s WHERE rating >= %s AND rating<= %s " %(robin_partion_table_name,ratingMinValue, ratingMaxValue))
        rows_partition=cur.fetchall()

        for n in rows_partition:
           data.append([robin_partion_table_name]+list(n))
    

    writeToFile('RangeQueryOut.txt',data)
        
      
        

    

    


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    
    
    #--------range partition table-----------------------
    #locate range partition table number from RangeRatingsMetadata where ratingValue between  MinRating and MaxRating
    cur.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE %s BETWEEN MinRating AND MaxRating" %(ratingValue))
    rows_n=cur.fetchall()
      
    data=[]
        # read data from selected range partition table
    for PartitionNum in rows_n:
        range_partion_table_name=RANGE_TABLE_PREFIX+str(PartitionNum[0])
        cur.execute("SELECT * FROM %s WHERE rating = %s " %(range_partion_table_name,ratingValue))
        rows_partition=cur.fetchall()

        for n in rows_partition:
            data.append([range_partion_table_name]+list(n))


    #--------robin partition table-----------------------
    #find total robin partition table number 
    cur.execute("SELECT COUNT(table_name) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'roundrobinratingspart%' ")
    row=cur.fetchall()
    rows_n=row[0][0]
    
    
    # read data from all robin partition table
    for PartitionNum in range(rows_n):
        robin_partion_table_name=RROBIN_TABLE_PREFIX+str(PartitionNum)
        cur.execute("SELECT * FROM %s WHERE rating = %s " %(robin_partion_table_name,ratingValue))
        rows_partition=cur.fetchall()

        for n in rows_partition:
           data.append([robin_partion_table_name]+list(n))
    

    writeToFile('PointQueryOut.txt',data)
    


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()





#Assignment1.createDB(dbname='dds_assignment')
#ratingsfilepath='test_data.txt'
#ratingstablename='ratings'
#ratingMinValue=0
#ratingMaxValue=3
#ratingValue=4.5
#userid=2
#itemid=994
#rating=4.5

#numberofpartitions=5
#Assignment1.loadRatings(ratingstablename,ratingsfilepath,Assignment1.getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#Assignment1.rangePartition(ratingstablename, numberofpartitions, Assignment1.getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#Assignment1.roundRobinPartition(ratingstablename, numberofpartitions, Assignment1.getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#roundrobininsert(ratingstablename, userid, itemid, rating, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#rangeinsert(ratingstablename, userid, itemid, rating, getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#RangeQuery(ratingstablename, ratingMinValue, ratingMaxValue, Assignment1.getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))
#PointQuery(ratingstablename, ratingValue, Assignment1.getOpenConnection(user='postgres', password='somerabbit', dbname='dds_assignment'))