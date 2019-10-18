#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()

def sortEachTable(InputTable, SortingColumnName, min, max, OutputTable, openconnection):
    cur = openconnection.cursor()

    if OutputTable=="t1":
        query = "Create table {0} as select * from {1} where {2}>={3} and {2}<={4} order by {2}".format(OutputTable,
                                                                                                        InputTable,
                                                                                                        SortingColumnName,
                                                                                                        min,max)
    else:
        query = "Create table {0} as select * from {1} where {2}>{3} and {2}<={4} order by {2}".format(OutputTable,
                                                                                                        InputTable,
                                                                                                        SortingColumnName,
                                                                                                        min, max)

    cur.execute(query)



def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    query = "Select max({0}), min({0}) from {1}".format(SortingColumnName, InputTable)
    cur.execute(query)
    maximum, minimum = cur.fetchone()
    n_threads = 5
    thread_results = []
    thread_tables = ["t1", "t2", "t3", "t4", "t5"]

    interval = float(maximum-minimum)/5.0

    for i in range(0,n_threads):
        thread_results.append(threading.Thread(target=sortEachTable,args=(InputTable, SortingColumnName, minimum, minimum+interval, thread_tables[i], openconnection)))
        thread_results[i].start()
        minimum = minimum + interval

    cur.execute("Drop table if exists {0}".format(OutputTable))
    cur.execute("Create table {0} as select * from {1} where 2=1".format(OutputTable,InputTable))

    for i in range(0,n_threads):
        thread_results[i].join()
        query = "Insert into {0} select * from {1}".format(OutputTable,thread_tables[i])
        cur.execute(query)

    cur.close()
    openconnection.commit()

def joinEachTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, min, max, OutputTable, openconnection):
    cur = openconnection.cursor()
    if OutputTable == "t1":
        query = "Create table {0} as select * from {1} as A, {2} as B where A.{3}=B.{4} and A.{3}>={5} and A.{3}<={6}"\
            .format(OutputTable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,min,max)
    else:
        query = "Create table {0} as select * from {1} as A, {2} as B where A.{3}=B.{4} and A.{3}>{5} and A.{3}<={6}"\
            .format(OutputTable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,min,max)

    cur.execute(query)


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()
    query = "Select max({0}), min({0}) from {1}".format(Table1JoinColumn, InputTable1)
    cur.execute(query)
    maximum, minimum = cur.fetchone()
    n_threads = 5
    thread_results = []
    thread_tables = ["t1", "t2", "t3", "t4", "t5"]

    interval = float(maximum - minimum) / 5.0

    for i in range(0,n_threads):
        thread_results.append(threading.Thread(target=joinEachTable,args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, minimum, minimum+interval, thread_tables[i], openconnection)))
        thread_results[i].start()
        minimum = minimum + interval

    cur.execute("Drop table if exists {0}".format(OutputTable))
    cur.execute("Create table {0} as select * from {1},{2} where 2=1".format(OutputTable,InputTable1,InputTable2))

    for i in range(0,n_threads):
        thread_results[i].join()
        query = "Insert into {0} select * from {1}".format(OutputTable,thread_tables[i])
        cur.execute(query)

    cur.close()
    openconnection.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
