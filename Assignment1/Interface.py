#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # loading data into database

    try:
        with openconnection.cursor() as cur:
            cur.execute("Create table {0} (UserID integer,Dummy1 text,MovieID integer, Dummy2 text, Rating float, Dummy3 text, Dummy4 bigint)".format(ratingstablename))
            #print("table created")
            cur.execute("Copy {0} from '{1}' delimiter ':'".format(ratingstablename, ratingsfilepath))
            cur.execute("Alter table {0} drop Dummy1".format(ratingstablename))
            cur.execute("Alter table {0} drop Dummy2".format(ratingstablename))
            cur.execute("Alter table {0} drop Dummy3".format(ratingstablename))
            cur.execute("Alter table {0} drop Dummy4".format(ratingstablename))

            cur.close()
            openconnection.commit()

    except Exception as e:
        print("Exception in Load Ratings: {0}".format(e))
        #cur.execute("Drop table {0}".format(ratingstablename))


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    width = 5.0/numberofpartitions
    #print(width)
    RANGE_TABLE_PREFIX = 'range_part'

    try:
        with openconnection.cursor() as cur:
            start = 0
            end = start + width
            cur.execute("Create table {0}{1} as select * from {2} where rating >= {3} and rating<={4}".format(RANGE_TABLE_PREFIX,0,ratingstablename,start, end))
            start = end
            for i in range(1,numberofpartitions):
                end = start + width
                #print(start,end)
                cur.execute("Create table {0}{1} as select * from {2} where rating > {3} and rating<={4}".format(RANGE_TABLE_PREFIX, i, ratingstablename, start, end))
                start = end

            cur.close()
            openconnection.commit()
    except Exception as e:
        print("Exception in Rang Partitioning: {0}".format(e))


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    #Function to fragment table using round robin
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur = openconnection.cursor()

    for i in range(0, numberofpartitions):
        cur.execute("Create table {0}{1} as select userid, movieid, rating from (select row_number() over()-1 as row, * from {2}) as temp where mod(row,{3})={1}".format(RROBIN_TABLE_PREFIX,i,ratingstablename,numberofpartitions))

    cur.close()
    openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    #Function to insert a row in main table and round robbin fragment

    RROBIN_TABLE_PREFIX = 'rrobin_part'
    try:
        cur = openconnection.cursor()
        cur.execute("Select count(*) from pg_stat_user_tables where relname like '{0}%'".format(RROBIN_TABLE_PREFIX))
        numberofpartitions = cur.fetchone()[0]
        #print("numberofpartitions: ",numberofpartitions)

        cur.execute("insert into {0} values ({1}, {2}, {3})".format(ratingstablename, userid, itemid, rating))
        cur.execute("select count(*) from {0}".format(ratingstablename))
        row = cur.fetchone()[0] - 1
        #print("row",row)

        possible_partition = row%numberofpartitions
        RROBIN_TABLE = RROBIN_TABLE_PREFIX + str(possible_partition)
        #print("RROBIN Table:", RROBIN_TABLE)

        cur.execute("insert into {0} values ({1}, {2}, {3})".format(RROBIN_TABLE, userid, itemid, rating))
        cur.close()
        openconnection.commit()
    except Exception as e:
        print("Exception in Round Robbin Insert: {0}".format(e))


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    #Function to insert 1 row in main table and appropriate fragment
    RANGE_TABLE_PREFIX = 'range_part'
    cur = openconnection.cursor()
    cur.execute("Select count(*) from pg_stat_user_tables where relname like '{0}%'".format(RANGE_TABLE_PREFIX))

    numberofpartitions = cur.fetchone()[0]
    width = 5.0/numberofpartitions
    possible_range = int(rating/width)
    if possible_range!=0 and rating%width==0:
        possible_range = possible_range-1
    RANGE_TABLE = RANGE_TABLE_PREFIX+str(possible_range)

    cur.execute("insert into {0} values ({1}, {2}, {3})".format(RANGE_TABLE,userid,itemid, rating))
    cur.execute("insert into {0} values ({1}, {2}, {3})".format(ratingstablename,userid,itemid, rating))

    cur.close()
    openconnection.commit()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()

#
# if __name__ == '__main__':
#     create_db(DATABASE_NAME)
#     con = getopenconnection('postgres','1234',DATABASE_NAME)
#     con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
#
#     RATINGS_TABLE = 'ratings_test'
#     INPUT_FILE_PATH = '/home/user/Downloads/Assignment1/test_data.dat'
#     loadratings(RATINGS_TABLE, INPUT_FILE_PATH, con)
#     # rangepartition(RATINGS_TABLE, 3, con)
#     # rangeinsert(RATINGS_TABLE,4,70,0, con);
#     # rangeinsert(RATINGS_TABLE, 4, 70, 1, con);
#     # rangeinsert(RATINGS_TABLE, 4, 70, 2.5, con);
#     # rangeinsert(RATINGS_TABLE, 4, 70, 4.5, con);
#
#     roundrobinpartition(RATINGS_TABLE, 5, con)
#     roundrobininsert(RATINGS_TABLE, 5, 90, 0, con)
#     roundrobininsert(RATINGS_TABLE, 5, 90, 1, con)
#     roundrobininsert(RATINGS_TABLE, 5, 90, 2.5, con)
#     # roundrobininsert(RATINGS_TABLE, 5, 90, 4.5, con)
