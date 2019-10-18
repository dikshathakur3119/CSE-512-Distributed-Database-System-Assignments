#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.

    range_part = 'RangeRatingsPart'
    rr_part = 'RoundRobinRatingsPart'
    file = open(outputPath, "w+")

    cur = openconnection.cursor()
    cur.execute("Select * from RangeRatingsMetadata")
    rows = cur.fetchall()
    for row in rows:
        table = range_part+str(row[0])
        if not(row[1] > ratingMaxValue or ratingMinValue > row[2]):
            cur.execute("Select * from {0} where rating>={1} and rating<={2}".format(table,ratingMinValue,ratingMaxValue))
            data = cur.fetchall()

            for d in data:
                wr = table+','+str(d[0])+','+str(d[1])+','+str(d[2])+'\n'
                file.write(wr)

    cur.execute("Select * from RoundRobinRatingsMetadata")
    part_nums = cur.fetchall()[0][0]
    for partition in range(0,part_nums):
        table = rr_part + str(partition)
        cur.execute("Select * from {0} where rating>={1} and rating<={2}".format(table,ratingMinValue,ratingMaxValue))
        data = cur.fetchall()
        for d in data:
            wr = table + ',' + str(d[0])+','+str(d[1])+','+str(d[2]) + '\n'
            file.write(wr)

    file.close()
    cur.close()
    openconnection.commit()

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    range_part = 'RangeRatingsPart'
    rr_part = 'RoundRobinRatingsPart'
    file = open(outputPath, "w+")

    cur = openconnection.cursor()
    cur.execute("Select * from RangeRatingsMetadata")
    rows = cur.fetchall()
    for row in rows:
        table = range_part + str(row[0])
        if not (row[1] > ratingValue or ratingValue > row[2]):
            cur.execute(
                "Select * from {0} where rating={1}".format(table, ratingValue))
            data = cur.fetchall()

            for d in data:
                wr = table + ',' + str(d[0])+','+str(d[1])+','+str(d[2]) + '\n'
                file.write(wr)

    #RoundRobin Part
    cur.execute("Select * from RoundRobinRatingsMetadata")
    part_nums = cur.fetchall()[0][0]
    for partition in range(0,part_nums):
        table = rr_part + str(partition)
        cur.execute("Select * from {0} where rating={1}".format(table,ratingValue))
        data = cur.fetchall()
        for d in data:
            wr = table + ',' + str(d[0])+','+str(d[1])+','+str(d[2]) + '\n'
            file.write(wr)

    file.close()
    cur.close()
    openconnection.commit()
